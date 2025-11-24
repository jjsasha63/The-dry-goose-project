import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple, Literal
import json
import openai
from pathlib import Path
import openpyxl
from openpyxl.utils import get_column_letter
import re
import tiktoken
from pydantic import BaseModel, Field, validator
from enum import Enum
from collections import defaultdict
import hashlib

# ============================================================================
# STRICT SCHEMA DEFINITIONS
# ============================================================================

class DataType(str, Enum):
    """Supported data types for extraction"""
    NUMBER = "number"
    CURRENCY = "currency"
    PERCENTAGE = "percentage"
    DATE = "date"
    TEXT = "text"
    BOOLEAN = "boolean"
    LIST = "list"

class ExtractionResult(BaseModel):
    """Strict schema for extraction results - ensures perfect structure"""
    found: bool = Field(description="Whether the value was found")
    value: Optional[Any] = Field(description="The exact extracted value")
    sheet_name: Optional[str] = Field(description="Sheet containing the value")
    cell_reference: Optional[str | List[str]] = Field(description="Cell reference(s)")
    data_type: DataType = Field(description="Type of the extracted value")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence score 0-1")
    reasoning: str = Field(description="Step-by-step reasoning for extraction")
    verification_steps: List[str] = Field(description="Steps taken to verify accuracy")
    alternative_interpretations: Optional[List[str]] = Field(
        default=None, 
        description="Other possible interpretations if ambiguous"
    )
    formula_if_applicable: Optional[str] = Field(default=None)
    context_cells: Optional[Dict[str, str]] = Field(
        default=None,
        description="Surrounding cells that provide context"
    )
    
    @validator('confidence')
    def confidence_must_be_justified(cls, v, values):
        """Ensure confidence matches the reasoning"""
        if v > 0.9 and not values.get('found'):
            raise ValueError("High confidence without found value is invalid")
        return v

class MultiPassResult(BaseModel):
    """Result from multiple extraction passes"""
    pass_1_result: ExtractionResult
    pass_2_result: ExtractionResult
    pass_3_result: Optional[ExtractionResult] = None
    consensus_value: Any
    consensus_confidence: float
    discrepancies: List[str]
    final_verified: bool

# ============================================================================
# PRECISION EXCEL EXTRACTOR
# ============================================================================

class PrecisionExcelExtractor:
    """
    Maximum precision extractor using:
    - Structured outputs (Pydantic schemas)
    - Multi-pass extraction with verification
    - Few-shot examples for perfect formatting
    - Chunked processing with update-on-change logic
    - Token-optimized context
    """
    
    def __init__(self, 
                 api_key: str,
                 model: str = "gpt-4",
                 azure_endpoint: Optional[str] = None,
                 max_context_tokens: int = 120000):
        
        if azure_endpoint:
            openai.api_type = "azure"
            openai.api_base = azure_endpoint
            openai.api_version = "2024-02-15-preview"
            openai.api_key = api_key
        else:
            openai.api_key = api_key
            
        self.model = model
        self.azure_endpoint = azure_endpoint
        self.max_context_tokens = max_context_tokens
        
        try:
            self.encoding = tiktoken.encoding_for_model(model)
        except KeyError:
            self.encoding = tiktoken.get_encoding("cl100k_base")
    
    def count_tokens(self, text: str) -> int:
        """Precise token counting"""
        return len(self.encoding.encode(text))
    
    def extract_sheet_structure(self, ws, ws_data_only, max_rows: Optional[int] = None) -> Dict[str, Any]:
        """Extract complete sheet structure with semantic understanding"""
        
        max_row = ws.max_row if not max_rows else min(ws.max_row, max_rows)
        max_col = ws.max_column
        
        # Identify structural elements
        headers = []
        bold_cells = {}
        highlighted_cells = {}
        formula_cells = {}
        numeric_cells = {}
        text_cells = {}
        
        # Build spatial index for fast lookup
        cell_index = {}
        
        for row_idx in range(1, max_row + 1):
            for col_idx in range(1, max_col + 1):
                cell = ws.cell(row_idx, col_idx)
                value = cell.value
                
                if value is None or (isinstance(value, str) and not value.strip()):
                    continue
                
                cell_ref = f"{get_column_letter(col_idx)}{row_idx}"
                
                # Classify cell
                cell_info = {
                    "value": value,
                    "row": row_idx,
                    "col": col_idx,
                    "is_bold": cell.font and cell.font.bold,
                    "has_fill": cell.fill and cell.fill.start_color.index != "00000000",
                    "is_merged": False
                }
                
                # Handle formulas
                if isinstance(value, str) and value.startswith('='):
                    calculated = ws_data_only.cell(row_idx, col_idx).value
                    cell_info["is_formula"] = True
                    cell_info["formula"] = value
                    cell_info["calculated_value"] = calculated
                    formula_cells[cell_ref] = cell_info
                else:
                    cell_info["is_formula"] = False
                
                # Categorize by type and position
                if cell_info["is_bold"] and row_idx <= 5:
                    headers.append((cell_ref, value))
                
                if cell_info["is_bold"]:
                    bold_cells[cell_ref] = value
                
                if cell_info["has_fill"]:
                    highlighted_cells[cell_ref] = value
                
                if isinstance(value, (int, float)) or (isinstance(calculated := cell_info.get("calculated_value"), (int, float))):
                    numeric_cells[cell_ref] = calculated if cell_info.get("is_formula") else value
                
                if isinstance(value, str) and not value.startswith('='):
                    text_cells[cell_ref] = value
                
                cell_index[cell_ref] = cell_info
        
        # Build contextual grid (semantic representation)
        grid_semantic = []
        for row_idx in range(1, min(max_row + 1, 51)):  # First 50 rows for semantic understanding
            row_cells = []
            for col_idx in range(1, min(max_col + 1, 21)):  # First 20 cols
                cell_ref = f"{get_column_letter(col_idx)}{row_idx}"
                if cell_ref in cell_index:
                    info = cell_index[cell_ref]
                    val = info.get("calculated_value", info["value"])
                    
                    # Add semantic markers
                    markers = []
                    if info["is_bold"]:
                        markers.append("BOLD")
                    if info["has_fill"]:
                        markers.append("HIGHLIGHT")
                    if info.get("is_formula"):
                        markers.append(f"CALC")
                    
                    if markers:
                        row_cells.append(f"{val}[{','.join(markers)}]")
                    else:
                        row_cells.append(str(val))
                else:
                    row_cells.append("")
            
            if any(c for c in row_cells):  # Only add non-empty rows
                grid_semantic.append(row_cells)
        
        return {
            "dimensions": {"rows": max_row, "cols": max_col},
            "headers": headers,
            "bold_cells": bold_cells,
            "highlighted_cells": highlighted_cells,
            "formula_cells": formula_cells,
            "numeric_cells": numeric_cells,
            "text_cells": text_cells,
            "cell_index": cell_index,
            "grid_semantic": grid_semantic
        }
    
    def read_all_sheets_optimized(self, file_path: str) -> Dict[str, Any]:
        """Read all sheets with optimized structure extraction"""
        
        wb = openpyxl.load_workbook(file_path, data_only=False)
        wb_data_only = openpyxl.load_workbook(file_path, data_only=True)
        
        sheets_data = {}
        
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            ws_data_only = wb_data_only[sheet_name]
            
            sheets_data[sheet_name] = self.extract_sheet_structure(ws, ws_data_only)
        
        wb.close()
        wb_data_only.close()
        
        return {
            "file_path": file_path,
            "sheet_names": list(sheets_data.keys()),
            "sheets": sheets_data
        }
    
    def build_precision_context(self, 
                                all_sheets: Dict[str, Any], 
                                query: str,
                                target_tokens: int) -> str:
        """Build context optimized for precision, not just compression"""
        
        sections = []
        
        # File overview
        sections.append(f"FILE: {all_sheets['file_path']}")
        sections.append(f"SHEETS: {', '.join(all_sheets['sheet_names'])}")
        sections.append("")
        
        # For each sheet, build hierarchical representation
        for sheet_name, sheet_data in all_sheets["sheets"].items():
            sheet_parts = []
            sheet_parts.append(f"### SHEET: {sheet_name} ###")
            
            dims = sheet_data["dimensions"]
            sheet_parts.append(f"Size: {dims['rows']}×{dims['cols']}")
            
            # Headers (critical for understanding structure)
            if sheet_data["headers"]:
                header_str = " | ".join([f"{ref}='{val}'" for ref, val in sheet_data["headers"][:15]])
                sheet_parts.append(f"HEADERS: {header_str}")
            
            # Key labeled cells (bold text usually indicates labels)
            if sheet_data["bold_cells"]:
                bold_items = list(sheet_data["bold_cells"].items())[:10]
                bold_str = " | ".join([f"{ref}='{val}'" for ref, val in bold_items])
                sheet_parts.append(f"LABELS: {bold_str}")
            
            # Important numeric values (highlighted cells often = results)
            if sheet_data["highlighted_cells"]:
                hl_items = list(sheet_data["highlighted_cells"].items())[:8]
                hl_str = " | ".join([f"{ref}='{val}'" for ref, val in hl_items])
                sheet_parts.append(f"HIGHLIGHTED: {hl_str}")
            
            # Formulas (show calculations)
            if sheet_data["formula_cells"]:
                formula_items = list(sheet_data["formula_cells"].items())[:5]
                for ref, info in formula_items:
                    sheet_parts.append(f"FORMULA {ref}: {info['formula']} → {info['calculated_value']}")
            
            # Semantic grid (shows spatial relationships)
            sheet_parts.append("\nGRID:")
            for row_idx, row in enumerate(sheet_data["grid_semantic"][:30], 1):  # First 30 rows
                non_empty = [(i, v) for i, v in enumerate(row) if v]
                if non_empty:
                    row_str = " | ".join([f"{get_column_letter(i+1)}{row_idx}:{v[:30]}" 
                                         for i, v in non_empty[:10]])
                    sheet_parts.append(f"  R{row_idx}: {row_str}")
            
            sheet_context = "\n".join(sheet_parts)
            
            # Check token budget
            test_context = "\n\n".join(sections + [sheet_context])
            if self.count_tokens(test_context) > target_tokens:
                # Truncate grid if needed
                sheet_parts = sheet_parts[:sheet_parts.index("\nGRID:") + 11]  # Keep first 10 grid rows
                sheet_context = "\n".join(sheet_parts)
            
            sections.append(sheet_context)
            sections.append("")
        
        return "\n\n".join(sections)
    
    def create_few_shot_examples(self) -> List[Dict[str, str]]:
        """Few-shot examples for perfect extraction format"""
        return [
            {
                "role": "user",
                "content": """SHEET: Q4_Results
HEADERS: A1='Quarter' | B1='Revenue' | C1='Profit'
GRID:
  R2: A2:Q4 | B2:$1,250,000 | C2:$340,000

Query: What was Q4 revenue?
Expected type: currency"""
            },
            {
                "role": "assistant",
                "content": json.dumps({
                    "found": True,
                    "value": 1250000,
                    "sheet_name": "Q4_Results",
                    "cell_reference": "B2",
                    "data_type": "currency",
                    "confidence": 1.0,
                    "reasoning": "1. Located 'Revenue' header in B1. 2. Found Q4 label in A2. 3. Retrieved corresponding value in B2: $1,250,000. 4. Verified no other Q4 revenue entries exist.",
                    "verification_steps": [
                        "Confirmed B1 contains 'Revenue' header",
                        "Confirmed A2 contains 'Q4' label",
                        "Confirmed B2 is in same row as Q4",
                        "Verified value format matches currency pattern"
                    ],
                    "alternative_interpretations": None,
                    "formula_if_applicable": None,
                    "context_cells": {
                        "A2": "Q4",
                        "B1": "Revenue",
                        "C2": "$340,000"
                    }
                }, indent=2)
            },
            {
                "role": "user",
                "content": """SHEET: Summary
LABELS: A5='Total Annual Revenue' | A10='Operating Margin'
FORMULA B5: =SUM(B2:B4) → 5000000
GRID:
  R5: A5:Total Annual Revenue[BOLD] | B5:5000000[CALC,HIGHLIGHT]

Query: What is total annual revenue?
Expected type: currency"""
            },
            {
                "role": "assistant",
                "content": json.dumps({
                    "found": True,
                    "value": 5000000,
                    "sheet_name": "Summary",
                    "cell_reference": "B5",
                    "data_type": "currency",
                    "confidence": 1.0,
                    "reasoning": "1. Found exact label match 'Total Annual Revenue' in A5 (bold formatting confirms it's a label). 2. Adjacent cell B5 contains calculated value 5,000,000. 3. Cell is highlighted, indicating it's a key result. 4. Formula =SUM(B2:B4) confirms this aggregates revenue data.",
                    "verification_steps": [
                        "Exact text match: 'Total Annual Revenue' in A5",
                        "A5 is bold, confirming it's a row label",
                        "B5 is in same row as label",
                        "B5 has formula calculating sum",
                        "B5 is highlighted as important value"
                    ],
                    "alternative_interpretations": None,
                    "formula_if_applicable": "=SUM(B2:B4)",
                    "context_cells": {
                        "A5": "Total Annual Revenue",
                        "B2": "Q1 Revenue",
                        "B3": "Q2 Revenue",
                        "B4": "Q3 Revenue"
                    }
                }, indent=2)
            }
        ]
    
    def extract_with_verification(self,
                                  file_path: str,
                                  query: str,
                                  expected_type: str = "auto",
                                  num_passes: int = 2) -> MultiPassResult:
        """
        Multi-pass extraction with verification for maximum precision
        
        Args:
            file_path: Path to Excel file
            query: Natural language query
            expected_type: Expected data type
            num_passes: Number of independent extraction passes (2-3 recommended)
        """
        
        print(f"\n{'='*80}")
        print(f"PRECISION EXTRACTION: {query}")
        print(f"{'='*80}\n")
        
        # Load data once
        print("Loading Excel file...")
        all_sheets = self.read_all_sheets_optimized(file_path)
        print(f"✓ Loaded {len(all_sheets['sheet_names'])} sheets: {', '.join(all_sheets['sheet_names'])}")
        
        # Build optimized context
        print("\nBuilding precision context...")
        excel_context = self.build_precision_context(all_sheets, query, self.max_context_tokens)
        context_tokens = self.count_tokens(excel_context)
        print(f"✓ Context: {context_tokens:,} tokens")
        
        # System prompt with strict instructions
        system_prompt = """You are a PRECISION Excel data extraction system. Your responses must be PERFECT.

CRITICAL REQUIREMENTS:
1. Extract EXACT values - no approximations, no rounding
2. Find the PRECISE cell reference
3. Provide STEP-BY-STEP verification of your answer
4. If multiple interpretations exist, list ALL of them
5. Confidence must reflect true certainty (use <1.0 if ANY doubt exists)

EXTRACTION PROCESS:
1. Parse the query to understand what is being asked
2. Identify relevant headers, labels, and structure
3. Locate the exact cell(s) containing the answer
4. Verify surrounding context confirms this is correct
5. Check for formulas or calculations
6. Double-check no other cells could match the query

OUTPUT: Valid JSON matching ExtractionResult schema."""
        
        # Few-shot examples
        few_shot = self.create_few_shot_examples()
        
        # Perform multiple independent passes
        pass_results = []
        
        for pass_num in range(1, num_passes + 1):
            print(f"\n--- Pass {pass_num}/{num_passes} ---")
            
            # Slight variation in temperature for independent reasoning
            temp = 0.0 if pass_num == 1 else 0.1
            
            messages = [
                {"role": "system", "content": system_prompt}
            ] + few_shot + [
                {"role": "user", "content": f"""{excel_context}

=== EXTRACTION TASK (Pass {pass_num}) ===
Query: {query}
Expected Type: {expected_type}

Provide your extraction in valid JSON format matching the ExtractionResult schema.
Be MAXIMALLY PRECISE. Include detailed verification steps."""}
            ]
            
            # Calculate token usage
            total_input = sum(self.count_tokens(m["content"]) for m in messages)
            print(f"Input tokens: {total_input:,}")
            
            # API call
            if self.azure_endpoint:
                response = openai.ChatCompletion.create(
                    engine=self.model,
                    messages=messages,
                    temperature=temp,
                    max_tokens=3000,
                    response_format={"type": "json_object"}  # Enforce JSON
                )
            else:
                response = openai.ChatCompletion.create(
                    model=self.model,
                    messages=messages,
                    temperature=temp,
                    max_tokens=3000,
                    response_format={"type": "json_object"}
                )
            
            result_text = response.choices[0].message.content.strip()
            
            # Parse and validate with Pydantic
            try:
                result_dict = json.loads(result_text)
                result = ExtractionResult(**result_dict)
                pass_results.append(result)
                
                print(f"✓ Found: {result.found}")
                print(f"✓ Value: {result.value}")
                print(f"✓ Cell: {result.cell_reference}")
                print(f"✓ Confidence: {result.confidence:.2%}")
                
            except Exception as e:
                print(f"✗ Parse error: {e}")
                print(f"Raw response: {result_text[:200]}")
                # Create error result
                error_result = ExtractionResult(
                    found=False,
                    value=None,
                    sheet_name=None,
                    cell_reference=None,
                    data_type=DataType.TEXT,
                    confidence=0.0,
                    reasoning=f"Parse error: {str(e)}",
                    verification_steps=[]
                )
                pass_results.append(error_result)
        
        # Consensus analysis
        print(f"\n{'='*80}")
        print("CONSENSUS ANALYSIS")
        print(f"{'='*80}")
        
        discrepancies = []
        
        # Check if all passes agree
        values = [r.value for r in pass_results if r.found]
        cells = [r.cell_reference for r in pass_results if r.found]
        
        if len(set(str(v) for v in values)) > 1:
            discrepancies.append(f"Value disagreement: {values}")
        
        if len(set(str(c) for c in cells)) > 1:
            discrepancies.append(f"Cell disagreement: {cells}")
        
        # Determine consensus
        if not discrepancies and values:
            consensus_value = values[0]
            consensus_confidence = min(r.confidence for r in pass_results)
            final_verified = True
            print("✓ All passes agree - HIGH CONFIDENCE")
        elif values:
            # Take most common value
            from collections import Counter
            consensus_value = Counter(values).most_common(1)[0][0]
            consensus_confidence = 0.7  # Reduced due to disagreement
            final_verified = False
            print("⚠ Passes disagree - MEDIUM CONFIDENCE")
            for disc in discrepancies:
                print(f"  - {disc}")
        else:
            consensus_value = None
            consensus_confidence = 0.0
            final_verified = False
            print("✗ No value found - FAILED")
        
        print(f"\nFinal Value: {consensus_value}")
        print(f"Final Confidence: {consensus_confidence:.2%}")
        
        return MultiPassResult(
            pass_1_result=pass_results[0],
            pass_2_result=pass_results[1],
            pass_3_result=pass_results[2] if len(pass_results) > 2 else None,
            consensus_value=consensus_value,
            consensus_confidence=consensus_confidence,
            discrepancies=discrepancies,
            final_verified=final_verified
        )

# ============================================================================
# USAGE
# ============================================================================

def main():
    extractor = PrecisionExcelExtractor(
        api_key="your-azure-api-key",
        model="gpt-4",
        azure_endpoint="https://your-resource.openai.azure.com/",
        max_context_tokens=120000
    )
    
    # Single extraction with verification
    result = extractor.extract_with_verification(
        file_path="financial_report.xlsx",
        query="What is the total revenue for Q4 2024?",
        expected_type="currency",
        num_passes=2  # Run twice for verification
    )
    
    print(f"\n{'='*80}")
    print("FINAL RESULT")
    print(f"{'='*80}")
    print(f"Value: {result.consensus_value}")
    print(f"Confidence: {result.consensus_confidence:.2%}")
    print(f"Verified: {result.final_verified}")
    print(f"\nPass 1: {result.pass_1_result.value} (conf: {result.pass_1_result.confidence:.2%})")
    print(f"Pass 2: {result.pass_2_result.value} (conf: {result.pass_2_result.confidence:.2%})")
    
    if result.discrepancies:
        print(f"\nDiscrepancies:")
        for disc in result.discrepancies:
            print(f"  - {disc}")
    
    # Export detailed results
    output = {
        "query": "What is the total revenue for Q4 2024?",
        "consensus_value": result.consensus_value,
        "consensus_confidence": result.consensus_confidence,
        "verified": result.final_verified,
        "pass_1": result.pass_1_result.dict(),
        "pass_2": result.pass_2_result.dict(),
        "discrepancies": result.discrepancies
    }
    
    with open('precision_extraction_result.json', 'w') as f:
        json.dump(output, f, indent=2, default=str)
    
    print("\n✓ Detailed results saved to precision_extraction_result.json")

if __name__ == "__main__":
    main()
