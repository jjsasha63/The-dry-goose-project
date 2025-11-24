import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import json
import openai
from pathlib import Path
import openpyxl
from openpyxl.utils import get_column_letter
import re
import tiktoken
from collections import defaultdict

class MultiSheetExcelExtractor:
    """
    Extract precise values from multi-sheet Excel files with context optimization.
    Keeps total context under 128K tokens through intelligent summarization.
    """
    
    def __init__(self, 
                 api_key: str, 
                 model: str = "gpt-4", 
                 azure_endpoint: Optional[str] = None,
                 max_context_tokens: int = 120000):  # Leave 8K buffer for response
        """
        Initialize the extractor with token limits.
        
        Args:
            api_key: OpenAI API key or Azure API key
            model: Model name (e.g., 'gpt-4', 'gpt-4-turbo')
            azure_endpoint: Azure OpenAI endpoint URL (optional)
            max_context_tokens: Maximum tokens for context (default 120K, leaves 8K for response)
        """
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
        
        # Initialize tokenizer
        try:
            self.encoding = tiktoken.encoding_for_model(model)
        except KeyError:
            self.encoding = tiktoken.get_encoding("cl100k_base")
    
    def count_tokens(self, text: str) -> int:
        """Count tokens in a text string."""
        return len(self.encoding.encode(text))
    
    def read_all_sheets(self, file_path: str, 
                       max_rows_per_sheet: Optional[int] = None) -> Dict[str, Any]:
        """
        Read all sheets from Excel file with comprehensive metadata.
        
        Args:
            file_path: Path to Excel file
            max_rows_per_sheet: Optional limit on rows per sheet (for very large files)
            
        Returns:
            Dictionary containing data from all sheets
        """
        wb = openpyxl.load_workbook(file_path, data_only=False, read_only=False)
        wb_data_only = openpyxl.load_workbook(file_path, data_only=True, read_only=True)
        
        all_sheets_data = {
            "file_path": file_path,
            "total_sheets": len(wb.sheetnames),
            "sheet_names": wb.sheetnames,
            "sheets": {}
        }
        
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            ws_data_only = wb_data_only[sheet_name]
            
            # Basic dimensions
            max_row = ws.max_row if not max_rows_per_sheet else min(ws.max_row, max_rows_per_sheet)
            max_col = ws.max_column
            
            # Extract cell data
            cell_data = []
            formulas = {}
            merged_ranges = [str(r) for r in ws.merged_cells.ranges]
            
            for row_idx in range(1, max_row + 1):
                for col_idx in range(1, max_col + 1):
                    cell = ws.cell(row_idx, col_idx)
                    value = cell.value
                    
                    if value is not None:
                        cell_ref = f"{get_column_letter(col_idx)}{row_idx}"
                        
                        cell_info = {
                            "cell": cell_ref,
                            "row": row_idx,
                            "col": col_idx,
                            "value": value,
                            "is_bold": cell.font.bold if cell.font else False,
                            "has_fill": cell.fill.start_color.index != "00000000" if cell.fill else False,
                        }
                        
                        # Handle formulas
                        if isinstance(value, str) and value.startswith('='):
                            formulas[cell_ref] = value
                            cell_info["is_formula"] = True
                            cell_info["calculated_value"] = ws_data_only.cell(row_idx, col_idx).value
                        else:
                            cell_info["is_formula"] = False
                        
                        cell_data.append(cell_info)
            
            # Create grid representation
            grid = []
            for row_idx in range(1, max_row + 1):
                row_data = []
                for col_idx in range(1, max_col + 1):
                    cell = ws.cell(row_idx, col_idx)
                    row_data.append(str(cell.value) if cell.value is not None else "")
                grid.append(row_data)
            
            all_sheets_data["sheets"][sheet_name] = {
                "dimensions": {"rows": max_row, "cols": max_col},
                "cell_data": cell_data,
                "formulas": formulas,
                "merged_ranges": merged_ranges,
                "grid": grid,
                "dataframe": pd.DataFrame(grid)
            }
        
        wb.close()
        wb_data_only.close()
        
        return all_sheets_data
    
    def create_compact_sheet_summary(self, sheet_data: Dict[str, Any], 
                                    sheet_name: str) -> str:
        """
        Create a compact summary of a single sheet optimized for token efficiency.
        """
        summary_parts = []
        
        # Basic info (very compact)
        dims = sheet_data['dimensions']
        summary_parts.append(f"## {sheet_name} [{dims['rows']}x{dims['cols']}]")
        
        # Identify key structural elements
        headers = []
        bold_cells = []
        formula_cells = []
        
        for cell_info in sheet_data['cell_data']:
            if cell_info['is_bold'] and cell_info['row'] <= 3:
                headers.append(f"{cell_info['cell']}:{cell_info['value']}")
            elif cell_info['is_bold']:
                bold_cells.append(f"{cell_info['cell']}:{cell_info['value']}")
            if cell_info['is_formula']:
                calc_val = cell_info.get('calculated_value', 'N/A')
                formula_cells.append(f"{cell_info['cell']}={calc_val}")
        
        if headers:
            summary_parts.append(f"Headers: {', '.join(headers[:10])}")
        if bold_cells[:5]:
            summary_parts.append(f"Key labels: {', '.join(bold_cells[:5])}")
        if formula_cells[:5]:
            summary_parts.append(f"Calculations: {', '.join(formula_cells[:5])}")
        
        # Sample grid (first 10 rows, essential columns only)
        df = sheet_data['dataframe']
        non_empty_cols = [i for i in range(len(df.columns)) 
                         if df.iloc[:, i].astype(str).str.strip().ne('').any()][:10]
        
        if non_empty_cols:
            summary_parts.append("\nData preview:")
            for idx in range(min(10, len(df))):
                row_data = [f"{get_column_letter(col+1)}{idx+1}:{str(df.iloc[idx, col])[:20]}" 
                           for col in non_empty_cols if str(df.iloc[idx, col]).strip()]
                if row_data:
                    summary_parts.append(f"  R{idx+1}: {', '.join(row_data)}")
        
        return "\n".join(summary_parts)
    
    def create_detailed_sheet_context(self, sheet_data: Dict[str, Any], 
                                     sheet_name: str,
                                     max_tokens: int) -> str:
        """
        Create detailed context for a single sheet with token limit.
        """
        context_parts = []
        
        # Header
        dims = sheet_data['dimensions']
        context_parts.append(f"=== SHEET: {sheet_name} ===")
        context_parts.append(f"Dimensions: {dims['rows']} rows × {dims['cols']} columns\n")
        
        # Full grid with cell references
        df = sheet_data['dataframe']
        col_letters = [get_column_letter(i+1) for i in range(len(df.columns))]
        
        context_parts.append("GRID:")
        context_parts.append("    " + " | ".join(f"{col:^8}" for col in col_letters))
        context_parts.append("----" + "-|-".join("-" * 8 for _ in col_letters))
        
        for idx, row in df.iterrows():
            row_num = idx + 1
            row_values = [str(val)[:8].ljust(8) if val else " " * 8 for val in row]
            row_text = f"{row_num:3} | " + " | ".join(row_values)
            
            # Check token budget
            current_text = "\n".join(context_parts + [row_text])
            if self.count_tokens(current_text) > max_tokens:
                context_parts.append(f"... ({dims['rows'] - idx} more rows)")
                break
            
            context_parts.append(row_text)
        
        # Add cell metadata if space allows
        current_tokens = self.count_tokens("\n".join(context_parts))
        if current_tokens < max_tokens * 0.8:  # Use up to 80% of budget
            context_parts.append("\nKEY CELLS:")
            for cell_info in sheet_data['cell_data']:
                if cell_info['is_bold'] or cell_info['is_formula'] or cell_info['has_fill']:
                    tags = []
                    if cell_info['is_bold']:
                        tags.append("BOLD")
                    if cell_info['has_fill']:
                        tags.append("HIGHLIGHTED")
                    if cell_info['is_formula']:
                        tags.append(f"={cell_info.get('calculated_value', 'N/A')}")
                    
                    cell_line = f"  {cell_info['cell']}: {cell_info['value']} [{', '.join(tags)}]"
                    
                    test_text = "\n".join(context_parts + [cell_line])
                    if self.count_tokens(test_text) > max_tokens:
                        break
                    
                    context_parts.append(cell_line)
        
        return "\n".join(context_parts)
    
    def optimize_context_distribution(self, 
                                     all_sheets_data: Dict[str, Any],
                                     query: str,
                                     target_tokens: int) -> str:
        """
        Intelligently distribute token budget across all sheets based on relevance.
        """
        context_parts = []
        
        # File-level metadata (minimal tokens)
        context_parts.append(f"=== EXCEL FILE: {all_sheets_data['file_path']} ===")
        context_parts.append(f"Total Sheets: {all_sheets_data['total_sheets']}")
        context_parts.append(f"Sheet Names: {', '.join(all_sheets_data['sheet_names'])}\n")
        
        # Reserve tokens for system prompt and query
        header_tokens = self.count_tokens("\n".join(context_parts))
        query_tokens = self.count_tokens(query)
        system_tokens = 500  # Approximate
        available_tokens = target_tokens - header_tokens - query_tokens - system_tokens
        
        # First pass: create compact summaries for all sheets
        summaries = {}
        summary_tokens = 0
        for sheet_name, sheet_data in all_sheets_data["sheets"].items():
            summary = self.create_compact_sheet_summary(sheet_data, sheet_name)
            summaries[sheet_name] = summary
            summary_tokens += self.count_tokens(summary)
        
        # Determine strategy based on total size
        if summary_tokens < available_tokens * 0.3:
            # Strategy 1: Include all summaries + detailed view of relevant sheets
            context_parts.append("=== ALL SHEETS SUMMARY ===")
            for sheet_name, summary in summaries.items():
                context_parts.append(summary + "\n")
            
            # Use remaining budget for detailed sheets
            used_tokens = self.count_tokens("\n".join(context_parts))
            remaining_tokens = available_tokens - used_tokens
            
            # Identify potentially relevant sheets based on query keywords
            query_lower = query.lower()
            relevant_sheets = []
            for sheet_name, sheet_data in all_sheets_data["sheets"].items():
                # Check if sheet name or content matches query
                relevance_score = 0
                if any(word in sheet_name.lower() for word in query_lower.split()):
                    relevance_score += 10
                
                # Check cell values for query keywords
                for cell_info in sheet_data['cell_data'][:100]:  # Sample first 100 cells
                    cell_val = str(cell_info['value']).lower()
                    if any(word in cell_val for word in query_lower.split() if len(word) > 3):
                        relevance_score += 1
                
                if relevance_score > 0:
                    relevant_sheets.append((sheet_name, relevance_score))
            
            relevant_sheets.sort(key=lambda x: x[1], reverse=True)
            
            # Add detailed context for most relevant sheets
            if relevant_sheets:
                context_parts.append("\n=== DETAILED VIEWS (Most Relevant) ===\n")
                tokens_per_sheet = remaining_tokens // min(len(relevant_sheets), 3)
                
                for sheet_name, _ in relevant_sheets[:3]:
                    sheet_data = all_sheets_data["sheets"][sheet_name]
                    detailed = self.create_detailed_sheet_context(
                        sheet_data, sheet_name, tokens_per_sheet
                    )
                    context_parts.append(detailed + "\n")
        
        else:
            # Strategy 2: Proportional token allocation across all sheets
            tokens_per_sheet = available_tokens // len(all_sheets_data["sheets"])
            
            for sheet_name, sheet_data in all_sheets_data["sheets"].items():
                detailed = self.create_detailed_sheet_context(
                    sheet_data, sheet_name, tokens_per_sheet
                )
                context_parts.append(detailed + "\n")
        
        final_context = "\n".join(context_parts)
        
        # Verify we're under budget
        final_tokens = self.count_tokens(final_context)
        if final_tokens > target_tokens:
            # Truncate if necessary (shouldn't happen, but safety measure)
            tokens = self.encoding.encode(final_context)
            truncated_tokens = tokens[:target_tokens]
            final_context = self.encoding.decode(truncated_tokens)
        
        return final_context
    
    def extract_from_all_sheets(self,
                               file_path: str,
                               query: str,
                               expected_type: str = "auto") -> Dict[str, Any]:
        """
        Extract values from multi-sheet Excel file with optimized context.
        
        Args:
            file_path: Path to Excel file
            query: Natural language query
            expected_type: Expected data type
            
        Returns:
            Extraction result with sheet information
        """
        # Read all sheets
        print(f"Reading Excel file: {file_path}")
        all_sheets_data = self.read_all_sheets(file_path)
        print(f"Loaded {all_sheets_data['total_sheets']} sheets: {', '.join(all_sheets_data['sheet_names'])}")
        
        # Create optimized context
        print(f"Optimizing context (target: {self.max_context_tokens} tokens)...")
        excel_context = self.optimize_context_distribution(
            all_sheets_data, query, self.max_context_tokens
        )
        
        context_tokens = self.count_tokens(excel_context)
        print(f"Context size: {context_tokens:,} tokens ({context_tokens/self.max_context_tokens*100:.1f}% of budget)")
        
        # Build system prompt
        system_prompt = """You are a precise multi-sheet Excel data extraction assistant.

TASK:
1. Analyze data across ALL sheets in the workbook
2. Locate the exact value that answers the user's query
3. Identify which sheet contains the answer
4. Return precise cell reference and value

RULES:
- Search across ALL sheets provided
- Return EXACT values, not approximations
- Include sheet name in your response
- Consider cross-sheet references and formulas
- If data spans multiple sheets, explain the relationship
- If ambiguous, specify which sheet and why

RESPONSE FORMAT (JSON):
{
    "found": true/false,
    "value": <extracted_value>,
    "sheet_name": "Sheet1",
    "cell_reference": "A1" or ["A1", "A2"],
    "data_type": "number"/"currency"/"percentage"/"date"/"text",
    "confidence": 0.0-1.0,
    "reasoning": "Why this is the correct value",
    "related_sheets": ["Sheet2", "Sheet3"] if data references other sheets,
    "cross_sheet_context": "Explanation if data involves multiple sheets"
}"""
        
        user_prompt = f"""{excel_context}

=== USER QUERY ===
{query}

Expected data type: {expected_type}

Extract the precise value answering this query. Search ALL sheets and return JSON response."""
        
        # Count total tokens
        system_tokens = self.count_tokens(system_prompt)
        user_tokens = self.count_tokens(user_prompt)
        total_input_tokens = system_tokens + user_tokens
        
        print(f"Total input tokens: {total_input_tokens:,}")
        print(f"  System: {system_tokens:,}")
        print(f"  User (context + query): {user_tokens:,}")
        
        if total_input_tokens > 128000:
            print(f"WARNING: Input exceeds 128K tokens! ({total_input_tokens:,})")
        
        # Call LLM
        print("Calling LLM...")
        if self.azure_endpoint:
            response = openai.ChatCompletion.create(
                engine=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.0,
                max_tokens=2000
            )
        else:
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.0,
                max_tokens=2000
            )
        
        # Parse response
        result_text = response.choices[0].message.content.strip()
        
        json_match = re.search(r'``````', result_text, re.DOTALL)
        if json_match:
            result_json = json.loads(json_match.group(1))
        else:
            try:
                result_json = json.loads(result_text)
            except json.JSONDecodeError:
                result_json = {
                    "found": False,
                    "value": None,
                    "sheet_name": None,
                    "cell_reference": None,
                    "confidence": 0.0,
                    "reasoning": "Failed to parse response",
                    "raw_response": result_text
                }
        
        # Add token usage info
        result_json["token_usage"] = {
            "input_tokens": total_input_tokens,
            "output_tokens": response.usage.completion_tokens,
            "total_tokens": response.usage.total_tokens
        }
        
        return result_json
    
    def batch_extract_multi_sheet(self,
                                 file_path: str,
                                 queries: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """
        Extract multiple values from multi-sheet Excel efficiently.
        Reads file once and reuses context.
        """
        print(f"Reading Excel file once for batch processing...")
        all_sheets_data = self.read_all_sheets(file_path)
        
        results = []
        for i, query_info in enumerate(queries, 1):
            print(f"\n--- Query {i}/{len(queries)} ---")
            query = query_info['query']
            expected_type = query_info.get('expected_type', 'auto')
            
            # Optimize context for this specific query
            excel_context = self.optimize_context_distribution(
                all_sheets_data, query, self.max_context_tokens
            )
            
            context_tokens = self.count_tokens(excel_context)
            print(f"Query: {query}")
            print(f"Context: {context_tokens:,} tokens")
            
            # Extract (rest of logic same as extract_from_all_sheets)
            # ... [Continue with LLM call using optimized context]
            
            result = self._extract_with_context(excel_context, query, expected_type)
            result['original_query'] = query
            results.append(result)
        
        return results
    
    def _extract_with_context(self, excel_context: str, query: str, 
                             expected_type: str) -> Dict[str, Any]:
        """Helper method for extraction with pre-built context."""
        system_prompt = """You are a precise multi-sheet Excel data extraction assistant... [same as above]"""
        
        user_prompt = f"""{excel_context}

=== USER QUERY ===
{query}

Expected data type: {expected_type}

Extract the precise value answering this query."""
        
        if self.azure_endpoint:
            response = openai.ChatCompletion.create(
                engine=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.0,
                max_tokens=2000
            )
        else:
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.0,
                max_tokens=2000
            )
        
        result_text = response.choices[0].message.content.strip()
        json_match = re.search(r'``````', result_text, re.DOTALL)
        
        if json_match:
            return json.loads(json_match.group(1))
        else:
            try:
                return json.loads(result_text)
            except:
                return {"found": False, "error": "Parse failed", "raw": result_text}


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

def main():
    """Complete example for multi-sheet Excel extraction"""
    
    # Initialize with token limit
    extractor = MultiSheetExcelExtractor(
        api_key="your-azure-api-key",
        model="gpt-4",
        azure_endpoint="https://your-resource.openai.azure.com/",
        max_context_tokens=120000  # 120K for context, 8K for response
    )
    
    # Single extraction across all sheets
    print("="*80)
    print("MULTI-SHEET EXTRACTION")
    print("="*80)
    
    result = extractor.extract_from_all_sheets(
        file_path="financial_report_2024.xlsx",
        query="What is the total revenue across all quarters?",
        expected_type="currency"
    )
    
    print(f"\nResult:")
    print(f"  Found: {result['found']}")
    print(f"  Value: {result['value']}")
    print(f"  Sheet: {result.get('sheet_name', 'N/A')}")
    print(f"  Cell: {result.get('cell_reference', 'N/A')}")
    print(f"  Confidence: {result.get('confidence', 0):.2%}")
    print(f"  Reasoning: {result.get('reasoning', 'N/A')}")
    
    if result.get('related_sheets'):
        print(f"  Related sheets: {', '.join(result['related_sheets'])}")
    
    print(f"\nToken Usage:")
    print(f"  Input: {result['token_usage']['input_tokens']:,}")
    print(f"  Output: {result['token_usage']['output_tokens']:,}")
    print(f"  Total: {result['token_usage']['total_tokens']:,}")
    
    # Batch extraction
    print("\n" + "="*80)
    print("BATCH EXTRACTION")
    print("="*80)
    
    queries = [
        {"query": "What is the Q1 revenue?", "expected_type": "currency"},
        {"query": "What is the total annual profit?", "expected_type": "currency"},
        {"query": "How many employees in the engineering department?", "expected_type": "number"},
        {"query": "What is the company's operating margin?", "expected_type": "percentage"}
    ]
    
    batch_results = extractor.batch_extract_multi_sheet(
        file_path="financial_report_2024.xlsx",
        queries=queries
    )
    
    # Export results
    results_df = pd.DataFrame([
        {
            'query': r['original_query'],
            'value': r['value'],
            'sheet': r.get('sheet_name', 'N/A'),
            'cell': r.get('cell_reference', 'N/A'),
            'confidence': r.get('confidence', 0)
        }
        for r in batch_results
    ])
    
    results_df.to_csv('multi_sheet_extraction_results.csv', index=False)
    print("\nResults saved to multi_sheet_extraction_results.csv")


if __name__ == "__main__":
    main()
