import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import json
import openai
from pathlib import Path
import openpyxl
from openpyxl.utils import get_column_letter
import re

class PreciseExcelExtractor:
    """
    Extract precise values from semi-structured Excel files using LLM with full data context.
    Designed for financial spreadsheets with variable layouts.
    """
    
    def __init__(self, api_key: str, model: str = "gpt-4", azure_endpoint: Optional[str] = None):
        """
        Initialize the extractor with OpenAI or Azure OpenAI credentials.
        
        Args:
            api_key: OpenAI API key or Azure API key
            model: Model name (e.g., 'gpt-4', 'gpt-4-turbo')
            azure_endpoint: Azure OpenAI endpoint URL (optional)
        """
        if azure_endpoint:
            # For Azure OpenAI (Databricks compatible)
            openai.api_type = "azure"
            openai.api_base = azure_endpoint
            openai.api_version = "2024-02-15-preview"
            openai.api_key = api_key
        else:
            # For standard OpenAI
            openai.api_key = api_key
            
        self.model = model
        self.azure_endpoint = azure_endpoint
    
    def read_excel_with_metadata(self, file_path: str, sheet_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Read Excel file and extract comprehensive metadata including cell formatting,
        formulas, merged cells, and spatial relationships.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Specific sheet name (if None, reads first sheet)
            
        Returns:
            Dictionary containing raw data, metadata, and structural information
        """
        wb = openpyxl.load_workbook(file_path, data_only=False)
        
        if sheet_name:
            ws = wb[sheet_name]
        else:
            ws = wb.active
            
        # Extract all data with positions
        cell_data = []
        formulas = {}
        merged_ranges = []
        formatting_info = {}
        
        # Get merged cell ranges
        for merged_range in ws.merged_cells.ranges:
            merged_ranges.append(str(merged_range))
        
        # Extract all cell data with metadata
        for row_idx, row in enumerate(ws.iter_rows(), start=1):
            for col_idx, cell in enumerate(row, start=1):
                cell_ref = f"{get_column_letter(col_idx)}{row_idx}"
                value = cell.value
                
                if value is not None:
                    cell_info = {
                        "cell": cell_ref,
                        "row": row_idx,
                        "col": col_idx,
                        "value": value,
                        "data_type": str(type(value).__name__),
                        "is_bold": cell.font.bold if cell.font else False,
                        "is_italic": cell.font.italic if cell.font else False,
                        "font_size": cell.font.size if cell.font else None,
                        "has_fill": cell.fill.start_color.index != "00000000" if cell.fill else False,
                        "alignment": cell.alignment.horizontal if cell.alignment else None
                    }
                    
                    # Track formulas separately
                    if isinstance(value, str) and value.startswith('='):
                        formulas[cell_ref] = value
                        cell_info["is_formula"] = True
                        # Also get calculated value
                        ws_data_only = openpyxl.load_workbook(file_path, data_only=True).active
                        cell_info["calculated_value"] = ws_data_only[cell_ref].value
                    else:
                        cell_info["is_formula"] = False
                    
                    cell_data.append(cell_info)
        
        # Create a grid representation
        max_row = ws.max_row
        max_col = ws.max_column
        
        grid = []
        for row_idx in range(1, max_row + 1):
            row_data = []
            for col_idx in range(1, max_col + 1):
                cell = ws.cell(row_idx, col_idx)
                row_data.append(str(cell.value) if cell.value is not None else "")
            grid.append(row_data)
        
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(grid)
        
        return {
            "file_path": file_path,
            "sheet_name": ws.title,
            "dimensions": {"rows": max_row, "cols": max_col},
            "cell_data": cell_data,
            "formulas": formulas,
            "merged_ranges": merged_ranges,
            "grid": grid,
            "dataframe": df,
            "available_sheets": wb.sheetnames
        }
    
    def create_structured_context(self, excel_data: Dict[str, Any]) -> str:
        """
        Create a comprehensive structured text representation of the Excel data
        that can be fed to the LLM for precise extraction.
        
        Args:
            excel_data: Dictionary from read_excel_with_metadata
            
        Returns:
            Formatted string with complete Excel context
        """
        context_parts = []
        
        # Basic information
        context_parts.append(f"=== EXCEL FILE METADATA ===")
        context_parts.append(f"File: {excel_data['file_path']}")
        context_parts.append(f"Sheet: {excel_data['sheet_name']}")
        context_parts.append(f"Dimensions: {excel_data['dimensions']['rows']} rows × {excel_data['dimensions']['cols']} columns")
        context_parts.append(f"Available sheets: {', '.join(excel_data['available_sheets'])}")
        context_parts.append("")
        
        # Merged cells (important for headers)
        if excel_data['merged_ranges']:
            context_parts.append("=== MERGED CELLS ===")
            for merged in excel_data['merged_ranges']:
                context_parts.append(f"- {merged}")
            context_parts.append("")
        
        # Grid representation (the actual visible spreadsheet)
        context_parts.append("=== SPREADSHEET GRID (Row-by-Row) ===")
        df = excel_data['dataframe']
        
        # Add column headers (A, B, C, etc.)
        col_letters = [get_column_letter(i+1) for i in range(len(df.columns))]
        context_parts.append("   | " + " | ".join(f"{col:^10}" for col in col_letters))
        context_parts.append("---|-" + "-|-".join("-" * 10 for _ in col_letters))
        
        # Add each row with row number
        for idx, row in df.iterrows():
            row_num = idx + 1
            row_values = [str(val)[:10].ljust(10) if val else " " * 10 for val in row]
            context_parts.append(f"{row_num:2} | " + " | ".join(row_values))
        
        context_parts.append("")
        
        # Cell-by-cell detailed information (with formatting)
        context_parts.append("=== DETAILED CELL INFORMATION ===")
        
        # Group by rows for better readability
        cells_by_row = {}
        for cell_info in excel_data['cell_data']:
            row = cell_info['row']
            if row not in cells_by_row:
                cells_by_row[row] = []
            cells_by_row[row].append(cell_info)
        
        for row_num in sorted(cells_by_row.keys()):
            context_parts.append(f"\nRow {row_num}:")
            for cell_info in cells_by_row[row_num]:
                cell_desc = f"  {cell_info['cell']}: {cell_info['value']}"
                
                # Add formatting indicators
                format_tags = []
                if cell_info['is_bold']:
                    format_tags.append("BOLD")
                if cell_info['is_italic']:
                    format_tags.append("ITALIC")
                if cell_info['has_fill']:
                    format_tags.append("HIGHLIGHTED")
                if cell_info['is_formula']:
                    format_tags.append(f"FORMULA→{cell_info.get('calculated_value', 'N/A')}")
                
                if format_tags:
                    cell_desc += f" [{', '.join(format_tags)}]"
                
                context_parts.append(cell_desc)
        
        # Formulas section
        if excel_data['formulas']:
            context_parts.append("\n=== FORMULAS ===")
            for cell_ref, formula in excel_data['formulas'].items():
                context_parts.append(f"{cell_ref}: {formula}")
            context_parts.append("")
        
        return "\n".join(context_parts)
    
    def extract_with_query(self, 
                          excel_data: Dict[str, Any], 
                          query: str,
                          expected_type: str = "auto") -> Dict[str, Any]:
        """
        Extract precise values from Excel data using natural language query.
        Feeds complete Excel structure and data to the model.
        
        Args:
            excel_data: Dictionary from read_excel_with_metadata
            query: Natural language query (e.g., "What is the total revenue for Q4 2024?")
            expected_type: Expected data type ('number', 'currency', 'percentage', 'date', 'text', 'auto')
            
        Returns:
            Dictionary with extracted value, confidence, location, and reasoning
        """
        # Create comprehensive context
        excel_context = self.create_structured_context(excel_data)
        
        # Build the prompt
        system_prompt = """You are a precise financial data extraction assistant. Your task is to:

1. Analyze the complete Excel spreadsheet structure and data provided
2. Understand the user's query and identify exactly what value they need
3. Locate the precise cell(s) containing the answer
4. Extract the exact value with complete accuracy
5. Provide the cell reference, value, and confidence level

RULES:
- Use ONLY the data provided in the spreadsheet context
- Return exact values, not approximations
- If a value is calculated, show the formula and result
- If multiple cells match, explain which one is most relevant
- Consider cell formatting (bold headers, highlighted cells) as structural hints
- Pay attention to merged cells for section headers
- If the query is ambiguous or data is missing, explain why

Return your answer in JSON format:
{
    "found": true/false,
    "value": <extracted_value>,
    "cell_reference": "A1" or ["A1", "A2"] for multiple cells,
    "data_type": "number"/"currency"/"percentage"/"date"/"text",
    "confidence": 0.0-1.0,
    "reasoning": "Explanation of why this is the correct value",
    "context": "Relevant surrounding cells or headers that confirm this is correct",
    "formula_used": "If the cell contains a formula" or null
}"""
        
        user_prompt = f"""
{excel_context}

=== USER QUERY ===
{query}

Expected data type: {expected_type}

Please extract the precise value that answers this query. Analyze the complete spreadsheet structure above and return your response in the JSON format specified.
"""
        
        # Call the LLM
        if self.azure_endpoint:
            response = openai.ChatCompletion.create(
                engine=self.model,  # Azure uses 'engine' instead of 'model'
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.0,  # Maximum precision
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
        
        # Extract JSON from response (handle markdown code blocks)
        json_match = re.search(r'``````', result_text, re.DOTALL)
        if json_match:
            result_json = json.loads(json_match.group(1))
        else:
            # Try to parse the entire response as JSON
            try:
                result_json = json.loads(result_text)
            except json.JSONDecodeError:
                # Fallback: create structured response
                result_json = {
                    "found": False,
                    "value": None,
                    "cell_reference": None,
                    "confidence": 0.0,
                    "reasoning": "Failed to parse LLM response",
                    "raw_response": result_text
                }
        
        return result_json
    
    def batch_extract(self, 
                     excel_data: Dict[str, Any], 
                     queries: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """
        Extract multiple values from the same Excel file efficiently.
        
        Args:
            excel_data: Dictionary from read_excel_with_metadata
            queries: List of dicts with 'query' and optional 'expected_type' keys
            
        Returns:
            List of extraction results
        """
        results = []
        for query_info in queries:
            query = query_info['query']
            expected_type = query_info.get('expected_type', 'auto')
            
            result = self.extract_with_query(excel_data, query, expected_type)
            result['original_query'] = query
            results.append(result)
        
        return results
    
    def validate_extraction(self, 
                           excel_data: Dict[str, Any],
                           extraction_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate extracted value by checking cell reference and data type.
        
        Args:
            excel_data: Dictionary from read_excel_with_metadata
            extraction_result: Result from extract_with_query
            
        Returns:
            Validation result with verification status
        """
        validation = {
            "is_valid": False,
            "cell_exists": False,
            "value_matches": False,
            "actual_cell_value": None,
            "issues": []
        }
        
        if not extraction_result.get('found'):
            validation['issues'].append("No value was found by extraction")
            return validation
        
        cell_ref = extraction_result.get('cell_reference')
        extracted_value = extraction_result.get('value')
        
        if not cell_ref:
            validation['issues'].append("No cell reference provided")
            return validation
        
        # Handle multiple cell references
        if isinstance(cell_ref, list):
            cell_ref = cell_ref[0]
        
        # Find the cell in the data
        cell_info = next((c for c in excel_data['cell_data'] if c['cell'] == cell_ref), None)
        
        if cell_info:
            validation['cell_exists'] = True
            actual_value = cell_info.get('calculated_value', cell_info['value'])
            validation['actual_cell_value'] = actual_value
            
            # Compare values (with type coercion)
            try:
                if str(extracted_value).strip() == str(actual_value).strip():
                    validation['value_matches'] = True
                    validation['is_valid'] = True
                else:
                    validation['issues'].append(
                        f"Value mismatch: extracted '{extracted_value}' vs actual '{actual_value}'"
                    )
            except Exception as e:
                validation['issues'].append(f"Error comparing values: {str(e)}")
        else:
            validation['issues'].append(f"Cell {cell_ref} not found in spreadsheet")
        
        return validation


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

def main():
    """
    Complete example showing how to use the PreciseExcelExtractor
    """
    
    # Initialize extractor
    # For Azure OpenAI (Databricks):
    extractor = PreciseExcelExtractor(
        api_key="your-azure-api-key",
        model="gpt-4",  # or your deployment name
        azure_endpoint="https://your-resource.openai.azure.com/"
    )
    
    # Or for standard OpenAI:
    # extractor = PreciseExcelExtractor(
    #     api_key="your-openai-api-key",
    #     model="gpt-4-turbo"
    # )
    
    # Read Excel file with complete metadata
    excel_file = "financial_report.xlsx"
    excel_data = extractor.read_excel_with_metadata(excel_file, sheet_name="Summary")
    
    print(f"Loaded Excel: {excel_data['sheet_name']}")
    print(f"Dimensions: {excel_data['dimensions']['rows']} x {excel_data['dimensions']['cols']}")
    print(f"Total cells with data: {len(excel_data['cell_data'])}")
    print("\n" + "="*80 + "\n")
    
    # Single query extraction
    print("=== SINGLE EXTRACTION ===")
    result = extractor.extract_with_query(
        excel_data=excel_data,
        query="What is the total revenue for Q4 2024?",
        expected_type="currency"
    )
    
    print(f"Found: {result['found']}")
    print(f"Value: {result['value']}")
    print(f"Cell: {result['cell_reference']}")
    print(f"Confidence: {result['confidence']:.2%}")
    print(f"Reasoning: {result['reasoning']}")
    print(f"Context: {result.get('context', 'N/A')}")
    print("\n" + "="*80 + "\n")
    
    # Validate the extraction
    validation = extractor.validate_extraction(excel_data, result)
    print(f"Validation - Valid: {validation['is_valid']}")
    print(f"Actual cell value: {validation['actual_cell_value']}")
    if validation['issues']:
        print(f"Issues: {', '.join(validation['issues'])}")
    print("\n" + "="*80 + "\n")
    
    # Batch extraction
    print("=== BATCH EXTRACTION ===")
    queries = [
        {"query": "What is the total revenue for Q4 2024?", "expected_type": "currency"},
        {"query": "What is the operating margin percentage?", "expected_type": "percentage"},
        {"query": "What is the date of this report?", "expected_type": "date"},
        {"query": "How many employees does the company have?", "expected_type": "number"},
        {"query": "What is the company name?", "expected_type": "text"}
    ]
    
    batch_results = extractor.batch_extract(excel_data, queries)
    
    for i, result in enumerate(batch_results, 1):
        print(f"\n{i}. Query: {result['original_query']}")
        print(f"   Answer: {result['value']}")
        print(f"   Cell: {result['cell_reference']}")
        print(f"   Confidence: {result['confidence']:.2%}")
    
    # Export results
    results_df = pd.DataFrame([
        {
            'query': r['original_query'],
            'value': r['value'],
            'cell': r['cell_reference'],
            'confidence': r['confidence'],
            'data_type': r.get('data_type', 'unknown')
        }
        for r in batch_results
    ])
    
    results_df.to_csv('extraction_results.csv', index=False)
    print("\n\nResults saved to extraction_results.csv")


if __name__ == "__main__":
    main()
