"""
SIMPLE WORKING EXCEL EXTRACTOR
Hybrid LLM + algorithmic approach that actually works
"""

import pandas as pd
import openpyxl
from openpyxl.utils import get_column_letter, column_index_from_string
import openai
import json
import re
from typing import Dict, Any, List, Optional

# ============================================================================
# CONFIGURATION
# ============================================================================

openai.api_key = "your-key-here"  # Set your OpenAI key

# ============================================================================
# STEP 1: READ EXCEL FILE (ALL SHEETS)
# ============================================================================

def read_excel_all_sheets(file_path: str) -> Dict[str, pd.DataFrame]:
    """
    Read all sheets from Excel file into pandas DataFrames.
    Returns dict of {sheet_name: dataframe}
    """
    excel_file = pd.ExcelFile(file_path)
    sheets = {}
    
    for sheet_name in excel_file.sheet_names:
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        sheets[sheet_name] = df
        print(f"✓ Loaded sheet '{sheet_name}': {df.shape[0]} rows × {df.shape[1]} cols")
    
    return sheets

# ============================================================================
# STEP 2: CREATE COMPACT SUMMARY FOR LLM
# ============================================================================

def create_sheet_summary(df: pd.DataFrame, sheet_name: str, max_rows: int = 30) -> str:
    """
    Create a compact text representation of a sheet.
    """
    lines = [f"SHEET: {sheet_name} ({df.shape[0]} rows × {df.shape[1]} cols)"]
    lines.append("")
    
    # Show first N rows
    preview_rows = min(max_rows, len(df))
    
    for row_idx in range(preview_rows):
        row = df.iloc[row_idx]
        # Format as: Row1: A=value, B=value, C=value, ...
        non_empty = []
        for col_idx, val in enumerate(row):
            if pd.notna(val) and str(val).strip():
                col_letter = get_column_letter(col_idx + 1)
                val_str = str(val)[:50]  # Truncate long values
                non_empty.append(f"{col_letter}={val_str}")
        
        if non_empty:
            lines.append(f"Row{row_idx+1}: {', '.join(non_empty)}")
    
    if len(df) > preview_rows:
        lines.append(f"... ({len(df) - preview_rows} more rows)")
    
    return "\n".join(lines)

def create_workbook_summary(sheets: Dict[str, pd.DataFrame]) -> str:
    """
    Create summary of entire workbook.
    """
    parts = [f"EXCEL WORKBOOK with {len(sheets)} sheets: {', '.join(sheets.keys())}"]
    parts.append("="*80)
    parts.append("")
    
    for sheet_name, df in sheets.items():
        parts.append(create_sheet_summary(df, sheet_name))
        parts.append("")
    
    return "\n".join(parts)

# ============================================================================
# STEP 3: LLM GENERATES EXTRACTION INSTRUCTIONS (NOT THE ANSWER)
# ============================================================================

SYSTEM_PROMPT = """You are an Excel data extraction planner. 

Your job is to analyze the Excel structure and tell the user EXACTLY where to find the data they're asking for.

Return a JSON object with this EXACT structure:
{
  "sheet_name": "the sheet name containing the answer",
  "cell_reference": "exact cell reference like B5 or range like B2:B10",
  "operation": "return_cell" or "sum" or "average" or "max" or "min" or "count",
  "reasoning": "brief explanation of why this cell/range contains the answer"
}

RULES:
- cell_reference must be in Excel format (A1, B5, C2:C10, etc.)
- operation must be one of: return_cell, sum, average, max, min, count
- Use "return_cell" for single values, other operations for ranges
- Be PRECISE with cell references

Examples:
Query: "What is the total revenue?"
Response: {"sheet_name": "Summary", "cell_reference": "B10", "operation": "return_cell", "reasoning": "Cell B10 contains 'Total Revenue' value"}

Query: "What is the sum of all sales in Q1?"
Response: {"sheet_name": "Q1_Sales", "cell_reference": "C2:C50", "operation": "sum", "reasoning": "Column C contains sales amounts from row 2 to 50"}
"""

def get_extraction_plan(workbook_summary: str, query: str) -> Dict[str, Any]:
    """
    Ask LLM to generate extraction plan (NOT compute the answer).
    """
    user_prompt = f"""{workbook_summary}

USER QUERY: {query}

Analyze the data above and return ONLY a JSON object telling me exactly where to find the answer.
Do NOT compute the answer yourself. Just tell me the cell reference and operation."""

    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.0,
        response_format={"type": "json_object"}
    )
    
    plan_str = response.choices[0].message.content
    plan = json.loads(plan_str)
    
    print("\n" + "="*80)
    print("LLM EXTRACTION PLAN:")
    print(json.dumps(plan, indent=2))
    print("="*80 + "\n")
    
    return plan

# ============================================================================
# STEP 4: EXECUTE PLAN ALGORITHMICALLY (GUARANTEED PRECISE)
# ============================================================================

def parse_cell_reference(cell_ref: str) -> tuple:
    """
    Parse cell reference like 'B5' into (row, col) as 0-indexed.
    Returns (row_idx, col_idx)
    """
    match = re.match(r'([A-Z]+)(\d+)', cell_ref.upper())
    if not match:
        raise ValueError(f"Invalid cell reference: {cell_ref}")
    
    col_letter = match.group(1)
    row_num = int(match.group(2))
    
    col_idx = column_index_from_string(col_letter) - 1  # Convert to 0-indexed
    row_idx = row_num - 1  # Convert to 0-indexed
    
    return row_idx, col_idx

def parse_range(range_ref: str) -> tuple:
    """
    Parse range like 'B2:B10' into (start_row, start_col, end_row, end_col) as 0-indexed.
    """
    if ':' not in range_ref:
        row, col = parse_cell_reference(range_ref)
        return row, col, row, col
    
    start_cell, end_cell = range_ref.split(':')
    start_row, start_col = parse_cell_reference(start_cell)
    end_row, end_col = parse_cell_reference(end_cell)
    
    return start_row, start_col, end_row, end_col

def execute_plan(sheets: Dict[str, pd.DataFrame], plan: Dict[str, Any]) -> Any:
    """
    Execute the extraction plan on actual Excel data.
    This is ALGORITHMIC - no LLM involved, guaranteed precision.
    """
    sheet_name = plan['sheet_name']
    cell_ref = plan['cell_reference']
    operation = plan['operation']
    
    if sheet_name not in sheets:
        raise ValueError(f"Sheet '{sheet_name}' not found. Available: {list(sheets.keys())}")
    
    df = sheets[sheet_name]
    
    print(f"Executing: {operation} on {sheet_name}!{cell_ref}")
    
    if operation == "return_cell":
        # Single cell lookup
        row_idx, col_idx = parse_cell_reference(cell_ref)
        value = df.iloc[row_idx, col_idx]
        print(f"✓ Found value at {cell_ref}: {value}")
        return value
    
    else:
        # Range operations (sum, average, etc.)
        start_row, start_col, end_row, end_col = parse_range(cell_ref)
        
        # Extract the range
        range_data = df.iloc[start_row:end_row+1, start_col:end_col+1]
        
        # Convert to numeric, coercing errors
        numeric_data = pd.to_numeric(range_data.values.flatten(), errors='coerce')
        numeric_data = numeric_data[~pd.isna(numeric_data)]  # Remove NaN
        
        print(f"✓ Extracted {len(numeric_data)} numeric values from range")
        
        if operation == "sum":
            result = float(numeric_data.sum())
        elif operation == "average":
            result = float(numeric_data.mean())
        elif operation == "max":
            result = float(numeric_data.max())
        elif operation == "min":
            result = float(numeric_data.min())
        elif operation == "count":
            result = int(len(numeric_data))
        else:
            raise ValueError(f"Unknown operation: {operation}")
        
        print(f"✓ {operation.upper()} = {result}")
        return result

# ============================================================================
# STEP 5: MAIN INTERFACE
# ============================================================================

def query_excel(file_path: str, query: str) -> Dict[str, Any]:
    """
    Main function: query Excel file in natural language.
    Returns the exact extracted value.
    """
    print("\n" + "="*80)
    print(f"QUERY: {query}")
    print("="*80 + "\n")
    
    # Step 1: Load all sheets
    print("STEP 1: Loading Excel file...")
    sheets = read_excel_all_sheets(file_path)
    
    # Step 2: Create summary
    print("\nSTEP 2: Creating workbook summary...")
    summary = create_workbook_summary(sheets)
    print(f"✓ Summary created ({len(summary)} characters)\n")
    
    # Step 3: Get LLM plan
    print("STEP 3: Getting extraction plan from LLM...")
    plan = get_extraction_plan(summary, query)
    
    # Step 4: Execute plan algorithmically
    print("STEP 4: Executing plan algorithmically...")
    value = execute_plan(sheets, plan)
    
    print("\n" + "="*80)
    print("FINAL RESULT:")
    print(f"  Value: {value}")
    print(f"  Type: {type(value).__name__}")
    print("="*80 + "\n")
    
    return {
        "query": query,
        "plan": plan,
        "value": value,
        "type": type(value).__name__
    }

# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    # Example 1: Single cell lookup
    result1 = query_excel(
        "financial_report.xlsx",
        "What is the total revenue for Q4 2024?"
    )
    
    print(f"Answer: {result1['value']}\n")
    
    # Example 2: Sum of range
    result2 = query_excel(
        "financial_report.xlsx",
        "What is the sum of all monthly sales?"
    )
    
    print(f"Answer: {result2['value']}\n")
    
    # Example 3: Multiple sheets
    result3 = query_excel(
        "financial_report.xlsx",
        "What is the operating margin percentage?"
    )
    
    print(f"Answer: {result3['value']}\n")
