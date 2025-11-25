"""
STRUCTURE-AWARE EXCEL EXTRACTOR
Detects multi-dimensional tables, merged cells, headers, and spatial layouts
"""

import pandas as pd
import openpyxl
from openpyxl.utils import get_column_letter, column_index_from_string
import openai
import json
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict

# ============================================================================
# CONFIGURATION
# ============================================================================

openai.api_key = "your-key-here"

# ============================================================================
# STEP 1: DETECT TABLE STRUCTURES IN EXCEL
# ============================================================================

@dataclass
class CellInfo:
    """Complete information about a single cell"""
    row: int
    col: int
    value: Any
    is_bold: bool
    is_merged: bool
    merged_range: Optional[str]
    has_border: bool
    has_fill: bool
    font_size: float
    alignment: str

@dataclass
class TableRegion:
    """Detected table region with headers and data"""
    sheet_name: str
    start_row: int
    end_row: int
    start_col: int
    end_col: int
    header_rows: List[int]  # Which rows are headers
    header_cols: List[int]  # Which columns are row headers
    data_area: Tuple[int, int, int, int]  # (start_row, end_row, start_col, end_col)
    headers_map: Dict[str, str]  # Maps cell ref -> header text
    orientation: str  # "horizontal" or "vertical" or "matrix"

def load_excel_with_structure(file_path: str) -> Dict[str, Any]:
    """
    Load Excel with COMPLETE structural information.
    Detects merged cells, table boundaries, headers, etc.
    """
    wb = openpyxl.load_workbook(file_path, data_only=False)
    wb_data = openpyxl.load_workbook(file_path, data_only=True)
    
    workbook_structure = {
        "file_path": file_path,
        "sheets": {}
    }
    
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        ws_data = wb_data[sheet_name]
        
        # Extract all cell information
        cells = {}
        merged_ranges = {}
        
        # Get merged cell ranges
        for merged_range in ws.merged_cells.ranges:
            min_col, min_row, max_col, max_row = merged_range.bounds
            merged_str = str(merged_range)
            for row in range(min_row, max_row + 1):
                for col in range(min_col, max_col + 1):
                    merged_ranges[(row, col)] = merged_str
        
        # Scan all cells
        for row in range(1, ws.max_row + 1):
            for col in range(1, ws.max_column + 1):
                cell = ws.cell(row, col)
                cell_data = ws_data.cell(row, col)
                
                # Get actual value (formula result)
                value = cell_data.value if cell_data.value is not None else cell.value
                
                if value is None or (isinstance(value, str) and not value.strip()):
                    continue
                
                cell_ref = f"{get_column_letter(col)}{row}"
                
                cells[cell_ref] = CellInfo(
                    row=row,
                    col=col,
                    value=value,
                    is_bold=cell.font.bold if cell.font else False,
                    is_merged=(row, col) in merged_ranges,
                    merged_range=merged_ranges.get((row, col)),
                    has_border=cell.border is not None and any([
                        cell.border.top.style,
                        cell.border.bottom.style,
                        cell.border.left.style,
                        cell.border.right.style
                    ]) if cell.border else False,
                    has_fill=cell.fill.start_color.index != "00000000" if cell.fill else False,
                    font_size=cell.font.size if cell.font and cell.font.size else 11.0,
                    alignment=cell.alignment.horizontal if cell.alignment else "general"
                )
        
        # Detect table regions
        tables = detect_table_regions(cells, ws.max_row, ws.max_column)
        
        workbook_structure["sheets"][sheet_name] = {
            "dimensions": {"rows": ws.max_row, "cols": ws.max_column},
            "cells": cells,
            "tables": tables
        }
    
    wb.close()
    wb_data.close()
    
    return workbook_structure

def detect_table_regions(cells: Dict[str, CellInfo], max_row: int, max_col: int) -> List[TableRegion]:
    """
    Intelligently detect table structures including:
    - Multi-row headers
    - Multi-column row headers
    - Matrix-style tables
    - Nested tables
    """
    tables = []
    
    # Build density map (which areas have data)
    density = defaultdict(int)
    for cell_info in cells.values():
        density[cell_info.row] += 1
    
    # Find contiguous data regions
    regions = []
    in_region = False
    start_row = None
    
    for row in range(1, max_row + 1):
        if density[row] > 2:  # At least 3 cells in row
            if not in_region:
                start_row = row
                in_region = True
        else:
            if in_region and start_row:
                regions.append((start_row, row - 1))
                in_region = False
    
    if in_region and start_row:
        regions.append((start_row, max_row))
    
    # Analyze each region
    for start_row, end_row in regions:
        # Find column boundaries
        cols_used = set()
        for cell_info in cells.values():
            if start_row <= cell_info.row <= end_row:
                cols_used.add(cell_info.col)
        
        if not cols_used:
            continue
        
        min_col = min(cols_used)
        max_col = max(cols_used)
        
        # Detect header rows (bold, larger font, top of region)
        header_rows = []
        for row in range(start_row, min(start_row + 3, end_row + 1)):
            row_cells = [c for c in cells.values() if c.row == row]
            if not row_cells:
                continue
            
            # Header heuristics
            bold_ratio = sum(1 for c in row_cells if c.is_bold) / len(row_cells)
            is_top_rows = row <= start_row + 2
            
            if bold_ratio > 0.5 or (is_top_rows and all(isinstance(c.value, str) for c in row_cells)):
                header_rows.append(row)
        
        # Detect header columns (leftmost, bold)
        header_cols = []
        for col in range(min_col, min(min_col + 3, max_col + 1)):
            col_cells = [c for c in cells.values() if c.col == col and start_row <= c.row <= end_row]
            if not col_cells:
                continue
            
            bold_ratio = sum(1 for c in col_cells if c.is_bold) / len(col_cells)
            if bold_ratio > 0.3:
                header_cols.append(col)
        
        # Data area (excluding headers)
        data_start_row = max(header_rows) + 1 if header_rows else start_row
        data_start_col = max(header_cols) + 1 if header_cols else min_col
        
        # Build headers map
        headers_map = {}
        for cell_ref, cell_info in cells.items():
            if cell_info.row in header_rows or cell_info.col in header_cols:
                headers_map[cell_ref] = str(cell_info.value)
        
        # Determine orientation
        if header_rows and header_cols:
            orientation = "matrix"
        elif header_rows:
            orientation = "horizontal"
        elif header_cols:
            orientation = "vertical"
        else:
            orientation = "horizontal"  # default
        
        tables.append(TableRegion(
            sheet_name="",  # Will be filled in
            start_row=start_row,
            end_row=end_row,
            start_col=min_col,
            end_col=max_col,
            header_rows=header_rows,
            header_cols=header_cols,
            data_area=(data_start_row, end_row, data_start_col, max_col),
            headers_map=headers_map,
            orientation=orientation
        ))
    
    return tables

# ============================================================================
# STEP 2: CREATE RICH STRUCTURAL REPRESENTATION FOR LLM
# ============================================================================

def create_rich_representation(structure: Dict[str, Any]) -> str:
    """
    Create a RICH representation showing table structures, headers, orientation.
    """
    lines = [f"EXCEL FILE: {structure['file_path']}"]
    lines.append("="*80)
    lines.append("")
    
    for sheet_name, sheet_info in structure["sheets"].items():
        lines.append(f"### SHEET: {sheet_name} ###")
        lines.append(f"Dimensions: {sheet_info['dimensions']['rows']} rows × {sheet_info['dimensions']['cols']} cols")
        lines.append("")
        
        if not sheet_info["tables"]:
            lines.append("No structured tables detected - showing raw ")
            # Show first 20 cells
            for i, (cell_ref, cell_info) in enumerate(list(sheet_info["cells"].items())[:20]):
                lines.append(f"  {cell_ref} = {cell_info.value}")
            lines.append("")
            continue
        
        for table_idx, table in enumerate(sheet_info["tables"], 1):
            lines.append(f"TABLE #{table_idx} (Orientation: {table.orientation.upper()})")
            lines.append(f"  Location: Rows {table.start_row}-{table.end_row}, Cols {get_column_letter(table.start_col)}-{get_column_letter(table.end_col)}")
            
            # Show headers
            if table.header_rows:
                lines.append(f"  Header Rows: {table.header_rows}")
                for row in table.header_rows:
                    row_headers = []
                    for cell_ref, text in table.headers_map.items():
                        cell = sheet_info["cells"][cell_ref]
                        if cell.row == row:
                            row_headers.append(f"{cell_ref}='{text}'")
                    if row_headers:
                        lines.append(f"    Row {row}: {' | '.join(row_headers)}")
            
            if table.header_cols:
                lines.append(f"  Header Columns: {[get_column_letter(c) for c in table.header_cols]}")
            
            # Show data area sample
            lines.append(f"  Data Area: Rows {table.data_area[0]}-{table.data_area[1]}, Cols {get_column_letter(table.data_area[2])}-{get_column_letter(table.data_area[3])}")
            lines.append(f"  Sample ")
            
            # Show first few data rows
            data_rows = defaultdict(dict)
            for cell_ref, cell_info in sheet_info["cells"].items():
                if (table.data_area[0] <= cell_info.row <= table.data_area[1] and
                    table.data_area[2] <= cell_info.col <= table.data_area[3]):
                    data_rows[cell_info.row][cell_info.col] = cell_info.value
            
            for row_num in sorted(list(data_rows.keys())[:10]):
                row_data = data_rows[row_num]
                row_str = []
                for col in sorted(row_data.keys())[:8]:
                    val = str(row_data[col])[:30]
                    row_str.append(f"{get_column_letter(col)}{row_num}={val}")
                lines.append(f"    {' | '.join(row_str)}")
            
            lines.append("")
    
    return "\n".join(lines)

# ============================================================================
# STEP 3: LLM PLANNING WITH STRUCTURE AWARENESS
# ============================================================================

STRUCTURE_AWARE_PROMPT = """You are an Excel extraction planner with STRUCTURE AWARENESS.

You receive detailed information about:
- Table locations and boundaries
- Multi-row/multi-column headers
- Table orientation (horizontal/vertical/matrix)
- Merged cells and formatting

Your job: Return a precise JSON extraction plan.

JSON SCHEMA:
{
  "sheet_name": "sheet name",
  "table_number": 1,
  "cell_reference": "B5" or "B2:B10",
  "operation": "return_cell" | "sum" | "average" | "filter_sum" | "lookup",
  "context": {
    "row_header": "Q4 2024",  // if applicable
    "col_header": "Revenue",  // if applicable
    "table_type": "horizontal" | "vertical" | "matrix"
  },
  "reasoning": "detailed explanation"
}

IMPORTANT:
- Pay attention to table orientation
- Use header information to locate correct cells
- For matrix tables, consider both row and column headers
- Be precise with cell references

Example for MATRIX table:
If headers are:
  Row headers (A): Q1, Q2, Q3, Q4
  Column headers (Row 1): Revenue, Profit, Expenses
Query: "What is Q4 Revenue?"
Answer: Find intersection of Q4 row and Revenue column
"""

def get_structure_aware_plan(structure: Dict[str, Any], query: str) -> Dict[str, Any]:
    """Get extraction plan with full structure awareness"""
    
    representation = create_rich_representation(structure)
    
    messages = [
        {"role": "system", "content": STRUCTURE_AWARE_PROMPT},
        {"role": "user", "content": f"{representation}\n\nQUERY: {query}\n\nReturn extraction plan in JSON:"}
    ]
    
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=messages,
        temperature=0.0,
        response_format={"type": "json_object"}
    )
    
    plan = json.loads(response.choices[0].message.content)
    
    print("\n" + "="*80)
    print("STRUCTURE-AWARE PLAN:")
    print(json.dumps(plan, indent=2))
    print("="*80 + "\n")
    
    return plan

# ============================================================================
# STEP 4: STRUCTURE-AWARE EXECUTION
# ============================================================================

def execute_structure_aware_plan(structure: Dict[str, Any], plan: Dict[str, Any]) -> Any:
    """Execute plan with structure awareness"""
    
    sheet_name = plan["sheet_name"]
    sheet_info = structure["sheets"][sheet_name]
    cells = sheet_info["cells"]
    
    # Get table
    table_num = plan.get("table_number", 1)
    table = sheet_info["tables"][table_num - 1] if sheet_info["tables"] else None
    
    context = plan.get("context", {})
    operation = plan["operation"]
    
    print(f"Executing on: {sheet_name}, Table #{table_num}")
    print(f"Table type: {context.get('table_type', 'unknown')}")
    print(f"Operation: {operation}")
    
    # If we have row/col headers in context, find intersection
    if context.get("row_header") and context.get("col_header") and table:
        row_header_text = context["row_header"]
        col_header_text = context["col_header"]
        
        print(f"Looking for intersection: row='{row_header_text}', col='{col_header_text}'")
        
        # Find row matching row_header
        target_row = None
        for cell_ref, cell_info in cells.items():
            if str(cell_info.value).strip().lower() == row_header_text.strip().lower():
                if cell_info.col in table.header_cols:
                    target_row = cell_info.row
                    print(f"  Found row header at {cell_ref}, row={target_row}")
                    break
        
        # Find column matching col_header
        target_col = None
        for cell_ref, cell_info in cells.items():
            if str(cell_info.value).strip().lower() == col_header_text.strip().lower():
                if cell_info.row in table.header_rows:
                    target_col = cell_info.col
                    print(f"  Found col header at {cell_ref}, col={get_column_letter(target_col)}")
                    break
        
        if target_row and target_col:
            # Find cell at intersection
            intersection_ref = f"{get_column_letter(target_col)}{target_row}"
            if intersection_ref in cells:
                value = cells[intersection_ref].value
                print(f"✓ Found value at intersection {intersection_ref}: {value}")
                return value
    
    # Fallback: use cell_reference directly
    cell_ref = plan.get("cell_reference", "")
    
    if ":" not in cell_ref:
        # Single cell
        if cell_ref in cells:
            value = cells[cell_ref].value
            print(f"✓ Retrieved {cell_ref}: {value}")
            return value
    else:
        # Range operation
        # Convert to pandas for easier range operations
        df = pd.DataFrame.from_dict(
            {cell_ref: [cell.value] for cell_ref, cell in cells.items()},
            orient='index'
        )
        
        # Parse range and extract values
        start, end = cell_ref.split(":")
        start_row, start_col = parse_cell_ref(start)
        end_row, end_col = parse_cell_ref(end)
        
        values = []
        for row in range(start_row, end_row + 1):
            for col in range(start_col, end_col + 1):
                ref = f"{get_column_letter(col)}{row}"
                if ref in cells:
                    val = cells[ref].value
                    if isinstance(val, (int, float)):
                        values.append(val)
        
        if operation == "sum":
            result = sum(values)
        elif operation == "average":
            result = sum(values) / len(values) if values else 0
        else:
            result = values[0] if values else None
        
        print(f"✓ {operation.upper()} over {len(values)} values: {result}")
        return result
    
    return None

def parse_cell_ref(ref: str) -> Tuple[int, int]:
    """Parse cell reference like 'B5' into (row, col)"""
    import re
    match = re.match(r'([A-Z]+)(\d+)', ref)
    col_letter = match.group(1)
    row_num = int(match.group(2))
    col_num = column_index_from_string(col_letter)
    return row_num, col_num

# ============================================================================
# MAIN INTERFACE
# ============================================================================

def query_excel_with_structure(file_path: str, query: str) -> Dict[str, Any]:
    """Main function with full structure awareness"""
    
    print("\n" + "="*80)
    print(f"QUERY: {query}")
    print("="*80)
    
    # Load with structure detection
    print("\n1. Loading Excel and detecting structures...")
    structure = load_excel_with_structure(file_path)
    
    total_tables = sum(len(s["tables"]) for s in structure["sheets"].values())
    print(f"✓ Found {total_tables} table regions across {len(structure['sheets'])} sheets")
    
    # Get structure-aware plan
    print("\n2. Getting structure-aware extraction plan...")
    plan = get_structure_aware_plan(structure, query)
    
    # Execute
    print("\n3. Executing plan...")
    value = execute_structure_aware_plan(structure, plan)
    
    print("\n" + "="*80)
    print(f"RESULT: {value}")
    print("="*80 + "\n")
    
    return {
        "query": query,
        "value": value,
        "plan": plan
    }

# ============================================================================
# USAGE
# ============================================================================

if __name__ == "__main__":
    result = query_excel_with_structure(
        "financial_report.xlsx",
        "What is the Q4 2024 revenue?"
    )
    
    print(f"Final Answer: {result['value']}")
