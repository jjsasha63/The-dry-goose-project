"""
UNIVERSAL EXCEL FLATTENER - Class-Based Architecture
Converts any Excel structure into flat key-value pairs
AI only searches, never computes
"""

import pandas as pd
import openpyxl
from openpyxl.utils import get_column_letter, column_index_from_string
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
import re
import json
import openai
from pathlib import Path


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class FlattenedCell:
    """A single flattened cell with full context path"""
    sheet: str
    path: str  # e.g., "Revenue-Q4-2024" or "Summary-Total-Amount"
    cell_ref: str  # e.g., "B5"
    value: Any
    value_type: str  # "number", "text", "date", etc.
    row: int
    col: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "sheet": self.sheet,
            "path": self.path,
            "cell_ref": self.cell_ref,
            "value": self.value,
            "value_type": self.value_type,
            "row": self.row,
            "col": self.col
        }


@dataclass
class QueryResult:
    """Result of a query operation"""
    query: str
    result: Any
    matches: List[Tuple[str, Any, str, str]]  # (path, value, sheet, cell_ref)
    operation: str
    confidence: float = 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "result": self.result,
            "matches": self.matches,
            "operation": self.operation,
            "confidence": self.confidence
        }


# ============================================================================
# STRUCTURE DETECTOR
# ============================================================================

class ExcelStructureDetector:
    """Detects headers and table structures in Excel sheets"""
    
    def __init__(self, max_header_rows: int = 5, max_row_header_cols: int = 3):
        self.max_header_rows = max_header_rows
        self.max_row_header_cols = max_row_header_cols
    
    def detect_header_rows(self, ws) -> List[int]:
        """
        Detect which rows are likely headers by checking:
        - Top N rows
        - Mostly text (not numbers)
        - Has bold formatting
        """
        header_rows = []
        
        for row_num in range(1, min(self.max_header_rows + 1, ws.max_row + 1)):
            row_cells = [ws.cell(row_num, col) for col in range(1, ws.max_column + 1)]
            
            # Count non-empty cells
            non_empty = [c for c in row_cells if c.value is not None]
            if len(non_empty) < 2:
                continue
            
            # Count text vs numbers
            text_count = sum(1 for c in non_empty if isinstance(c.value, str))
            
            # Count bold
            bold_count = sum(1 for c in non_empty if c.font and c.font.bold)
            
            # Header heuristic: mostly text or mostly bold
            if text_count / len(non_empty) > 0.7 or bold_count / len(non_empty) > 0.5:
                header_rows.append(row_num)
        
        return header_rows
    
    def detect_row_header_cols(self, ws, header_rows: List[int]) -> List[int]:
        """
        Detect which columns are row headers (left-side labels).
        Usually the leftmost 1-2 columns with text.
        """
        row_header_cols = []
        
        data_start_row = max(header_rows) + 1 if header_rows else 1
        
        for col_num in range(1, min(self.max_row_header_cols + 1, ws.max_column + 1)):
            col_cells = [ws.cell(row, col_num) 
                        for row in range(data_start_row, min(data_start_row + 20, ws.max_row + 1))]
            
            non_empty = [c for c in col_cells if c.value is not None]
            if len(non_empty) < 2:
                continue
            
            # Mostly text = probably labels
            text_count = sum(1 for c in non_empty if isinstance(c.value, str))
            if text_count / len(non_empty) > 0.6:
                row_header_cols.append(col_num)
        
        return row_header_cols
    
    def build_column_header_path(self, ws, col: int, header_rows: List[int]) -> str:
        """
        Build hierarchical path from multi-level column headers.
        E.g., if B1="Revenue" and B2="Q4", returns "Revenue-Q4"
        """
        parts = []
        for row in header_rows:
            cell = ws.cell(row, col)
            if cell.value is not None and str(cell.value).strip():
                parts.append(str(cell.value).strip())
        
        return "-".join(parts) if parts else f"Col{get_column_letter(col)}"
    
    def build_row_header_path(self, ws, row: int, row_header_cols: List[int]) -> str:
        """
        Build hierarchical path from multi-level row headers.
        E.g., if A5="2024" and B5="Q4", returns "2024-Q4"
        """
        parts = []
        for col in row_header_cols:
            cell = ws.cell(row, col)
            if cell.value is not None and str(cell.value).strip():
                parts.append(str(cell.value).strip())
        
        return "-".join(parts) if parts else f"Row{row}"


# ============================================================================
# SHEET FLATTENER
# ============================================================================

class SheetFlattener:
    """Flattens a single Excel sheet into path-value pairs"""
    
    def __init__(self, detector: ExcelStructureDetector):
        self.detector = detector
    
    def flatten(self, ws, ws_data, sheet_name: str, verbose: bool = True) -> List[FlattenedCell]:
        """
        Flatten a single sheet into path-value pairs.
        Handles multi-level headers automatically.
        """
        flattened = []
        
        # Detect structure
        header_rows = self.detector.detect_header_rows(ws)
        row_header_cols = self.detector.detect_row_header_cols(ws, header_rows)
        
        if verbose:
            print(f"  Sheet '{sheet_name}':")
            print(f"    Header rows: {header_rows}")
            print(f"    Row header cols: {[get_column_letter(c) for c in row_header_cols]}")
        
        # Determine data area
        data_start_row = max(header_rows) + 1 if header_rows else 1
        data_start_col = max(row_header_cols) + 1 if row_header_cols else 1
        
        # Flatten all data cells
        for row in range(data_start_row, ws.max_row + 1):
            # Build row context
            row_path = self.detector.build_row_header_path(ws, row, row_header_cols)
            
            for col in range(data_start_col, ws.max_column + 1):
                cell = ws.cell(row, col)
                cell_data = ws_data.cell(row, col)
                
                # Get actual value (formula result)
                value = cell_data.value if cell_data.value is not None else cell.value
                
                if value is None:
                    continue
                
                # Build column context
                col_path = self.detector.build_column_header_path(ws, col, header_rows)
                
                # Combine into full path: Sheet-RowHeaders-ColHeaders
                full_path = f"{sheet_name}-{row_path}-{col_path}"
                
                # Determine value type
                if isinstance(value, (int, float)):
                    value_type = "number"
                elif isinstance(value, str):
                    value_type = "text"
                else:
                    value_type = "other"
                
                cell_ref = f"{get_column_letter(col)}{row}"
                
                flattened.append(FlattenedCell(
                    sheet=sheet_name,
                    path=full_path,
                    cell_ref=cell_ref,
                    value=value,
                    value_type=value_type,
                    row=row,
                    col=col
                ))
        
        return flattened


# ============================================================================
# WORKBOOK FLATTENER
# ============================================================================

class WorkbookFlattener:
    """Flattens entire Excel workbook into searchable format"""
    
    def __init__(self, detector: Optional[ExcelStructureDetector] = None):
        self.detector = detector or ExcelStructureDetector()
        self.sheet_flattener = SheetFlattener(self.detector)
        self.flattened_ List[FlattenedCell] = []
        self.file_path: Optional[str] = None
    
    def flatten(self, file_path: str, verbose: bool = True) -> List[FlattenedCell]:
        """
        Flatten entire workbook into searchable key-value pairs.
        """
        self.file_path = file_path
        
        if verbose:
            print(f"Flattening workbook: {file_path}")
            print("="*80)
        
        wb = openpyxl.load_workbook(file_path, data_only=False)
        wb_data = openpyxl.load_workbook(file_path, data_only=True)
        
        all_flattened = []
        
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            ws_data = wb_data[sheet_name]
            
            sheet_flattened = self.sheet_flattener.flatten(ws, ws_data, sheet_name, verbose)
            all_flattened.extend(sheet_flattened)
            
            if verbose:
                print(f"    → Extracted {len(sheet_flattened)} values")
        
        wb.close()
        wb_data.close()
        
        self.flattened_data = all_flattened
        
        if verbose:
            print(f"\n✓ Total flattened entries: {len(all_flattened)}")
            print("="*80 + "\n")
        
        return all_flattened
    
    def export_to_csv(self, output_path: str):
        """Export flattened data to CSV for inspection/debugging"""
        import csv
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Sheet", "Path", "Cell", "Value", "Type", "Row", "Col"])
            
            for entry in self.flattened_
                writer.writerow([
                    entry.sheet,
                    entry.path,
                    entry.cell_ref,
                    entry.value,
                    entry.value_type,
                    entry.row,
                    entry.col
                ])
        
        print(f"✓ Exported flattened data to: {output_path}")
    
    def export_to_json(self, output_path: str):
        """Export flattened data to JSON"""
        data = [entry.to_dict() for entry in self.flattened_data]
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str)
        
        print(f"✓ Exported flattened data to: {output_path}")


# ============================================================================
# AI SEMANTIC SEARCHER
# ============================================================================

class AISemanticSearcher:
    """Uses AI to semantically search flattened paths (NO computation)"""
    
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.api_key = api_key
        self.model = model
        openai.api_key = api_key
    
    def search(self, query: str, flattened: List[FlattenedCell], 
               top_k: int = 5, sample_size: int = 500) -> List[Tuple[FlattenedCell, str]]:
        """
        Use AI to semantically search the flattened paths.
        AI only picks which paths match - never computes values.
        """
        
        # Create compact representation of paths (sample for token efficiency)
        path_list = []
        sample = flattened[:sample_size] if len(flattened) > sample_size else flattened
        
        for i, entry in enumerate(sample):
            path_list.append(f"{i}: {entry.path} = {entry.value}")
        
        paths_text = "\n".join(path_list)
        
        prompt = f"""You have a flattened Excel database with these entries:

{paths_text}

USER QUERY: {query}

Return ONLY a JSON array of the indices (numbers) that best match the query.
Return top {top_k} matches.

Example: [45, 123, 67]

If the query asks for a calculation (sum, average, etc.), return ALL relevant indices.

JSON response:"""

        response = openai.ChatCompletion.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=200
        )
        
        result_text = response.choices[0].message.content.strip()
        
        # Parse JSON
        try:
            indices = json.loads(result_text)
        except:
            # Fallback: extract numbers
            indices = [int(x) for x in re.findall(r'\d+', result_text)]
        
        matches = []
        for idx in indices[:top_k]:
            if 0 <= idx < len(sample):
                matches.append((sample[idx], f"Match #{len(matches)+1}"))
        
        return matches


# ============================================================================
# VALUE EXTRACTOR
# ============================================================================

class ValueExtractor:
    """Extracts values algorithmically from matches (GUARANTEED PRECISE)"""
    
    @staticmethod
    def extract(matches: List[Tuple[FlattenedCell, str]], operation: str = "return") -> Any:
        """
        Extract value(s) algorithmically - NO AI involved.
        
        Operations:
        - return: return first match
        - sum: sum all numeric matches
        - average: average all numeric matches
        - max: maximum value
        - min: minimum value
        - count: count matches
        - list: return all matches
        """
        
        if not matches:
            return None
        
        if operation == "return":
            return matches[0][0].value
        
        elif operation == "sum":
            values = [m[0].value for m in matches if m[0].value_type == "number"]
            return sum(values) if values else None
        
        elif operation == "average":
            values = [m[0].value for m in matches if m[0].value_type == "number"]
            return sum(values) / len(values) if values else None
        
        elif operation == "max":
            values = [m[0].value for m in matches if m[0].value_type == "number"]
            return max(values) if values else None
        
        elif operation == "min":
            values = [m[0].value for m in matches if m[0].value_type == "number"]
            return min(values) if values else None
        
        elif operation == "count":
            return len(matches)
        
        elif operation == "list":
            return [m[0].value for m in matches]
        
        else:
            raise ValueError(f"Unknown operation: {operation}")


# ============================================================================
# MAIN QUERY ENGINE
# ============================================================================

class ExcelQueryEngine:
    """
    Main query engine combining flattening, AI search, and algorithmic extraction.
    """
    
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.flattener = WorkbookFlattener()
        self.searcher = AISemanticSearcher(api_key, model)
        self.extractor = ValueExtractor()
        self.flattened_ Optional[List[FlattenedCell]] = None
        self.file_path: Optional[str] = None
    
    def load_workbook(self, file_path: str, verbose: bool = True) -> 'ExcelQueryEngine':
        """Load and flatten workbook"""
        self.file_path = file_path
        self.flattened_data = self.flattener.flatten(file_path, verbose)
        return self
    
    def query(self, query: str, operation: str = "return", 
              top_k: int = 10, verbose: bool = True) -> QueryResult:
        """
        Query Excel using flattened structure + AI search + algorithmic extraction.
        
        Args:
            query: Natural language query
            operation: "return" | "sum" | "average" | "max" | "min" | "count" | "list"
            top_k: Number of matches to retrieve
            verbose: Print progress
        """
        
        if self.flattened_data is None:
            raise ValueError("No workbook loaded. Call load_workbook() first.")
        
        if verbose:
            print("\n" + "="*80)
            print(f"QUERY: {query}")
            print(f"OPERATION: {operation}")
            print("="*80 + "\n")
        
        # AI semantic search
        if verbose:
            print("Searching with AI...")
        
        matches = self.searcher.search(query, self.flattened_data, top_k)
        
        if verbose:
            print(f"\n✓ Found {len(matches)} matches:")
            for entry, label in matches[:5]:
                print(f"  {label}: {entry.path} = {entry.value} [{entry.sheet}!{entry.cell_ref}]")
        
        # Algorithmic extraction
        if verbose:
            print(f"\nExtracting value (operation: {operation})...")
        
        result = self.extractor.extract(matches, operation)
        
        if verbose:
            print(f"\n{'='*80}")
            print(f"RESULT: {result}")
            print(f"{'='*80}\n")
        
        return QueryResult(
            query=query,
            result=result,
            matches=[(m.path, m.value, m.sheet, m.cell_ref) for m, _ in matches],
            operation=operation
        )
    
    def export_flattened(self, output_path: str, format: str = "csv"):
        """Export flattened data for inspection"""
        if format == "csv":
            self.flattener.export_to_csv(output_path)
        elif format == "json":
            self.flattener.export_to_json(output_path)
        else:
            raise ValueError(f"Unknown format: {format}")
    
    def get_sample_paths(self, n: int = 20) -> List[str]:
        """Get sample paths for debugging"""
        if self.flattened_data is None:
            return []
        return [entry.path for entry in self.flattened_data[:n]]


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    # Initialize query engine
    engine = ExcelQueryEngine(api_key="your-openai-api-key")
    
    # Load workbook
    engine.load_workbook("financial_report.xlsx")
    
    # Example 1: Single value lookup
    result1 = engine.query(
        "What is the Q4 2024 revenue?",
        operation="return"
    )
    print(f"Answer: {result1.result}\n")
    
    # Example 2: Sum multiple values
    result2 = engine.query(
        "What is the total of all quarterly revenues?",
        operation="sum"
    )
    print(f"Answer: {result2.result}\n")
    
    # Example 3: Average
    result3 = engine.query(
        "What is the average profit margin?",
        operation="average"
    )
    print(f"Answer: {result3.result}\n")
    
    # Export flattened structure for debugging
    engine.export_flattened("flattened_excel.csv", format="csv")
    
    # Show sample paths
    print("\nSample of flattened paths:")
    for path in engine.get_sample_paths(10):
        print(f"  {path}")
    
    # Export query results
    with open("query_results.json", "w") as f:
        json.dump([
            result1.to_dict(),
            result2.to_dict(),
            result3.to_dict()
        ], f, indent=2, default=str)
    
    print("\n✓ Query results exported to query_results.json")
