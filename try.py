"""
IMPROVED UNIVERSAL EXCEL FLATTENER - With Data Type Validation
Properly separates headers from data values using type analysis
"""

import pandas as pd
import openpyxl
from openpyxl.utils import get_column_letter, column_index_from_string
from typing import Dict, List, Tuple, Any, Optional, Set
from dataclasses import dataclass
import re
import json
import openai
from collections import defaultdict, Counter


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class FlattenedCell:
    """A single flattened cell with full context path"""
    sheet: str
    path: str
    cell_ref: str
    value: Any
    value_type: str
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
    matches: List[Tuple[str, Any, str, str]]
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
# CELL TYPE ANALYZER
# ============================================================================

class CellTypeAnalyzer:
    """Analyzes cell types to distinguish headers from data"""
    
    @staticmethod
    def is_numeric_value(value: Any) -> bool:
        """Check if value is numeric or numeric string"""
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return True
        
        if isinstance(value, str):
            # Try to parse as number
            try:
                float(value.replace(',', '').replace('$', '').replace('%', '').strip())
                return True
            except:
                return False
        
        return False
    
    @staticmethod
    def is_likely_header(value: Any) -> bool:
        """
        Determine if a value is likely a header (label) rather than data.
        Headers are typically:
        - Text strings (not numeric)
        - Short descriptive labels
        - Not dates
        """
        if value is None:
            return False
        
        # Numeric values are NOT headers
        if CellTypeAnalyzer.is_numeric_value(value):
            return False
        
        # Booleans are data, not headers
        if isinstance(value, bool):
            return False
        
        # Pure text strings are likely headers
        if isinstance(value, str):
            # Skip if it looks like a numeric string
            if CellTypeAnalyzer.is_numeric_value(value):
                return False
            
            # Very long text is likely data, not header
            if len(value) > 100:
                return False
            
            return True
        
        # Dates could be headers in some contexts, but usually data
        return False
    
    @staticmethod
    def classify_value_type(value: Any) -> str:
        """Classify value into detailed type"""
        if value is None:
            return "empty"
        
        if isinstance(value, bool):
            return "boolean"
        
        if isinstance(value, (int, float)):
            return "number"
        
        if isinstance(value, str):
            # Try to identify special formats
            cleaned = value.replace(',', '').replace(' ', '').strip()
            
            if '$' in value or '€' in value or '£' in value:
                return "currency"
            
            if '%' in value:
                return "percentage"
            
            # Check if it's a numeric string
            if CellTypeAnalyzer.is_numeric_value(value):
                return "numeric_string"
            
            # Check for date patterns
            if re.match(r'\d{1,4}[-/]\d{1,2}[-/]\d{1,4}', value):
                return "date_string"
            
            return "text"
        
        # Handle datetime objects
        if hasattr(value, 'year') and hasattr(value, 'month'):
            return "date"
        
        return "other"


# ============================================================================
# IMPROVED STRUCTURE DETECTOR
# ============================================================================

class ImprovedStructureDetector:
    """Detects headers and table structures using data type analysis"""
    
    def __init__(self, max_header_rows: int = 10, max_row_header_cols: int = 5):
        self.max_header_rows = max_header_rows
        self.max_row_header_cols = max_row_header_cols
        self.analyzer = CellTypeAnalyzer()
    
    def analyze_row_types(self, ws, row_num: int) -> Dict[str, Any]:
        """Analyze the types of values in a row"""
        values = []
        for col in range(1, ws.max_column + 1):
            cell = ws.cell(row_num, col)
            if cell.value is not None:
                values.append(cell.value)
        
        if not values:
            return {"empty": True}
        
        type_counts = Counter([self.analyzer.classify_value_type(v) for v in values])
        total = len(values)
        
        numeric_count = sum(type_counts.get(t, 0) for t in ["number", "currency", "percentage", "numeric_string"])
        text_count = type_counts.get("text", 0)
        
        return {
            "empty": False,
            "total_cells": total,
            "numeric_count": numeric_count,
            "text_count": text_count,
            "numeric_ratio": numeric_count / total if total > 0 else 0,
            "text_ratio": text_count / total if total > 0 else 0,
            "type_distribution": dict(type_counts)
        }
    
    def detect_header_rows(self, ws) -> List[int]:
        """
        Detect header rows using type analysis.
        Header rows have high text ratio and low numeric ratio.
        """
        header_rows = []
        consecutive_data_rows = 0
        
        for row_num in range(1, min(self.max_header_rows + 1, ws.max_row + 1)):
            row_info = self.analyze_row_types(ws, row_num)
            
            if row_info["empty"]:
                continue
            
            # Get formatting hints
            first_cell = ws.cell(row_num, 1)
            is_bold = first_cell.font and first_cell.font.bold
            
            # Header heuristics:
            # 1. High text ratio (>70%)
            # 2. Low numeric ratio (<30%)
            # 3. OR bold formatting with some text
            is_header = (
                row_info["text_ratio"] > 0.7 or
                (row_info["text_ratio"] > 0.4 and row_info["numeric_ratio"] < 0.3) or
                (is_bold and row_info["text_count"] > 0)
            )
            
            if is_header:
                header_rows.append(row_num)
                consecutive_data_rows = 0
            else:
                # If we see data rows, stop looking for headers
                consecutive_data_rows += 1
                if consecutive_data_rows >= 3:
                    break
        
        return header_rows
    
    def analyze_column_types(self, ws, col_num: int, start_row: int = 1) -> Dict[str, Any]:
        """Analyze the types of values in a column"""
        values = []
        for row in range(start_row, min(start_row + 30, ws.max_row + 1)):
            cell = ws.cell(row, col_num)
            if cell.value is not None:
                values.append(cell.value)
        
        if not values:
            return {"empty": True}
        
        type_counts = Counter([self.analyzer.classify_value_type(v) for v in values])
        total = len(values)
        
        numeric_count = sum(type_counts.get(t, 0) for t in ["number", "currency", "percentage", "numeric_string"])
        text_count = type_counts.get("text", 0)
        
        return {
            "empty": False,
            "total_cells": total,
            "numeric_count": numeric_count,
            "text_count": text_count,
            "numeric_ratio": numeric_count / total if total > 0 else 0,
            "text_ratio": text_count / total if total > 0 else 0,
            "type_distribution": dict(type_counts)
        }
    
    def detect_row_header_cols(self, ws, header_rows: List[int]) -> List[int]:
        """
        Detect row header columns using type analysis.
        Row headers are typically text-heavy leftmost columns.
        """
        row_header_cols = []
        data_start_row = max(header_rows) + 1 if header_rows else 1
        
        for col_num in range(1, min(self.max_row_header_cols + 1, ws.max_column + 1)):
            col_info = self.analyze_column_types(ws, col_num, data_start_row)
            
            if col_info["empty"]:
                continue
            
            # Row headers: high text ratio, low numeric ratio
            if col_info["text_ratio"] > 0.6 and col_info["numeric_ratio"] < 0.4:
                row_header_cols.append(col_num)
            else:
                # Stop at first data column
                break
        
        return row_header_cols
    
    def build_column_header_path(self, ws, col: int, header_rows: List[int]) -> str:
        """Build hierarchical path from multi-level column headers"""
        parts = []
        for row in header_rows:
            cell = ws.cell(row, col)
            value = cell.value
            
            # Only include if it's actually a header (text)
            if value is not None and self.analyzer.is_likely_header(value):
                parts.append(str(value).strip())
        
        return "-".join(parts) if parts else f"Col{get_column_letter(col)}"
    
    def build_row_header_path(self, ws, row: int, row_header_cols: List[int]) -> str:
        """Build hierarchical path from multi-level row headers"""
        parts = []
        for col in row_header_cols:
            cell = ws.cell(row, col)
            value = cell.value
            
            # Only include if it's actually a header (text)
            if value is not None and self.analyzer.is_likely_header(value):
                parts.append(str(value).strip())
        
        return "-".join(parts) if parts else f"Row{row}"


# ============================================================================
# IMPROVED SHEET FLATTENER
# ============================================================================

class ImprovedSheetFlattener:
    """Flattens a single Excel sheet with proper header/data separation"""
    
    def __init__(self, detector: ImprovedStructureDetector):
        self.detector = detector
        self.analyzer = CellTypeAnalyzer()
    
    def flatten(self, ws, ws_data, sheet_name: str, verbose: bool = True) -> List[FlattenedCell]:
        """
        Flatten a single sheet into path-value pairs.
        ONLY includes actual data cells, excludes header cells.
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
        
        if verbose:
            print(f"    Data area starts at: Row {data_start_row}, Col {get_column_letter(data_start_col)}")
        
        # Build header context maps
        col_headers = {}
        for col in range(data_start_col, ws.max_column + 1):
            col_headers[col] = self.detector.build_column_header_path(ws, col, header_rows)
        
        # Flatten all data cells (excluding headers)
        data_cell_count = 0
        skipped_header_cells = 0
        
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
                
                # CRITICAL: Skip if this looks like a header cell (text in data area)
                # This prevents including section headers within the data
                value_type = self.analyzer.classify_value_type(value)
                
                # If cell is pure text and surrounded by numeric cells, it might be a sub-header
                # Check surrounding cells
                if value_type == "text" and not self.analyzer.is_numeric_value(value):
                    # Check if this row is mostly text (might be a section header)
                    row_info = self.detector.analyze_row_types(ws, row)
                    if row_info.get("text_ratio", 0) > 0.8:
                        # This is likely a section header within the data
                        skipped_header_cells += 1
                        if verbose and skipped_header_cells <= 5:
                            print(f"      Skipping likely header cell: {get_column_letter(col)}{row} = '{value}'")
                        continue
                
                # Build column context
                col_path = col_headers.get(col, f"Col{get_column_letter(col)}")
                
                # Combine into full path: Sheet-RowHeaders-ColHeaders
                full_path = f"{sheet_name}-{row_path}-{col_path}"
                
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
                data_cell_count += 1
        
        if verbose:
            print(f"    → Extracted {data_cell_count} data values")
            if skipped_header_cells > 0:
                print(f"    → Skipped {skipped_header_cells} header/label cells")
        
        return flattened


# ============================================================================
# IMPROVED WORKBOOK FLATTENER
# ============================================================================

class ImprovedWorkbookFlattener:
    """Flattens entire Excel workbook with proper type validation"""
    
    def __init__(self, detector: Optional[ImprovedStructureDetector] = None):
        self.detector = detector or ImprovedStructureDetector()
        self.sheet_flattener = ImprovedSheetFlattener(self.detector)
        self.flattened_ List[FlattenedCell] = []
        self.file_path: Optional[str] = None
    
    def flatten(self, file_path: str, verbose: bool = True) -> List[FlattenedCell]:
        """Flatten entire workbook with type-based header filtering"""
        self.file_path = file_path
        
        if verbose:
            print(f"\nFlattening workbook: {file_path}")
            print("="*80)
        
        wb = openpyxl.load_workbook(file_path, data_only=False)
        wb_data = openpyxl.load_workbook(file_path, data_only=True)
        
        all_flattened = []
        
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            ws_data = wb_data[sheet_name]
            
            sheet_flattened = self.sheet_flattener.flatten(ws, ws_data, sheet_name, verbose)
            all_flattened.extend(sheet_flattened)
        
        wb.close()
        wb_data.close()
        
        self.flattened_data = all_flattened
        
        if verbose:
            print(f"\n✓ Total flattened DATA entries: {len(all_flattened)}")
            
            # Show type distribution
            type_counts = Counter([entry.value_type for entry in all_flattened])
            print(f"\nValue type distribution:")
            for vtype, count in type_counts.most_common():
                print(f"  {vtype}: {count}")
            
            print("="*80 + "\n")
        
        return all_flattened
    
    def export_to_csv(self, output_path: str):
        """Export flattened data to CSV"""
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
        
        print(f"✓ Exported {len(self.flattened_data)} data entries to: {output_path}")


# ============================================================================
# AI SEARCHER (unchanged)
# ============================================================================

class AISemanticSearcher:
    """Uses AI to semantically search flattened paths"""
    
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.api_key = api_key
        self.model = model
        openai.api_key = api_key
    
    def search(self, query: str, flattened: List[FlattenedCell], 
               top_k: int = 10, sample_size: int = 1000) -> List[Tuple[FlattenedCell, str]]:
        """AI semantic search over flattened paths"""
        
        path_list = []
        sample = flattened[:sample_size] if len(flattened) > sample_size else flattened
        
        for i, entry in enumerate(sample):
            path_list.append(f"{i}: {entry.path} = {entry.value} (type:{entry.value_type})")
        
        paths_text = "\n".join(path_list)
        
        prompt = f"""You have a flattened Excel database with these DATA entries (headers excluded):

{paths_text}

USER QUERY: {query}

Return ONLY a JSON array of indices matching the query.
Return top {top_k} matches. These are DATA cells only, not headers.

Example: [45, 123, 67]

JSON response:"""

        response = openai.ChatCompletion.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=200
        )
        
        result_text = response.choices[0].message.content.strip()
        
        try:
            indices = json.loads(result_text)
        except:
            indices = [int(x) for x in re.findall(r'\d+', result_text)]
        
        matches = []
        for idx in indices[:top_k]:
            if 0 <= idx < len(sample):
                matches.append((sample[idx], f"Match #{len(matches)+1}"))
        
        return matches


# ============================================================================
# VALUE EXTRACTOR (unchanged)
# ============================================================================

class ValueExtractor:
    """Extracts values algorithmically"""
    
    @staticmethod
    def extract(matches: List[Tuple[FlattenedCell, str]], operation: str = "return") -> Any:
        if not matches:
            return None
        
        if operation == "return":
            return matches[0][0].value
        
        elif operation == "sum":
            values = []
            for m, _ in matches:
                if m.value_type in ["number", "currency", "percentage", "numeric_string"]:
                    val = m.value
                    if isinstance(val, str):
                        val = float(val.replace(',', '').replace('$', '').replace('%', '').strip())
                    values.append(val)
            return sum(values) if values else None
        
        elif operation == "average":
            values = []
            for m, _ in matches:
                if m.value_type in ["number", "currency", "percentage", "numeric_string"]:
                    val = m.value
                    if isinstance(val, str):
                        val = float(val.replace(',', '').replace('$', '').replace('%', '').strip())
                    values.append(val)
            return sum(values) / len(values) if values else None
        
        elif operation in ["max", "min"]:
            values = []
            for m, _ in matches:
                if m.value_type in ["number", "currency", "percentage", "numeric_string"]:
                    val = m.value
                    if isinstance(val, str):
                        val = float(val.replace(',', '').replace('$', '').replace('%', '').strip())
                    values.append(val)
            return (max if operation == "max" else min)(values) if values else None
        
        elif operation == "count":
            return len(matches)
        
        elif operation == "list":
            return [m[0].value for m, _ in matches]
        
        return None


# ============================================================================
# MAIN QUERY ENGINE
# ============================================================================

class ExcelQueryEngine:
    """Main query engine with improved flattening"""
    
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.flattener = ImprovedWorkbookFlattener()
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
        """Query Excel with improved header filtering"""
        
        if self.flattened_data is None:
            raise ValueError("No workbook loaded. Call load_workbook() first.")
        
        if verbose:
            print("\n" + "="*80)
            print(f"QUERY: {query}")
            print(f"OPERATION: {operation}")
            print("="*80 + "\n")
        
        matches = self.searcher.search(query, self.flattened_data, top_k)
        
        if verbose:
            print(f"✓ Found {len(matches)} matches:")
            for entry, label in matches[:5]:
                print(f"  {label}: {entry.path}")
                print(f"    = {entry.value} (type: {entry.value_type}) [{entry.sheet}!{entry.cell_ref}]")
        
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
    
    def export_flattened(self, output_path: str):
        """Export flattened data"""
        self.flattener.export_to_csv(output_path)
    
    def get_sample_paths(self, n: int = 20) -> List[str]:
        """Get sample paths"""
        if self.flattened_data is None:
            return []
        return [entry.path for entry in self.flattened_data[:n]]


# ============================================================================
# USAGE
# ============================================================================

if __name__ == "__main__":
    engine = ExcelQueryEngine(api_key="your-key")
    engine.load_workbook("financial_report.xlsx")
    
    result = engine.query("What is the Q4 2024 revenue?")
    print(f"\nFinal Answer: {result.result}")
    
    engine.export_flattened("flattened_data_only.csv")
