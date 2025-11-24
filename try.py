"""
Smart Excel Extraction System
Extracts data from complex XLSX/XLSM files using natural language queries.
Automatically scans all sheets and returns formatted answers.
"""

import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

import openpyxl
from openpyxl.utils import get_column_letter
from openpyxl.utils.cell import range_boundaries
import json
import requests
from typing import Dict, List, Tuple, Optional, Any, Literal
from dataclasses import dataclass, asdict, field
from collections import defaultdict

# ==================== APPLY OPENPYXL FIX FIRST ====================

def apply_openpyxl_fix():
    """Fix for hidden images issue - must be called before loading workbooks"""
    try:
        import openpyxl.reader.drawings
        import openpyxl.reader.excel
        from openpyxl.reader.drawings import (
            fromstring,
            SpreadsheetDrawing,
            get_rel,
            get_rels_path,
            get_dependents,
            ChartSpace,
            read_chart,
            PILImage,
            IMAGE_NS,
            Image,
            BytesIO,
            warn,
        )

        def patched_find_images(archive, path):
            """Fixed version that skips hidden/NULL images"""
            src = archive.read(path)
            tree = fromstring(src)
            
            try:
                drawing = SpreadsheetDrawing.from_tree(tree)
            except TypeError:
                warn("DrawingML support incomplete - shapes will be lost.")
                return [], []

            rels_path = get_rels_path(path)
            deps = []
            if rels_path in archive.namelist():
                deps = get_dependents(archive, rels_path)

            charts = []
            for rel in drawing._chart_rels:
                try:
                    cs = get_rel(archive, deps, rel.id, ChartSpace)
                    chart = read_chart(cs)
                    chart.anchor = rel.anchor
                    charts.append(chart)
                except (TypeError, KeyError) as e:
                    warn(f"Unable to read chart: {e}")
                    continue

            images = []
            if not PILImage:
                return charts, images

            for rel in drawing._blip_rels:
                try:
                    dep = deps[rel.embed]
                    
                    # CRITICAL FIX: Skip hidden/NULL images
                    if dep.target == "xl/drawings/NULL":
                        warn("Hidden image skipped")
                        continue
                        
                    if dep.Type == IMAGE_NS:
                        try:
                            image = Image(BytesIO(archive.read(dep.target)))
                        except (OSError, KeyError) as e:
                            warn(f"Cannot read image: {e}")
                            continue
                            
                        if image.format.upper() == "WMF":
                            warn("WMF format not supported")
                            continue
                            
                        image.anchor = rel.anchor
                        images.append(image)
                except (KeyError, AttributeError, IndexError) as e:
                    warn(f"Error processing image: {e}")
                    continue
                    
            return charts, images

        openpyxl.reader.drawings.find_images = patched_find_images
        openpyxl.reader.excel.find_images = patched_find_images
        
        print("✓ OpenPyXL patch applied successfully")
        return True
        
    except Exception as e:
        print(f"⚠ Warning: Could not apply patch: {e}")
        return False

# Apply the fix immediately
PATCH_APPLIED = apply_openpyxl_fix()

# ==================== EXCEL LOADER ====================

class ExcelLoader:
    """Multi-strategy loader for problematic xlsm/xlsx files"""
    
    @staticmethod
    def load_workbook(file_path: str) -> Tuple[openpyxl.Workbook, openpyxl.Workbook]:
        """Try multiple strategies to load Excel files"""
        print(f"Loading: {file_path}")
        
        # Strategy 1: Normal load with VBA
        try:
            print("  Strategy 1: Normal load with VBA...")
            wb_data = openpyxl.load_workbook(
                file_path, 
                data_only=True, 
                keep_vba=True,
                read_only=False
            )
            wb_formula = openpyxl.load_workbook(
                file_path,
                data_only=False,
                keep_vba=True,
                read_only=False
            )
            print("  ✓ Success with Strategy 1")
            return wb_data, wb_formula
            
        except (KeyError, TypeError, AttributeError) as e:
            error_msg = str(e)
            print(f"  ✗ Strategy 1 failed: {error_msg[:100]}")
            
            # Strategy 2: Read-only mode
            if "NULL" in error_msg or "from_tree" in error_msg or "Nested" in error_msg:
                try:
                    print("  Strategy 2: Read-only mode (no drawings)...")
                    wb_data = openpyxl.load_workbook(
                        file_path,
                        read_only=True,
                        data_only=True,
                        keep_vba=False
                    )
                    wb_formula = openpyxl.load_workbook(
                        file_path,
                        read_only=False,
                        data_only=False,
                        keep_vba=False
                    )
                    print("  ✓ Success with Strategy 2 (read-only)")
                    return wb_data, wb_formula
                    
                except Exception as e2:
                    print(f"  ✗ Strategy 2 failed: {str(e2)[:100]}")
                    raise RuntimeError(
                        f"All loading strategies failed. Last error: {e2}"
                    )
            else:
                raise

# ==================== DATA STRUCTURES ====================

@dataclass
class TableRegion:
    """Detected table region"""
    min_row: int
    max_row: int
    min_col: int
    max_col: int
    orientation: str  # 'horizontal' or 'vertical'
    headers: List[str]
    table_type: str  # 'standard', 'pivot', 'multi-level'

@dataclass
class ExtractionInstruction:
    """Enhanced extraction instruction supporting ranges"""
    table_id: int
    extraction_type: Literal['single_cell', 'cell_range', 'row', 'column', 'table_section']
    sheet_name: str = ""
    
    # For single cell
    row: Optional[int] = None
    col: Optional[int] = None
    cell_address: Optional[str] = None
    
    # For ranges
    start_row: Optional[int] = None
    start_col: Optional[int] = None
    end_row: Optional[int] = None
    end_col: Optional[int] = None
    range_address: Optional[str] = None
    
    # Metadata
    extraction_context: str = ""
    include_formatting: bool = False
    include_formulas: bool = False

@dataclass
class WorkbookIndex:
    """Complete index of all sheets and their tables"""
    sheet_name: str
    tables: List[TableRegion]
    merged_cells: List[dict]
    meta Dict

# ==================== TABLE DETECTOR ====================

class TableDetector:
    """Detects table boundaries - works with regular and read-only workbooks"""
    
    def __init__(self, worksheet_data, worksheet_formula=None):
        self.ws_data = worksheet_data
        self.ws_formula = worksheet_formula
        self.is_readonly = not hasattr(worksheet_data, 'cell')
        
        if hasattr(worksheet_data, 'merged_cells'):
            self.merged_ranges = list(worksheet_data.merged_cells.ranges)
        else:
            self.merged_ranges = []
        
        self._cell_cache = {}
        self._max_row = None
        self._max_col = None
    
    def _get_dimensions(self) -> Tuple[int, int]:
        """Get worksheet dimensions safely"""
        if self._max_row and self._max_col:
            return self._max_row, self._max_col
        
        if hasattr(self.ws_data, 'max_row'):
            self._max_row = self.ws_data.max_row or 1
            self._max_col = self.ws_data.max_column or 1
        else:
            self._max_row = 0
            self._max_col = 0
            
            for row in self.ws_data.iter_rows():
                if row[0].row > self._max_row:
                    self._max_row = row[0].row
                for cell in row:
                    if cell.column > self._max_col:
                        self._max_col = cell.column
            
            self._max_row = max(self._max_row, 1)
            self._max_col = max(self._max_col, 1)
        
        return self._max_row, self._max_col
    
    def _get_cell_safe(self, row: int, col: int):
        """Safe cell access for both modes"""
        cache_key = (row, col)
        
        if cache_key in self._cell_cache:
            return self._cell_cache[cache_key]
        
        cell = None
        
        if self.is_readonly:
            try:
                for row_cells in self.ws_data.iter_rows(
                    min_row=row, max_row=row,
                    min_col=col, max_col=col
                ):
                    for c in row_cells:
                        cell = c
                        break
                    break
            except Exception:
                cell = None
        else:
            try:
                cell = self.ws_data.cell(row, col)
            except Exception:
                cell = None
        
        self._cell_cache[cache_key] = cell
        return cell
    
    def _preload_region(self, min_row: int, max_row: int, 
                       min_col: int, max_col: int):
        """Preload region for performance"""
        if self.is_readonly:
            try:
                for row_cells in self.ws_data.iter_rows(
                    min_row=min_row, max_row=max_row,
                    min_col=min_col, max_col=max_col
                ):
                    for cell in row_cells:
                        cache_key = (cell.row, cell.column)
                        self._cell_cache[cache_key] = cell
            except Exception as e:
                print(f"    ⚠ Warning preloading region: {e}")
    
    def detect_all_tables(self) -> List[TableRegion]:
        """Find all table regions"""
        tables = []
        visited_cells = set()
        
        max_row, max_col = self._get_dimensions()
        scan_max_row = min(max_row, 1000)
        scan_max_col = min(max_col, 50)
        
        print(f"    Scanning {scan_max_row} rows × {scan_max_col} columns...")
        
        self._preload_region(1, scan_max_row, 1, scan_max_col)
        
        for row in range(1, scan_max_row + 1):
            for col in range(1, scan_max_col + 1):
                coord = (row, col)
                
                if coord in visited_cells:
                    continue
                
                cell = self._get_cell_safe(row, col)
                if not cell or cell.value is None:
                    continue
                
                if self._is_likely_header(cell, row, col):
                    table = self._extract_table_from_header(row, col)
                    if table:
                        tables.append(table)
                        for r in range(table.min_row, table.max_row + 1):
                            for c in range(table.min_col, table.max_col + 1):
                                visited_cells.add((r, c))
        
        print(f"    ✓ Found {len(tables)} tables")
        return tables
    
    def _is_likely_header(self, cell, row: int, col: int) -> bool:
        """Heuristics to identify headers"""
        if cell.value is None:
            return False
        
        has_bold = False
        has_fill = False
        
        try:
            if hasattr(cell, 'font') and cell.font:
                has_bold = bool(cell.font.bold)
        except Exception:
            pass
        
        try:
            if hasattr(cell, 'fill') and cell.fill:
                has_fill = (
                    cell.fill.start_color and 
                    hasattr(cell.fill.start_color, 'index') and
                    cell.fill.start_color.index != '00000000'
                )
        except Exception:
            pass
        
        is_text = isinstance(cell.value, str)
        
        cell_below = self._get_cell_safe(row + 1, col)
        cell_right = self._get_cell_safe(row, col + 1)
        
        has_data_below = cell_below and cell_below.value is not None
        has_data_right = cell_right and cell_right.value is not None
        
        return is_text and ((has_bold or has_fill) or 
                           (has_data_below or has_data_right))
    
    def _extract_table_from_header(self, start_row: int, 
                                   start_col: int) -> Optional[TableRegion]:
        """Extract table from header position"""
        
        horizontal_density = self._count_consecutive_cells(
            start_row, start_col, 'horizontal'
        )
        vertical_density = self._count_consecutive_cells(
            start_row, start_col, 'vertical'
        )
        
        if horizontal_density < 2 and vertical_density < 2:
            return None
        
        orientation = (
            'horizontal' if horizontal_density >= vertical_density 
            else 'vertical'
        )
        
        if orientation == 'horizontal':
            return self._extract_horizontal_table(start_row, start_col)
        else:
            return self._extract_vertical_table(start_row, start_col)
    
    def _count_consecutive_cells(self, row: int, col: int, 
                                 direction: str) -> int:
        """Count consecutive non-empty cells"""
        count = 0
        max_row, max_col = self._get_dimensions()
        
        if direction == 'horizontal':
            max_check = min(col + 20, max_col + 1)
            for c in range(col, max_check):
                cell = self._get_cell_safe(row, c)
                if cell and cell.value is not None:
                    count += 1
                elif count > 0:
                    break
        else:
            max_check = min(row + 20, max_row + 1)
            for r in range(row, max_check):
                cell = self._get_cell_safe(r, col)
                if cell and cell.value is not None:
                    count += 1
                elif count > 0:
                    break
        
        return count
    
    def _extract_horizontal_table(self, header_row: int, 
                                  start_col: int) -> TableRegion:
        """Extract horizontal table"""
        max_row, max_col = self._get_dimensions()
        
        end_col = start_col
        for col in range(start_col, max_col + 1):
            cell = self._get_cell_safe(header_row, col)
            if not cell or cell.value is None:
                break
            end_col = col
        
        bottom_row = header_row + 1
        for row in range(header_row + 1, min(max_row + 1, header_row + 1000)):
            has_data = False
            for col in range(start_col, end_col + 1):
                cell = self._get_cell_safe(row, col)
                if cell and cell.value is not None:
                    has_data = True
                    break
            
            if not has_
                break
            bottom_row = row
        
        headers = []
        for col in range(start_col, end_col + 1):
            cell = self._get_cell_safe(header_row, col)
            header_val = cell.value if cell else None
            headers.append(
                str(header_val) if header_val else f"Column_{col}"
            )
        
        return TableRegion(
            min_row=header_row,
            max_row=bottom_row,
            min_col=start_col,
            max_col=end_col,
            orientation='horizontal',
            headers=headers,
            table_type='standard'
        )
    
    def _extract_vertical_table(self, start_row: int, 
                               header_col: int) -> TableRegion:
        """Extract vertical table"""
        max_row, max_col = self._get_dimensions()
        
        end_row = start_row
        for row in range(start_row, max_row + 1):
            cell = self._get_cell_safe(row, header_col)
            if not cell or cell.value is None:
                break
            end_row = row
        
        end_col = header_col + 1
        for col in range(header_col + 1, min(max_col + 1, header_col + 50)):
            has_data = False
            for row in range(start_row, end_row + 1):
                cell = self._get_cell_safe(row, col)
                if cell and cell.value is not None:
                    has_data = True
                    break
            
            if not has_
                break
            end_col = col
        
        headers = []
        for row in range(start_row, end_row + 1):
            cell = self._get_cell_safe(row, header_col)
            header_val = cell.value if cell else None
            headers.append(
                str(header_val) if header_val else f"Row_{row}"
            )
        
        return TableRegion(
            min_row=start_row,
            max_row=end_row,
            min_col=header_col,
            max_col=end_col,
            orientation='vertical',
            headers=headers,
            table_type='standard'
        )
    
    def get_merged_cell_info(self) -> List[dict]:
        """Get merged cell ranges"""
        if not self.merged_ranges:
            return []
        
        merged_info = []
        
        for merged_range in self.merged_ranges:
            try:
                bounds = merged_range.bounds
                min_col, min_row, max_col, max_row = bounds
                
                top_left_cell = self._get_cell_safe(min_row, min_col)
                top_left_value = top_left_cell.value if top_left_cell else None
                
                merged_info.append({
                    'range': str(merged_range),
                    'min_row': min_row,
                    'max_row': max_row,
                    'min_col': min_col,
                    'max_col': max_col,
                    'value': top_left_value
                })
            except Exception as e:
                print(f"    ⚠ Error processing merged range: {e}")
                continue
        
        return merged_info
    
    def clear_cache(self):
        """Clear cell cache"""
        self._cell_cache.clear()

# ==================== METADATA BUILDER ====================

class StructureMetadataBuilder:
    """Convert Excel structure to LLM-friendly metadata"""
    
    @staticmethod
    def build_metadata(worksheet_name: str, tables: List[TableRegion], 
                      merged_cells: List[Dict]) -> Dict:
        """Create metadata without sensitive values"""
        
        metadata = {
            'worksheet_name': worksheet_name,
            'total_tables': len(tables),
            'tables': [],
            'merged_cells': merged_cells
        }
        
        for idx, table in enumerate(tables):
            table_meta = {
                'table_id': idx,
                'location': {
                    'top_left': f"{get_column_letter(table.min_col)}{table.min_row}",
                    'bottom_right': f"{get_column_letter(table.max_col)}{table.max_row}",
                    'row_range': [table.min_row, table.max_row],
                    'col_range': [table.min_col, table.max_col]
                },
                'orientation': table.orientation,
                'headers': table.headers,
                'table_type': table.table_type,
                'dimensions': {
                    'rows': table.max_row - table.min_row + 1,
                    'cols': table.max_col - table.min_col + 1
                }
            }
            metadata['tables'].append(table_meta)
        
        return metadata

# ==================== MULTI-SHEET SCANNER ====================

class MultiSheetScanner:
    """Scans entire workbook and builds searchable index"""
    
    def __init__(self, workbook_data, workbook_formula):
        self.wb_data = workbook_data
        self.wb_formula = workbook_formula
        self.indexes: List[WorkbookIndex] = []
    
    def scan_all_sheets(self) -> List[WorkbookIndex]:
        """Scan every sheet and detect tables"""
        print(f"\n{'='*60}")
        print("SCANNING ALL SHEETS")
        print(f"{'='*60}")
        
        sheet_names = self.wb_data.sheetnames
        print(f"Found {len(sheet_names)} sheets: {', '.join(sheet_names)}")
        
        for sheet_name in sheet_names:
            try:
                print(f"\n[Sheet: {sheet_name}]")
                
                ws_data = self.wb_data[sheet_name]
                ws_formula = None
                
                if self.wb_formula and not self.wb_formula.read_only:
                    try:
                        ws_formula = self.wb_formula[sheet_name]
                    except Exception:
                        pass
                
                detector = TableDetector(ws_data, ws_formula)
                tables = detector.detect_all_tables()
                merged_cells = detector.get_merged_cell_info()
                
                metadata = StructureMetadataBuilder.build_metadata(
                    sheet_name, tables, merged_cells
                )
                
                self.indexes.append(WorkbookIndex(
                    sheet_name=sheet_name,
                    tables=tables,
                    merged_cells=merged_cells,
                    metadata=metadata
                ))
                
                detector.clear_cache()
                
            except Exception as e:
                print(f"  ⚠ Error scanning sheet '{sheet_name}': {e}")
                continue
        
        print(f"\n{'='*60}")
        print(f"✓ Indexed {len(self.indexes)} sheets successfully")
        print(f"{'='*60}\n")
        
        return self.indexes
    
    def get_combined_metadata(self) -> Dict:
        """Combine all sheet metadata for LLM"""
        combined = {
            'workbook': {
                'total_sheets': len(self.indexes),
                'sheets': []
            }
        }
        
        for index in self.indexes:
            combined['workbook']['sheets'].append(index.metadata)
        
        return combined

# ==================== OPENAI QUERY PROCESSOR ====================

class SmartQueryProcessor:
    """Process queries and find data across sheets"""
    
    def __init__(self, api_base_url: str, api_key: str, model: str = "gpt-4"):
        self.api_base_url = api_base_url.rstrip('/')
        self.api_key = api_key
        self.model = model
    
    def process_query_with_answer(self, user_query: str, 
                                  combined_meta Dict) -> Tuple[List[ExtractionInstruction], str, str]:
        """Find data and generate answer template"""
        
        system_prompt = """You are an Excel data retrieval assistant. Given metadata about ALL sheets in a workbook and a user query, you must:

1. Identify which sheet(s) contain the requested data
2. Provide precise extraction instructions
3. Generate a natural language answer template

EXTRACTION TYPES:
- single_cell: One specific value
- cell_range: Multiple related values
- row: Entire row
- column: Entire column
- table_section: Section of a table

Return JSON:
{
  "extractions": [
    {
      "sheet_name": "<exact sheet name>",
      "table_id": <int>,
      "extraction_type": "single_cell" | "cell_range" | "row" | "column" | "table_section",
      "row": <int>,
      "col": <int>,
      "cell_address": "<B5>",
      "start_row": <int>,
      "start_col": <int>,
      "end_row": <int>,
      "end_col": <int>,
      "range_address": "<B5:D10>",
      "extraction_context": "<description>",
      "include_formatting": <bool>,
      "include_formulas": <bool>
    }
  ],
  "answer_template": "<Natural language with {placeholders}>",
  "answer_format": "number" | "currency" | "percentage" | "date" | "text" | "table"
}

EXAMPLE:
Query: "What was the EBITDA for Asia in Q3 2024?"
{
  "extractions": [{
    "sheet_name": "Q3 2024",
    "table_id": 0,
    "extraction_type": "single_cell",
    "row": 15,
    "col": 4,
    "cell_address": "D15",
    "extraction_context": "Asia EBITDA Q3 2024",
    "include_formatting": true
  }],
  "answer_template": "The EBITDA for Asia in Q3 2024 was {D15}.",
  "answer_format": "currency"
}

Be precise. Search ALL sheets."""

        user_message = f"""Workbook Structure:
{json.dumps(combined_metadata, indent=2)}

User Query: {user_query}

Return extraction instructions and answer template as JSON."""

        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        
        payload = {
            'model': self.model,
            'messages': [
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': user_message}
            ],
            'response_format': {'type': 'json_object'},
            'temperature': 0
        }
        
        try:
            response = requests.post(
                f'{self.api_base_url}/chat/completions',
                headers=headers,
                json=payload,
                timeout=60
            )
            
            if not response.ok:
                error_detail = self._parse_error_response(response)
                raise RuntimeError(
                    f"OpenAI API error {response.status_code}: {error_detail}"
                )
            
            result = response.json()
            
            if not isinstance(result, dict):
                raise RuntimeError(f"Expected dict, got {type(result).__name__}")
            
            if 'error' in result:
                raise RuntimeError(f"API error: {result['error']}")
            
            content = result['choices'][0]['message']['content']
            extraction_data = json.loads(content)
            
            instructions = []
            for item in extraction_data.get('extractions', []):
                instructions.append(ExtractionInstruction(
                    sheet_name=item.get('sheet_name', ''),
                    table_id=int(item['table_id']),
                    extraction_type=item['extraction_type'],
                    row=item.get('row'),
                    col=item.get('col'),
                    cell_address=item.get('cell_address'),
                    start_row=item.get('start_row'),
                    start_col=item.get('start_col'),
                    end_row=item.get('end_row'),
                    end_col=item.get('end_col'),
                    range_address=item.get('range_address'),
                    extraction_context=item.get('extraction_context', ''),
                    include_formatting=item.get('include_formatting', False),
                    include_formulas=item.get('include_formulas', False)
                ))
            
            answer_template = extraction_data.get('answer_template', '')
            answer_format = extraction_data.get('answer_format', 'text')
            
            return instructions, answer_template, answer_format
            
        except Exception as e:
            print(f"Error processing query: {e}")
            raise
    
    def _parse_error_response(self, response: requests.Response) -> str:
        """Extract error from failed response"""
        try:
            error_json = response.json()
            if isinstance(error_json, dict) and 'error' in error_json:
                return str(error_json['error'])
            return str(error_json)
        except Exception:
            return response.text[:500]

# ==================== PRECISE EXTRACTOR ====================

class PreciseExtractor:
    """Execute extraction with formatting support"""
    
    def __init__(self, workbook_data, workbook_formula):
        self.wb_data = workbook_data
        self.wb_formula = workbook_formula
    
    def extract_values(self, worksheet_name: str, 
                      instructions: List[ExtractionInstruction]) -> Dict[str, Any]:
        """Execute extraction for multiple types"""
        
        ws_data = self.wb_data[worksheet_name]
        ws_formula = self.wb_formula[worksheet_name] if self.wb_formula else None
        
        results = {}
        
        for idx, instruction in enumerate(instructions):
            try:
                if instruction.extraction_type == 'single_cell':
                    result = self._extract_single_cell(
                        ws_data, ws_formula, instruction
                    )
                    key = instruction.cell_address or f"cell_{idx}"
                    
                elif instruction.extraction_type in ['cell_range', 'row', 'column', 'table_section']:
                    result = self._extract_range(
                        ws_data, ws_formula, instruction
                    )
                    key = instruction.range_address or f"range_{idx}"
                else:
                    result = {'error': f'Unknown type: {instruction.extraction_type}'}
                    key = f"unknown_{idx}"
                
                results[key] = result
                
            except Exception as e:
                results[f"error_{idx}"] = {
                    'error': str(e),
                    'context': instruction.extraction_context
                }
        
        return results
    
    def _extract_single_cell(self, ws_data, ws_formula, 
                            instruction: ExtractionInstruction) -> Dict[str, Any]:
        """Extract single cell"""
        
        cell_data = ws_data.cell(instruction.row, instruction.col)
        cell_formula = ws_formula.cell(instruction.row, instruction.col) if ws_formula else None
        
        value = cell_data.value
        
        if value is None and cell_formula and cell_formula.value:
            if isinstance(cell_formula.value, str) and cell_formula.value.startswith('='):
                value = f"FORMULA_NOT_CACHED: {cell_formula.value}"
        
        result = {
            'value': value,
            'context': instruction.extraction_context,
            'coordinates': {
                'row': instruction.row,
                'col': instruction.col,
                'address': instruction.cell_address
            }
        }
        
        if instruction.include_formatting:
            result['formatting'] = self._get_cell_formatting(cell_data)
        
        if instruction.include_formulas and cell_formula:
            result['formula'] = cell_formula.value if cell_formula.value else None
        
        return result
    
    def _extract_range(self, ws_data, ws_formula, 
                      instruction: ExtractionInstruction) -> Dict[str, Any]:
        """Extract range of cells"""
        
        start_row = instruction.start_row
        end_row = instruction.end_row
        start_col = instruction.start_col
        end_col = instruction.end_col
        
        data_array = []
        
        for row in range(start_row, end_row + 1):
            row_data = []
            
            for col in range(start_col, end_col + 1):
                cell_data = ws_data.cell(row, col)
                value = cell_data.value
                row_data.append(value)
            
            data_array.append(row_data)
        
        result = {
            'extraction_type': instruction.extraction_type,
            'data': data_array,
            'context': instruction.extraction_context,
            'dimensions': {
                'rows': end_row - start_row + 1,
                'cols': end_col - start_col + 1
            },
            'coordinates': {
                'start_row': start_row,
                'start_col': start_col,
                'end_row': end_row,
                'end_col': end_col,
                'range': instruction.range_address
            }
        }
        
        return result
    
    def _get_cell_formatting(self, cell) -> Dict[str, Any]:
        """Extract formatting"""
        formatting = {}
        
        try:
            if hasattr(cell, 'number_format'):
                formatting['number_format'] = cell.number_format
                formatting['is_currency'] = '$' in str(cell.number_format) or '€' in str(cell.number_format)
                formatting['is_percentage'] = '%' in str(cell.number_format)
            
            if hasattr(cell, 'font') and cell.font:
                formatting['font'] = {
                    'bold': cell.font.bold,
                    'size': cell.font.size
                }
                
        except Exception as e:
            formatting['error'] = f"Could not extract formatting: {e}"
        
        return formatting

# ==================== ANSWER FORMATTER ====================

class AnswerFormatter:
    """Format extracted data into natural language"""
    
    @staticmethod
    def format_answer(template: str, extracted_values: Dict, 
                     answer_format: str) -> str:
        """Replace placeholders with formatted values"""
        
        answer = template
        
        for key, data in extracted_values.items():
            placeholder = f"{{{key}}}"
            
            if placeholder in answer:
                if 'data' in 
                    formatted_value = AnswerFormatter._format_range(
                        data, answer_format
                    )
                else:
                    formatted_value = AnswerFormatter._format_single_value(
                        data, answer_format
                    )
                
                answer = answer.replace(placeholder, formatted_value)
        
        return answer
    
    @staticmethod
    def _format_single_value( Dict, answer_format: str) -> str:
        """Format single value"""
        value = data.get('value')
        formatting = data.get('formatting', {})
        
        if value is None:
            return "N/A"
        
        if answer_format == 'currency' or formatting.get('is_currency'):
            try:
                return f"${float(value):,.2f}"
            except (ValueError, TypeError):
                return str(value)
        
        elif answer_format == 'percentage' or formatting.get('is_percentage'):
            try:
                return f"{float(value) * 100:.2f}%"
            except (ValueError, TypeError):
                return str(value)
        
        elif answer_format == 'number':
            try:
                return f"{float(value):,.2f}"
            except (ValueError, TypeError):
                return str(value)
        
        else:
            return str(value)
    
    @staticmethod
    def _format_range( Dict, answer_format: str) -> str:
        """Format range of values"""
        data_array = data.get('data', [])
        
        if not data_array:
            return "No data"
        
        if len(data_array) == 1:
            values = data_array[0]
            formatted = [
                AnswerFormatter._format_single_value({'value': v}, answer_format)
                for v in values
            ]
            return ', '.join(formatted)
        
        elif all(len(row) == 1 for row in data_array):
            values = [row[0] for row in data_array]
            formatted = [
                AnswerFormatter._format_single_value({'value': v}, answer_format)
                for v in values
            ]
            return ', '.join(formatted)
        
        else:
            lines = []
            for row in data_array:
                formatted_row = [
                    AnswerFormatter._format_single_value({'value': v}, answer_format)
                    for v in row
                ]
                lines.append(' | '.join(formatted_row))
            return '\n' + '\n'.join(lines)

# ==================== MAIN SYSTEM ====================

class SmartExcelExtractionSystem:
    """
    Smart system that:
    - Scans all sheets automatically
    - Returns natural language answers
    - No sheet name required
    """
    
    def __init__(self, openai_base_url: str, openai_api_key: str, model: str = "gpt-4"):
        self.query_processor = SmartQueryProcessor(
            api_base_url=openai_base_url,
            api_key=openai_api_key,
            model=model
        )
    
    def process_file_smart(self, file_path: str, user_query: str) -> str:
        """
        Process file with natural language query.
        Returns formatted answer string.
        """
        
        print(f"\n{'='*60}")
        print(f"SMART EXCEL QUERY SYSTEM")
        print(f"{'='*60}")
        print(f"File: {file_path}")
        print(f"Query: {user_query}")
        
        # Load workbook
        print(f"\n[1/5] Loading workbook...")
        wb_data, wb_formula = ExcelLoader.load_workbook(file_path)
        
        # Scan all sheets
        print(f"[2/5] Scanning all sheets...")
        scanner = MultiSheetScanner(wb_data, wb_formula)
        indexes = scanner.scan_all_sheets()
        combined_metadata = scanner.get_combined_metadata()
        
        # Query LLM
        print(f"[3/5] Processing query across all sheets...")
        instructions, answer_template, answer_format = \
            self.query_processor.process_query_with_answer(user_query, combined_metadata)
        
        print(f"    Found data in {len(set(i.sheet_name for i in instructions))} sheet(s)")
        
        # Extract values
        print(f"[4/5] Extracting values...")
        all_extracted = {}
        
        for instruction in instructions:
            sheet_name = instruction.sheet_name
            extractor = PreciseExtractor(wb_data, wb_formula)
            result = extractor.extract_values(sheet_name, [instruction])
            all_extracted.update(result)
        
        # Format answer
        print(f"[5/5] Formatting answer...")
        final_answer = AnswerFormatter.format_answer(
            answer_template, all_extracted, answer_format
        )
        
        print(f"\n{'='*60}")
        print("✓ ANSWER READY")
        print(f"{'='*60}\n")
        
        return final_answer


# ==================== USAGE ====================

if __name__ == "__main__":
    
    # Initialize system
    system = SmartExcelExtractionSystem(
        openai_base_url="https://your-private-openai-endpoint.com/v1",
        openai_api_key="your-api-key-here",
        model="gpt-4"
    )
    
    # Example 1: Simple query
    answer = system.process_file_smart(
        file_path="financial_data.xlsm",
        user_query="What was the EBITDA for Asia in Q3 2024?"
    )
    print("ANSWER:")
    print(answer)
    
    # Example 2: Multiple values
    answer = system.process_file_smart(
        file_path="financial_data.xlsm",
        user_query="Show me all quarterly revenue for 2024"
    )
    print("ANSWER:")
    print(answer)
    
    # Example 3: Cross-sheet query
    answer = system.process_file_smart(
        file_path="financial_data.xlsm",
        user_query="What is the total headcount across all regions?"
    )
    print("ANSWER:")
    print(answer)
