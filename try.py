import openpyxl
from openpyxl.utils import get_column_letter, column_index_from_string
from openpyxl.utils.cell import coordinate_from_string, range_boundaries
import json
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
import requests
from collections import defaultdict

import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# ==================== APPLY MONKEY PATCH FIRST ====================
# This MUST be before any load_workbook calls

def apply_openpyxl_fix():
    """Apply fix for hidden images issue - call this ONCE at startup"""
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
            """Fixed version that skips hidden images"""
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

            # Handle charts
            charts = []
            for rel in drawing._chart_rels:
                try:
                    cs = get_rel(archive, deps, rel.id, ChartSpace)
                    chart = read_chart(cs)
                    chart.anchor = rel.anchor
                    charts.append(chart)
                except (TypeError, KeyError) as e:
                    warn(f"Unable to read chart {rel.id}: {e}")
                    continue

            # Handle images with NULL fix
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

        # Apply the patches
        openpyxl.reader.drawings.find_images = patched_find_images
        openpyxl.reader.excel.find_images = patched_find_images
        
        print("✓ OpenPyXL patch applied successfully")
        return True
        
    except Exception as e:
        print(f"⚠ Warning: Could not apply patch: {e}")
        return False

# Apply the fix IMMEDIATELY
PATCH_APPLIED = apply_openpyxl_fix()

# ==================== ROBUST EXCEL LOADER ====================

import openpyxl
from typing import Tuple, Optional

class ExcelLoader:
    """Multi-strategy loader for problematic xlsm files"""
    
    @staticmethod
    def load_workbook(file_path: str) -> Tuple[openpyxl.Workbook, openpyxl.Workbook]:
        """
        Try multiple strategies to load xlsm/xlsx files
        """
        print(f"Loading: {file_path}")
        
        # Strategy 1: Normal load with patch (works 90% of time)
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
            
            # Strategy 2: Read-only mode (skips drawings entirely)
            if "NULL" in error_msg or "from_tree" in error_msg or "Nested" in error_msg:
                try:
                    print("  Strategy 2: Read-only mode (no drawings)...")
                    wb_data = openpyxl.load_workbook(
                        file_path,
                        read_only=True,  # Skips problematic drawing parsing
                        data_only=True,
                        keep_vba=False
                    )
                    
                    # For formula workbook, we need write mode
                    # So load without drawings/images
                    wb_formula = openpyxl.load_workbook(
                        file_path,
                        read_only=False,
                        data_only=False,
                        keep_vba=False  # Disable VBA to avoid issues
                    )
                    print("  ✓ Success with Strategy 2 (read-only)")
                    return wb_data, wb_formula
                    
                except Exception as e2:
                    print(f"  ✗ Strategy 2 failed: {str(e2)[:100]}")
                    
                    # Strategy 3: Pandas conversion (last resort)
                    try:
                        print("  Strategy 3: Convert via pandas...")
                        import pandas as pd
                        import tempfile
                        import os
                        
                        # Create temp file
                        temp_fd, temp_path = tempfile.mkstemp(suffix='.xlsx')
                        os.close(temp_fd)
                        
                        # Read with pandas (more forgiving)
                        excel_file = pd.ExcelFile(file_path, engine='openpyxl')
                        
                        # Write to clean xlsx
                        with pd.ExcelWriter(temp_path, engine='openpyxl') as writer:
                            for sheet_name in excel_file.sheet_names:
                                try:
                                    df = pd.read_excel(excel_file, sheet_name=sheet_name)
                                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                                except Exception as sheet_error:
                                    print(f"    ⚠ Skipped sheet '{sheet_name}': {sheet_error}")
                                    continue
                        
                        # Load the cleaned file
                        wb_data = openpyxl.load_workbook(temp_path, data_only=True)
                        wb_formula = openpyxl.load_workbook(temp_path, data_only=False)
                        
                        print(f"  ✓ Success with Strategy 3 (cleaned file: {temp_path})")
                        print(f"  ⚠ Note: Macros and formatting lost in conversion")
                        
                        return wb_data, wb_formula
                        
                    except Exception as e3:
                        print(f"  ✗ Strategy 3 failed: {str(e3)[:100]}")
                        raise RuntimeError(
                            f"All loading strategies failed. Last error: {e3}"
                        )
            else:
                raise

@dataclass
class CellInfo:
    """Metadata about a cell"""
    row: int
    col: int
    value: Any
    is_merged: bool
    is_formula: bool
    has_formatting: bool
    coord: str

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

class TableDetector:
    """
    Detects table boundaries and structure in Excel sheets.
    Compatible with both regular and read-only workbooks.
    """
    
    def __init__(self, worksheet_data, worksheet_formula=None):
        self.ws_data = worksheet_data
        self.ws_formula = worksheet_formula
        
        # Check if read-only mode
        self.is_readonly = not hasattr(worksheet_data, 'cell')
        
        # Handle merged cells (not available in read-only mode)
        if hasattr(worksheet_data, 'merged_cells'):
            self.merged_ranges = list(worksheet_data.merged_cells.ranges)
        else:
            self.merged_ranges = []
            print("    ⚠ Read-only mode: merged cells detection disabled")
        
        # Cache for cell values in read-only mode
        self._cell_cache = {}
        self._max_row = None
        self._max_col = None
    
    def _get_dimensions(self) -> Tuple[int, int]:
        """Get worksheet dimensions safely"""
        if self._max_row and self._max_col:
            return self._max_row, self._max_col
        
        if hasattr(self.ws_data, 'max_row'):
            # Regular workbook
            self._max_row = self.ws_data.max_row or 1
            self._max_col = self.ws_data.max_column or 1
        else:
            # Read-only workbook: must scan
            self._max_row = 0
            self._max_col = 0
            
            for row in self.ws_data.iter_rows():
                if row[0].row > self._max_row:
                    self._max_row = row[0].row
                for cell in row:
                    if cell.column > self._max_col:
                        self._max_col = cell.column
            
            # Set minimum dimensions
            self._max_row = max(self._max_row, 1)
            self._max_col = max(self._max_col, 1)
        
        return self._max_row, self._max_col
    
    def _get_cell_safe(self, row: int, col: int):
        """
        Safe cell access for both regular and read-only workbooks.
        Returns cell object or None if not found.
        """
        cache_key = (row, col)
        
        # Check cache first
        if cache_key in self._cell_cache:
            return self._cell_cache[cache_key]
        
        cell = None
        
        if self.is_readonly:
            # Read-only mode: use iter_rows
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
            # Regular mode: direct access
            try:
                cell = self.ws_data.cell(row, col)
            except Exception:
                cell = None
        
        # Cache the result
        self._cell_cache[cache_key] = cell
        return cell
    
    def _preload_region(self, min_row: int, max_row: int, 
                       min_col: int, max_col: int):
        """
        Preload a region of cells into cache for better performance.
        Critical for read-only mode performance.
        """
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
        """Find all table regions in the worksheet"""
        tables = []
        visited_cells = set()
        
        max_row, max_col = self._get_dimensions()
        
        # Limit scanning for performance
        scan_max_row = min(max_row, 1000)
        scan_max_col = min(max_col, 50)
        
        print(f"    Scanning {scan_max_row} rows × {scan_max_col} columns...")
        
        # Preload the entire scan region for performance
        self._preload_region(1, scan_max_row, 1, scan_max_col)
        
        for row in range(1, scan_max_row + 1):
            for col in range(1, scan_max_col + 1):
                coord = (row, col)
                
                if coord in visited_cells:
                    continue
                
                cell = self._get_cell_safe(row, col)
                if not cell or cell.value is None:
                    continue
                
                # Check if this looks like a table header
                if self._is_likely_header(cell, row, col):
                    table = self._extract_table_from_header(row, col)
                    if table:
                        tables.append(table)
                        # Mark cells as visited
                        for r in range(table.min_row, table.max_row + 1):
                            for c in range(table.min_col, table.max_col + 1):
                                visited_cells.add((r, c))
        
        print(f"    ✓ Found {len(tables)} tables")
        return tables
    
    def _is_likely_header(self, cell, row: int, col: int) -> bool:
        """
        Heuristics to identify header cells.
        Works with limited formatting info in read-only mode.
        """
        if cell.value is None:
            return False
        
        # Check for bold formatting (if available)
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
        
        # Check if it's text (not number)
        is_text = isinstance(cell.value, str)
        
        # Check if cells below/to the right have data
        cell_below = self._get_cell_safe(row + 1, col)
        cell_right = self._get_cell_safe(row, col + 1)
        
        has_data_below = cell_below and cell_below.value is not None
        has_data_right = cell_right and cell_right.value is not None
        
        # Header criteria: text + (formatting OR has data adjacent)
        return is_text and ((has_bold or has_fill) or 
                           (has_data_below or has_data_right))
    
    def _extract_table_from_header(self, start_row: int, 
                                   start_col: int) -> Optional[TableRegion]:
        """Extract complete table starting from header position"""
        
        # Determine orientation by checking data density
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
        """Count consecutive non-empty cells in a direction"""
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
        else:  # vertical
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
        """Extract table with horizontal headers (columns)"""
        max_row, max_col = self._get_dimensions()
        
        # Find rightmost column of headers
        end_col = start_col
        for col in range(start_col, max_col + 1):
            cell = self._get_cell_safe(header_row, col)
            if not cell or cell.value is None:
                break
            end_col = col
        
        # Find bottom row of data
        bottom_row = header_row + 1
        for row in range(header_row + 1, min(max_row + 1, header_row + 1000)):
            # Check if row has any data
            has_data = False
            for col in range(start_col, end_col + 1):
                cell = self._get_cell_safe(row, col)
                if cell and cell.value is not None:
                    has_data = True
                    break
            
            if not has_
                break
            bottom_row = row
        
        # Extract headers
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
        """Extract table with vertical headers (rows)"""
        max_row, max_col = self._get_dimensions()
        
        # Find bottom row of headers
        end_row = start_row
        for row in range(start_row, max_row + 1):
            cell = self._get_cell_safe(row, header_col)
            if not cell or cell.value is None:
                break
            end_row = row
        
        # Find rightmost column of data
        end_col = header_col + 1
        for col in range(header_col + 1, min(max_col + 1, header_col + 50)):
            # Check if column has any data
            has_data = False
            for row in range(start_row, end_row + 1):
                cell = self._get_cell_safe(row, col)
                if cell and cell.value is not None:
                    has_data = True
                    break
            
            if not has_data:
                break
            end_col = col
        
        # Extract headers
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
        """
        Get all merged cell ranges.
        Returns empty list in read-only mode.
        """
        if not self.merged_ranges:
            return []
        
        merged_info = []
        
        for merged_range in self.merged_ranges:
            try:
                # Get bounds
                bounds = merged_range.bounds
                min_col, min_row, max_col, max_row = bounds
                
                # Get top-left value
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
    
    def get_table_data(self, table: TableRegion, 
                      include_headers: bool = True) -> List[List[Any]]:
        """
        Extract all data from a table region.
        Useful for debugging or data export.
        """
        data = []
        
        start_row = table.min_row if include_headers else table.min_row + 1
        
        # Preload the entire table region
        self._preload_region(
            start_row, table.max

# ==================== STRUCTURE TO METADATA ====================

class StructureMetadataBuilder:
    """Convert Excel structure to LLM-friendly metadata"""
    
    @staticmethod
    def build_metadata(worksheet_name: str, tables: List[TableRegion], 
                      merged_cells: List[Dict]) -> Dict:
        """Create metadata dictionary without actual sensitive values"""
        
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

# ==================== OPENAI INTEGRATION ====================

@dataclass
class ExtractionInstruction:
    """Structured extraction instruction from LLM"""
    table_id: int
    row: int
    col: int
    cell_address: str
    extraction_context: str

class OpenAIQueryProcessor:
    """Process natural language queries using OpenAI private endpoint"""
    
    def __init__(self, api_base_url: str, api_key: str, model: str = "gpt-4"):
        self.api_base_url = api_base_url.rstrip('/')
        self.api_key = api_key
        self.model = model
    
    def process_query(self, user_query: str, meta Dict) -> List[ExtractionInstruction]:
        """
        Send query + metadata to OpenAI, get back extraction instructions.
        Uses JSON mode for structured output.
        """
        
        system_prompt = """You are an Excel extraction assistant. Given spreadsheet structure metadata and a user query, return precise extraction instructions.

You will receive:
1. Metadata about table locations, headers, and structure (NO sensitive data)
2. A natural language query about what to extract

Return a JSON array of extraction instructions with this exact schema:
{
  "extractions": [
    {
      "table_id": <int>,
      "row": <int>,
      "col": <int>,
      "cell_address": "<string like 'B5'>",
      "extraction_context": "<explanation of what this cell represents>"
    }
  ]
}

Be precise with coordinates. Use 1-based indexing for rows and columns."""

        user_message = f"""Spreadsheet Structure:
{json.dumps(metadata, indent=2)}

User Query: {user_query}

Return extraction instructions as JSON."""

        # Call OpenAI API with JSON mode
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
            'response_format': {'type': 'json_object'},  # JSON mode
            'temperature': 0  # Deterministic for extraction tasks
        }
        
        try:
            response = requests.post(
                f'{self.api_base_url}/chat/completions',
                headers=headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            content = result['choices'][0]['message']['content']
            extraction_data = json.loads(content)
            
            # Parse into ExtractionInstruction objects
            instructions = []
            for item in extraction_data.get('extractions', []):
                instructions.append(ExtractionInstruction(
                    table_id=item['table_id'],
                    row=item['row'],
                    col=item['col'],
                    cell_address=item['cell_address'],
                    extraction_context=item['extraction_context']
                ))
            
            return instructions
            
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            raise
        except (KeyError, json.JSONDecodeError) as e:
            print(f"Failed to parse API response: {e}")
            raise

# ==================== PRECISE EXTRACTOR ====================

class PreciseExtractor:
    """Execute deterministic extraction based on LLM instructions"""
    
    def __init__(self, workbook_data, workbook_formula):
        self.wb_data = workbook_data
        self.wb_formula = workbook_formula
    
    def extract_values(self, worksheet_name: str, 
                      instructions: List[ExtractionInstruction]) -> Dict[str, Any]:
        """Extract exact values using coordinates from LLM"""
        
        ws_data = self.wb_data[worksheet_name]
        ws_formula = self.wb_formula[worksheet_name]
        
        results = {}
        
        for instruction in instructions:
            try:
                cell_data = ws_data.cell(instruction.row, instruction.col)
                cell_formula = ws_formula.cell(instruction.row, instruction.col)
                
                # Handle xlsm formula issue: data_only returns None if not cached
                value = cell_data.value
                
                # Fallback: if data_only returns None, try to get formula
                if value is None and cell_formula.value:
                    value = f"FORMULA: {cell_formula.value}"
                
                results[instruction.cell_address] = {
                    'value': value,
                    'context': instruction.extraction_context,
                    'coordinates': {
                        'row': instruction.row,
                        'col': instruction.col
                    },
                    'has_formula': cell_formula.value is not None and \
                                  isinstance(cell_formula.value, str) and \
                                  cell_formula.value.startswith('=')
                }
                
            except Exception as e:
                results[instruction.cell_address] = {
                    'error': str(e),
                    'context': instruction.extraction_context
                }
        
        return results

# ==================== MAIN ORCHESTRATOR ====================

class ExcelExtractionSystem:
    """Main system orchestrating all components"""
    
    def __init__(self, openai_base_url: str, openai_api_key: str, model: str = "gpt-4"):
        self.query_processor = OpenAIQueryProcessor(
            api_base_url=openai_base_url,
            api_key=openai_api_key,
            model=model
        )
    
    def process_file(self, file_path: str, worksheet_name: str, user_query: str) -> Dict:
        """
        Complete extraction pipeline:
        1. Load xlsm/xlsx with proper handling
        2. Detect table structure
        3. Build metadata (no sensitive data)
        4. Query OpenAI for extraction plan
        5. Execute precise extraction
        """
        
        print(f"[1/5] Loading workbook: {file_path}")
        wb_data, wb_formula = ExcelLoader.load_workbook(file_path)
        
        print(f"[2/5] Detecting tables in worksheet: {worksheet_name}")
        ws_data = wb_data[worksheet_name]
        ws_formula = wb_formula[worksheet_name]
        
        detector = TableDetector(ws_data, ws_formula)
        tables = detector.detect_all_tables()
        merged_cells = detector.get_merged_cell_info()
        
        print(f"    Found {len(tables)} tables")
        
        print("[3/5] Building structure metadata")
        metadata = StructureMetadataBuilder.build_metadata(
            worksheet_name, tables, merged_cells
        )
        
        print(f"[4/5] Querying OpenAI with: '{user_query}'")
        instructions = self.query_processor.process_query(user_query, metadata)
        
        print(f"    Received {len(instructions)} extraction instructions")
        
        print("[5/5] Executing precise extraction")
        extractor = PreciseExtractor(wb_data, wb_formula)
        results = extractor.extract_values(worksheet_name, instructions)
        
        return {
            'query': user_query,
            'metadata': metadata,
            'instructions': [asdict(i) for i in instructions],
            'extracted_values': results
        }

# ==================== USAGE EXAMPLE ====================

if __name__ == "__main__":
    
    # Initialize system with your private OpenAI endpoint
    system = ExcelExtractionSystem(
        openai_base_url="https://your-private-openai-endpoint.com/v1",
        openai_api_key="your-api-key",
        model="gpt-4"
    )
    
    # Process file with natural language query
    result = system.process_file(
        file_path="financial_data.xlsm",
        worksheet_name="Q4 Results",
        user_query="Extract the EBITDA value for Asia region in Q3 2024"
    )
    
    # Display results
    print("\n" + "="*60)
    print("EXTRACTION RESULTS")
    print("="*60)
    print(json.dumps(result['extracted_values'], indent=2))
