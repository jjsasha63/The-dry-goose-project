import openpyxl
from openpyxl.utils import get_column_letter, column_index_from_string
from openpyxl.utils.cell import coordinate_from_string, range_boundaries
import json
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
import requests
from collections import defaultdict

# ==================== XLSM FILE HANDLER ====================

class ExcelLoader:
    """Handles xlsm-specific issues like formula caching"""
    
    @staticmethod
    def load_workbook(file_path: str):
        """
        Load xlsm/xlsx with proper handling for formulas.
        xlsm files need data_only=True to read calculated values,
        but this returns None if Excel hasn't cached the values.
        """
        # Try data_only first for calculated values
        try:
            wb_data = openpyxl.load_workbook(
                file_path, 
                data_only=True, 
                keep_vba=True,  # Critical for xlsm files
                read_only=False
            )
            
            # Also load with formulas for structure analysis
            wb_formula = openpyxl.load_workbook(
                file_path,
                data_only=False,
                keep_vba=True,
                read_only=False
            )
            
            return wb_data, wb_formula
            
        except Exception as e:
            print(f"Error loading workbook: {e}")
            raise

# ==================== TABLE DETECTION ====================

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
    """Detects table boundaries and structure in Excel sheets"""
    
    def __init__(self, worksheet_data, worksheet_formula):
        self.ws_data = worksheet_data
        self.ws_formula = worksheet_formula
        self.merged_ranges = list(worksheet_data.merged_cells.ranges)
        
    def detect_all_tables(self) -> List[TableRegion]:
        """Find all table regions in the worksheet"""
        tables = []
        visited_cells = set()
        
        # Scan for table starting points
        for row in range(1, self.ws_data.max_row + 1):
            for col in range(1, self.ws_data.max_column + 1):
                coord = (row, col)
                
                if coord in visited_cells:
                    continue
                    
                cell = self.ws_data.cell(row, col)
                
                # Check if this looks like a table header
                if self._is_likely_header(cell, row, col):
                    table = self._extract_table_from_header(row, col)
                    if table:
                        tables.append(table)
                        # Mark cells as visited
                        for r in range(table.min_row, table.max_row + 1):
                            for c in range(table.min_col, table.max_col + 1):
                                visited_cells.add((r, c))
        
        return tables
    
    def _is_likely_header(self, cell, row: int, col: int) -> bool:
        """Heuristics to identify header cells"""
        if cell.value is None:
            return False
            
        # Check for bold formatting (common in headers)
        has_bold = cell.font and cell.font.bold
        
        # Check if cell has different background color
        has_fill = cell.fill and cell.fill.start_color and \
                   cell.fill.start_color.index != '00000000'
        
        # Check if it's text (not number)
        is_text = isinstance(cell.value, str)
        
        # Check if cells below/to the right have data
        has_data_below = self.ws_data.cell(row + 1, col).value is not None
        has_data_right = self.ws_data.cell(row, col + 1).value is not None
        
        return (has_bold or has_fill) and is_text and (has_data_below or has_data_right)
    
    def _extract_table_from_header(self, start_row: int, start_col: int) -> Optional[TableRegion]:
        """Extract complete table starting from header position"""
        
        # Determine orientation by checking data density
        horizontal_density = self._count_consecutive_cells(start_row, start_col, 'horizontal')
        vertical_density = self._count_consecutive_cells(start_row, start_col, 'vertical')
        
        if horizontal_density < 2 and vertical_density < 2:
            return None
            
        orientation = 'horizontal' if horizontal_density >= vertical_density else 'vertical'
        
        if orientation == 'horizontal':
            return self._extract_horizontal_table(start_row, start_col)
        else:
            return self._extract_vertical_table(start_row, start_col)
    
    def _count_consecutive_cells(self, row: int, col: int, direction: str) -> int:
        """Count consecutive non-empty cells"""
        count = 0
        
        if direction == 'horizontal':
            for c in range(col, min(col + 20, self.ws_data.max_column + 1)):
                if self.ws_data.cell(row, c).value is not None:
                    count += 1
                elif count > 0:
                    break
        else:  # vertical
            for r in range(row, min(row + 20, self.ws_data.max_row + 1)):
                if self.ws_data.cell(r, col).value is not None:
                    count += 1
                elif count > 0:
                    break
                    
        return count
    
    def _extract_horizontal_table(self, header_row: int, start_col: int) -> TableRegion:
        """Extract table with horizontal headers"""
        
        # Find rightmost column of headers
        max_col = start_col
        for col in range(start_col, self.ws_data.max_column + 1):
            if self.ws_data.cell(header_row, col).value is None:
                break
            max_col = col
        
        # Find bottom row of data
        max_row = header_row + 1
        for row in range(header_row + 1, self.ws_data.max_row + 1):
            # Check if row has any data
            has_data = any(
                self.ws_data.cell(row, col).value is not None 
                for col in range(start_col, max_col + 1)
            )
            if not has_
                break
            max_row = row
        
        # Extract headers
        headers = []
        for col in range(start_col, max_col + 1):
            header_val = self.ws_data.cell(header_row, col).value
            headers.append(str(header_val) if header_val else f"Column_{col}")
        
        return TableRegion(
            min_row=header_row,
            max_row=max_row,
            min_col=start_col,
            max_col=max_col,
            orientation='horizontal',
            headers=headers,
            table_type='standard'
        )
    
    def _extract_vertical_table(self, start_row: int, header_col: int) -> TableRegion:
        """Extract table with vertical headers"""
        
        # Find bottom row of headers
        max_row = start_row
        for row in range(start_row, self.ws_data.max_row + 1):
            if self.ws_data.cell(row, header_col).value is None:
                break
            max_row = row
        
        # Find rightmost column of data
        max_col = header_col + 1
        for col in range(header_col + 1, self.ws_data.max_column + 1):
            has_data = any(
                self.ws_data.cell(row, col).value is not None 
                for row in range(start_row, max_row + 1)
            )
            if not has_
                break
            max_col = col
        
        # Extract headers
        headers = []
        for row in range(start_row, max_row + 1):
            header_val = self.ws_data.cell(row, header_col).value
            headers.append(str(header_val) if header_val else f"Row_{row}")
        
        return TableRegion(
            min_row=start_row,
            max_row=max_row,
            min_col=header_col,
            max_col=max_col,
            orientation='vertical',
            headers=headers,
            table_type='standard'
        )
    
    def get_merged_cell_info(self) -> List[Dict]:
        """Get all merged cell ranges"""
        merged_info = []
        for merged_range in self.merged_ranges:
            min_col, min_row, max_col, max_row = range_boundaries(str(merged_range))
            top_left_value = self.ws_data.cell(min_row, min_col).value
            
            merged_info.append({
                'range': str(merged_range),
                'min_row': min_row,
                'max_row': max_row,
                'min_col': min_col,
                'max_col': max_col,
                'value': top_left_value
            })
        
        return merged_info

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
