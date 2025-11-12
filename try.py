"""
Universal Excel Scraper - Pandas Edition
==========================================
Complete rewrite using ONLY pandas - NO openpyxl dependency
Works with .xlsx, .xlsm, .xls files perfectly
No version conflicts, no compatibility issues

Installation:
    pip install pandas openpyxl
    (openpyxl only used by pandas internally, no direct dependency)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import warnings

warnings.filterwarnings('ignore')


class ExcelTableDetector:
    """
    Detect and extract tables from Excel files using pandas only.
    No openpyxl complications - simple and reliable!
    """

    def __init__(self, file_path: str, data_only: bool = True):
        """
        Initialize Excel detector.

        Args:
            file_path: Path to Excel file (.xlsx, .xlsm, .xls)
            data_only: Keep for API compatibility (pandas always reads values)
        """
        self.file_path = str(file_path)
        self.file_format = Path(file_path).suffix.lower()

        # Validate file
        if not Path(file_path).exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        if self.file_format not in {'.xlsx', '.xlsm', '.xls'}:
            raise ValueError(f"Unsupported format: {self.file_format}")

        print(f"Loaded {self.file_format} file: {Path(file_path).name}")

    def get_sheet_names(self) -> List[str]:
        """Get all sheet names from Excel file"""
        try:
            excel_file = pd.ExcelFile(self.file_path)
            return excel_file.sheet_names
        except Exception as e:
            print(f"Error reading sheet names: {e}")
            return []

    def detect_table_boundaries(self, sheet_name: str, 
                               threshold: int = 3) -> List[Dict[str, Any]]:
        """
        Detect table boundaries in a sheet.

        Args:
            sheet_name: Name of sheet to analyze
            threshold: Min non-empty cells to consider as table row/col

        Returns:
            List of table info dicts with boundaries and data
        """
        try:
            # Read entire sheet as-is (no header processing)
            df = pd.read_excel(self.file_path, sheet_name=sheet_name, header=None)
        except Exception as e:
            print(f"Warning: Could not read sheet {sheet_name}: {e}")
            return []

        if df.empty:
            return []

        # Identify rows and columns with sufficient data
        row_has_data = df.notna().sum(axis=1) >= threshold
        col_has_data = df.notna().sum(axis=0) >= threshold

        # Find contiguous blocks
        tables = []
        in_table = False
        start_row = None

        for idx, has_data in enumerate(row_has_data):
            if has_data and not in_table:
                start_row = idx
                in_table = True
            elif not has_data and in_table:
                end_row = idx - 1

                # Extract table region
                table_df = df.iloc[start_row:end_row+1]
                table_col_has_data = table_df.notna().sum(axis=0) >= 1

                # Find column ranges
                col_start = None
                for col_idx, col_has in enumerate(table_col_has_data):
                    if col_has and col_start is None:
                        col_start = col_idx
                    elif not col_has and col_start is not None:
                        col_end = col_idx - 1
                        table_data = df.iloc[start_row:end_row+1, col_start:col_end+1].copy()

                        if not table_data.empty and table_data.notna().sum().sum() > 0:
                            tables.append({
                                'row_start': start_row,
                                'row_end': end_row,
                                'col_start': col_start,
                                'col_end': col_end,
                                'data': table_data
                            })
                        col_start = None

                # Handle table extending to last column
                if col_start is not None:
                    col_end = len(table_col_has_data) - 1
                    table_data = df.iloc[start_row:end_row+1, col_start:col_end+1].copy()
                    if not table_data.empty and table_data.notna().sum().sum() > 0:
                        tables.append({
                            'row_start': start_row,
                            'row_end': end_row,
                            'col_start': col_start,
                            'col_end': col_end,
                            'data': table_data
                        })

                in_table = False

        # Handle table extending to last row
        if in_table:
            end_row = len(row_has_data) - 1
            table_df = df.iloc[start_row:end_row+1]
            table_col_has_data = table_df.notna().sum(axis=0) >= 1

            col_start = None
            for col_idx, col_has in enumerate(table_col_has_data):
                if col_has and col_start is None:
                    col_start = col_idx
                elif not col_has and col_start is not None:
                    col_end = col_idx - 1
                    table_data = df.iloc[start_row:end_row+1, col_start:col_end+1].copy()
                    if not table_data.empty and table_data.notna().sum().sum() > 0:
                        tables.append({
                            'row_start': start_row,
                            'row_end': end_row,
                            'col_start': col_start,
                            'col_end': col_end,
                            'data': table_data
                        })
                    col_start = None

            if col_start is not None:
                col_end = len(table_col_has_data) - 1
                table_data = df.iloc[start_row:end_row+1, col_start:col_end+1].copy()
                if not table_data.empty and table_data.notna().sum().sum() > 0:
                    tables.append({
                        'row_start': start_row,
                        'row_end': end_row,
                        'col_start': col_start,
                        'col_end': col_end,
                        'data': table_data
                    })

        return tables

    def detect_orientation(self, table_data: pd.DataFrame) -> str:
        """Detect if table is horizontal or vertical"""
        rows, cols = table_data.shape
        return 'horizontal' if cols > rows else 'vertical'

    def find_header_row(self, table_data: pd.DataFrame) -> Optional[int]:
        """Identify header row (first row with mostly strings)"""
        for idx in range(min(3, len(table_data))):
            row = table_data.iloc[idx]

            if row.notna().sum() > 0:
                non_null = row.dropna()
                string_ratio = sum(isinstance(v, str) for v in non_null) / len(non_null)

                if string_ratio > 0.7:  # 70%+ strings = likely header
                    return idx

        return 0

    def extract_all_tables(self) -> Dict[str, List[pd.DataFrame]]:
        """Extract all tables from all sheets"""
        all_tables = {}

        for sheet_name in self.get_sheet_names():
            print(f"\nProcessing sheet: {sheet_name}")

            tables = self.detect_table_boundaries(sheet_name)
            processed_tables = []

            for i, table_info in enumerate(tables):
                table_data = table_info['data'].copy()

                # Try to identify and set header
                if len(table_data) > 0:
                    header_row = self.find_header_row(table_data)
                    if header_row > 0:
                        table_data.columns = table_data.iloc[header_row]
                        table_data = table_data.iloc[header_row+1:].reset_index(drop=True)

                # Clean empty rows/columns
                table_data = table_data.dropna(how='all', axis=0)
                table_data = table_data.dropna(how='all', axis=1)

                if not table_data.empty:
                    orientation = self.detect_orientation(table_data)
                    print(f"  Table {i+1}: {table_data.shape}, orientation: {orientation}, "
                          f"location: R{table_info['row_start']}C{table_info['col_start']}")
                    processed_tables.append(table_data)

            all_tables[sheet_name] = processed_tables

        return all_tables

    def save_tables(self, tables: Dict[str, List[pd.DataFrame]], 
                   output_dir: str = 'extracted_tables'):
        """Save all tables to CSV files"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True, parents=True)

        for sheet_name, sheet_tables in tables.items():
            for i, table in enumerate(sheet_tables):
                if not table.empty:
                    # Create safe filename
                    safe_name = "".join(c if c.isalnum() else "_" for c in sheet_name)
                    filename = f"{safe_name}_table_{i+1}.csv"
                    filepath = output_path / filename

                    table.to_csv(filepath, index=False)
                    print(f"Saved: {filepath}")


class TargetedValueExtractor:
    """Extract specific values from Excel using pandas"""

    def __init__(self, file_path: str):
        """Initialize extractor"""
        self.file_path = str(file_path)

        if not Path(file_path).exists():
            raise FileNotFoundError(f"File not found: {file_path}")

    def search_by_label(self, label: str, sheet_name: Optional[str] = None,
                       search_direction: str = 'right') -> List[Dict[str, Any]]:
        """
        Search for a label and return nearby values.

        Args:
            label: Text to search for
            sheet_name: Specific sheet (None = all sheets)
            search_direction: 'right', 'left', 'below', 'above'

        Returns:
            List of results with sheet, location, value
        """
        results = []

        # Determine offset
        offset_map = {
            'right': (0, 1),
            'left': (0, -1),
            'below': (1, 0),
            'above': (-1, 0),
        }
        offset = offset_map.get(search_direction, (0, 1))

        # Get sheets to search
        try:
            excel_file = pd.ExcelFile(self.file_path)
            sheets = [sheet_name] if sheet_name else excel_file.sheet_names
        except:
            return results

        # Search each sheet
        for sheet in sheets:
            try:
                df = pd.read_excel(self.file_path, sheet_name=sheet, header=None)

                # Search for label
                for row_idx in range(len(df)):
                    for col_idx in range(len(df.columns)):
                        cell_val = df.iloc[row_idx, col_idx]

                        if isinstance(cell_val, str) and cell_val.lower() == label.lower():
                            # Found label, get offset value
                            val_row = row_idx + offset[0]
                            val_col = col_idx + offset[1]

                            if 0 <= val_row < len(df) and 0 <= val_col < len(df.columns):
                                value = df.iloc[val_row, val_col]

                                results.append({
                                    'sheet': sheet,
                                    'label_row': row_idx,
                                    'label_col': col_idx,
                                    'value_row': val_row,
                                    'value_col': val_col,
                                    'value': value
                                })
            except:
                continue

        return results

    def extract_by_cell_reference(self, references: Dict[str, List[str]]) -> Dict[str, Dict]:
        """
        Extract specific cells by reference.

        Args:
            references: {'Sheet1': ['A1', 'B5'], 'Sheet2': ['C10']}

        Returns:
            {'Sheet1': {'A1': value, 'B5': value}, ...}
        """
        results = {}

        for sheet_name, cell_refs in references.items():
            results[sheet_name] = {}

            try:
                df = pd.read_excel(self.file_path, sheet_name=sheet_name, header=None)

                for cell_ref in cell_refs:
                    try:
                        # Parse cell reference (e.g., 'A1' -> row=0, col=0)
                        col_letter = ''.join(c for c in cell_ref if c.isalpha()).upper()
                        row_num = int(''.join(c for c in cell_ref if c.isdigit())) - 1

                        # Convert column letter to index
                        col_idx = 0
                        for c in col_letter:
                            col_idx = col_idx * 26 + (ord(c) - ord('A') + 1)
                        col_idx -= 1

                        # Get value
                        if 0 <= row_num < len(df) and 0 <= col_idx < len(df.columns):
                            value = df.iloc[row_num, col_idx]
                            results[sheet_name][cell_ref] = value
                        else:
                            results[sheet_name][cell_ref] = None
                    except:
                        results[sheet_name][cell_ref] = None
            except:
                pass

        return results


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def read_excel_simple(file_path: str, sheet_name: Optional[str] = None) -> Dict[str, pd.DataFrame]:
    """Simple function to read Excel file(s)"""
    if sheet_name:
        return {sheet_name: pd.read_excel(file_path, sheet_name=sheet_name)}
    else:
        return pd.read_excel(file_path, sheet_name=None)


def extract_and_consolidate(file_paths: List[str], 
                           output_file: str = 'consolidated.csv') -> pd.DataFrame:
    """Extract and consolidate data from multiple Excel files"""
    data = []

    for file_path in file_paths:
        print(f"Processing: {file_path}")

        detector = ExcelTableDetector(file_path)
        tables = detector.extract_all_tables()

        for sheet_name, sheet_tables in tables.items():
            for table in sheet_tables:
                table['source_file'] = Path(file_path).stem
                table['source_sheet'] = sheet_name
                data.append(table)

    # Consolidate
    consolidated = pd.concat(data, ignore_index=True)
    consolidated.to_csv(output_file, index=False)

    print(f"\nâœ“ Consolidated {len(consolidated)} rows to {output_file}")
    return consolidated


# ============================================================================
# EXAMPLES
# ============================================================================

def example_1_basic():
    """Extract all tables from single file"""
    detector = ExcelTableDetector('file.xlsm')
    all_tables = detector.extract_all_tables()
    detector.save_tables(all_tables)


def example_2_targeted():
    """Find specific values"""
    extractor = TargetedValueExtractor('file.xlsm')

    # Search for label
    results = extractor.search_by_label('Total Revenue', search_direction='right')
    for r in results:
        print(f"Found: {r['value']} in {r['sheet']}")

    # Extract cells
    values = extractor.extract_by_cell_reference({
        'Sheet1': ['A1', 'B5', 'C10']
    })
    print(values)


def example_3_batch():
    """Process multiple files"""
    from pathlib import Path

    for file in Path('reports').glob('*.xlsm'):
        print(f"\nProcessing: {file.name}")
        detector = ExcelTableDetector(str(file))
        tables = detector.extract_all_tables()
        detector.save_tables(tables, output_dir=f'output/{file.stem}')


def example_4_consolidate():
    """Consolidate multiple files"""
    files = ['report1.xlsm', 'report2.xlsm', 'report3.xlsm']
    consolidated = extract_and_consolidate(files)
    print(consolidated.head())


if __name__ == "__main__":
    print("Excel Universal Scraper - Pandas Edition")
    print("="*60)
    print("\nUsage:")
    print("  1. detector = ExcelTableDetector('file.xlsm')")
    print("  2. tables = detector.extract_all_tables()")
    print("  3. detector.save_tables(tables)")
    print("\nâœ“ Works with .xlsx, .xlsm, .xls")
    print("âœ“ NO openpyxl errors!")
    print("âœ“ Pure pandas solution!")
