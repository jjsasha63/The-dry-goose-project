"""
Universal Excel Scraper (Enhanced for .xlsm support)
=====================================================
A robust Python script for extracting data from non-uniform Excel files (.xlsx, .xlsm) with:
- Multiple sheets
- Formulas (Excel macros supported)
- Tables in various orientations (horizontal/vertical)
- Multiple datasets per sheet
- Tables located anywhere in the sheet

Requirements:
pip install pandas openpyxl numpy pyxlsb

Author: Data Engineering Solution
Date: 2025-11-12
Version: 2.0 (Enhanced with .xlsm support)
"""

import pandas as pd
import openpyxl
from openpyxl import load_workbook
import numpy as np
from typing import List, Tuple, Dict, Any, Optional, Union
import json
from pathlib import Path
import warnings


class ExcelFileHandler:
    """
    Utility class to handle different Excel file formats (.xlsx, .xlsm, .xls)
    """

    SUPPORTED_FORMATS = {'.xlsx', '.xlsm', '.xls'}

    @staticmethod
    def validate_file(file_path: str) -> bool:
        """
        Validate if file exists and has supported extension.

        Args:
            file_path: Path to Excel file

        Returns:
            True if valid, False otherwise
        """
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        if path.suffix.lower() not in ExcelFileHandler.SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format: {path.suffix}. "
                f"Supported formats: {ExcelFileHandler.SUPPORTED_FORMATS}"
            )

        return True

    @staticmethod
    def get_file_format(file_path: str) -> str:
        """Get the file format extension."""
        return Path(file_path).suffix.lower()

    @staticmethod
    def load_workbook_safe(file_path: str, data_only: bool = True) -> openpyxl.Workbook:
        """
        Load workbook handling .xlsm files specially.

        Args:
            file_path: Path to Excel file
            data_only: If True, load values; if False, load formulas

        Returns:
            openpyxl Workbook object
        """
        file_format = ExcelFileHandler.get_file_format(file_path)

        try:
            if file_format == '.xlsm':
                # For .xlsm files, explicitly set keep_vba to False to avoid macro issues
                # Macros are not executed, but data is preserved
                wb = load_workbook(file_path, data_only=data_only, keep_vba=False)
            else:
                wb = load_workbook(file_path, data_only=data_only)

            return wb

        except PermissionError:
            raise PermissionError(f"Permission denied: Cannot open {file_path}. File may be locked.")
        except Exception as e:
            raise RuntimeError(f"Error loading workbook: {str(e)}")


class ExcelTableDetector:
    """Detects and extracts tables from Excel sheets using multiple strategies."""

    def __init__(self, file_path: str, data_only: bool = True):
        """
        Initialize the Excel scraper.

        Args:
            file_path: Path to the Excel file (.xlsx, .xlsm, .xls)
            data_only: If True, read formula results; if False, read formulas
        """
        # Validate file
        ExcelFileHandler.validate_file(file_path)

        self.file_path = file_path
        self.file_format = ExcelFileHandler.get_file_format(file_path)
        self.data_only = data_only

        # Load workbook using safe handler
        self.workbook = ExcelFileHandler.load_workbook_safe(file_path, data_only=data_only)

        # Print file info
        print(f"Loaded {self.file_format} file: {Path(file_path).name}")

    def get_sheet_names(self) -> List[str]:
        """Get all sheet names in the workbook."""
        return self.workbook.sheetnames

    def detect_table_boundaries(self, sheet_name: str, 
                               threshold: int = 3) -> List[Dict[str, Any]]:
        """
        Detect multiple tables in a sheet based on non-empty cell patterns.

        Args:
            sheet_name: Name of the sheet to analyze
            threshold: Minimum number of consecutive non-empty cells to consider a row/col as part of table

        Returns:
            List of dictionaries containing table boundaries and metadata
        """
        # Load sheet with pandas to get all data
        try:
            df = pd.read_excel(self.file_path, sheet_name=sheet_name, header=None)
        except Exception as e:
            print(f"Warning: Could not read sheet {sheet_name}: {str(e)}")
            return []

        # Identify rows and columns with data
        row_has_data = df.notna().sum(axis=1) >= threshold
        col_has_data = df.notna().sum(axis=0) >= threshold

        # Find contiguous blocks of rows with data
        tables = []
        in_table = False
        start_row = None

        for idx, has_data in enumerate(row_has_data):
            if has_data and not in_table:
                start_row = idx
                in_table = True
            elif not has_data and in_table:
                # Found end of table
                end_row = idx - 1

                # Find column boundaries for this table
                table_df = df.iloc[start_row:end_row+1]
                table_col_has_data = table_df.notna().sum(axis=0) >= 1

                # Find contiguous column ranges
                col_start = None
                for col_idx, col_data in enumerate(table_col_has_data):
                    if col_data and col_start is None:
                        col_start = col_idx
                    elif not col_data and col_start is not None:
                        col_end = col_idx - 1

                        # Extract this table
                        table_data = df.iloc[start_row:end_row+1, col_start:col_end+1]

                        if not table_data.empty and table_data.notna().sum().sum() > 0:
                            tables.append({
                                'row_start': start_row,
                                'row_end': end_row,
                                'col_start': col_start,
                                'col_end': col_end,
                                'data': table_data
                            })
                        col_start = None

                # Handle case where table extends to last column
                if col_start is not None:
                    col_end = len(table_col_has_data) - 1
                    table_data = df.iloc[start_row:end_row+1, col_start:col_end+1]
                    if not table_data.empty and table_data.notna().sum().sum() > 0:
                        tables.append({
                            'row_start': start_row,
                            'row_end': end_row,
                            'col_start': col_start,
                            'col_end': col_end,
                            'data': table_data
                        })

                in_table = False

        # Handle case where table extends to last row
        if in_table:
            end_row = len(row_has_data) - 1
            table_df = df.iloc[start_row:end_row+1]
            table_col_has_data = table_df.notna().sum(axis=0) >= 1

            col_start = None
            for col_idx, col_data in enumerate(table_col_has_data):
                if col_data and col_start is None:
                    col_start = col_idx
                elif not col_data and col_start is not None:
                    col_end = col_idx - 1
                    table_data = df.iloc[start_row:end_row+1, col_start:col_end+1]
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
                table_data = df.iloc[start_row:end_row+1, col_start:col_end+1]
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
        """
        Detect if table is horizontal or vertical based on data patterns.

        Args:
            table_data: DataFrame containing the table

        Returns:
            'horizontal' or 'vertical'
        """
        rows, cols = table_data.shape

        # Simple heuristic: if more columns than rows, likely horizontal
        if cols > rows:
            return 'horizontal'
        else:
            return 'vertical'

    def find_header_row(self, table_data: pd.DataFrame) -> Optional[int]:
        """
        Attempt to identify the header row based on data patterns.

        Args:
            table_data: DataFrame containing the table

        Returns:
            Index of likely header row, or None
        """
        # Check first few rows for consistent data types
        for idx in range(min(3, len(table_data))):
            row = table_data.iloc[idx]

            # Header typically has all string values
            if row.notna().sum() > 0:
                non_null_values = row.dropna()
                if len(non_null_values) > 0:
                    # If row has mostly strings and next row has numbers/different types
                    string_ratio = sum(isinstance(v, str) for v in non_null_values) / len(non_null_values)
                    if string_ratio > 0.7:  # 70% strings
                        return idx

        return 0  # Default to first row

    def extract_value_by_pattern(self, sheet_name: str, 
                                 cell_value: str = None,
                                 nearby_offset: Tuple[int, int] = (0, 1)) -> Any:
        """
        Extract a specific value based on finding a pattern/label.

        Args:
            sheet_name: Name of the sheet
            cell_value: Label to search for
            nearby_offset: (row_offset, col_offset) from found cell

        Returns:
            Value at the offset position
        """
        ws = self.workbook[sheet_name]

        # Search for the cell with matching value
        for row in ws.iter_rows():
            for cell in row:
                if cell.value and str(cell.value).strip() == cell_value:
                    # Found the label, get nearby value
                    target_row = cell.row + nearby_offset[0]
                    target_col = cell.column + nearby_offset[1]
                    return ws.cell(row=target_row, column=target_col).value

        return None

    def get_formulas(self, sheet_name: str) -> Dict[str, str]:
        """
        Get all formulas from a sheet (requires data_only=False).

        Note: For .xlsm files, macros are not executed, but data is preserved.

        Args:
            sheet_name: Name of the sheet

        Returns:
            Dictionary mapping cell addresses to formulas
        """
        if self.data_only:
            print("Warning: Workbook was loaded with data_only=True. Reload with data_only=False to see formulas.")
            return {}

        ws = self.workbook[sheet_name]
        formulas = {}

        for row in ws.iter_rows():
            for cell in row:
                if cell.value and isinstance(cell.value, str) and cell.value.startswith('='):
                    formulas[cell.coordinate] = cell.value

        return formulas

    def extract_all_tables(self, output_format: str = 'dict') -> Dict[str, List[pd.DataFrame]]:
        """
        Extract all tables from all sheets.

        Args:
            output_format: 'dict', 'csv', or 'json'

        Returns:
            Dictionary mapping sheet names to list of DataFrames
        """
        all_tables = {}

        for sheet_name in self.get_sheet_names():
            print(f"\nProcessing sheet: {sheet_name}")
            tables = self.detect_table_boundaries(sheet_name)

            processed_tables = []
            for i, table_info in enumerate(tables):
                table_data = table_info['data'].copy()

                # Try to identify header
                header_row = self.find_header_row(table_data)
                if header_row is not None and header_row > 0:
                    # Use identified row as header
                    table_data.columns = table_data.iloc[header_row]
                    table_data = table_data.iloc[header_row+1:].reset_index(drop=True)

                # Clean up: remove rows/columns that are all NaN
                table_data = table_data.dropna(how='all', axis=0).dropna(how='all', axis=1)

                if not table_data.empty:
                    orientation = self.detect_orientation(table_data)
                    print(f"  Table {i+1}: {table_data.shape}, orientation: {orientation}, "
                          f"location: R{table_info['row_start']}C{table_info['col_start']}")
                    processed_tables.append(table_data)

            all_tables[sheet_name] = processed_tables

        return all_tables

    def save_tables(self, tables: Dict[str, List[pd.DataFrame]], 
                   output_dir: str = 'extracted_tables'):
        """
        Save extracted tables to files.

        Args:
            tables: Dictionary of tables from extract_all_tables()
            output_dir: Directory to save tables
        """
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        for sheet_name, sheet_tables in tables.items():
            for i, table in enumerate(sheet_tables):
                # Create safe filename
                safe_sheet_name = "".join(c if c.isalnum() else "_" for c in sheet_name)
                filename = f"{safe_sheet_name}_table_{i+1}.csv"
                filepath = output_path / filename

                table.to_csv(filepath, index=False)
                print(f"Saved: {filepath}")


class TargetedValueExtractor:
    """Extract specific values from Excel based on labels or patterns."""

    def __init__(self, file_path: str):
        # Validate file
        ExcelFileHandler.validate_file(file_path)

        self.file_path = file_path
        self.file_format = ExcelFileHandler.get_file_format(file_path)
        self.workbook = ExcelFileHandler.load_workbook_safe(file_path, data_only=True)

    def search_by_label(self, label: str, sheet_name: Optional[str] = None,
                       search_direction: str = 'right') -> List[Dict[str, Any]]:
        """
        Search for a label and return nearby values.

        Args:
            label: Text to search for
            sheet_name: Specific sheet to search (None = all sheets)
            search_direction: 'right', 'left', 'below', 'above'

        Returns:
            List of dictionaries with sheet, location, and value
        """
        results = []
        sheets = [sheet_name] if sheet_name else self.workbook.sheetnames

        direction_offset = {
            'right': (0, 1),
            'left': (0, -1),
            'below': (1, 0),
            'above': (-1, 0)
        }

        offset = direction_offset.get(search_direction, (0, 1))

        for sheet in sheets:
            try:
                ws = self.workbook[sheet]
                for row in ws.iter_rows():
                    for cell in row:
                        if cell.value and str(cell.value).strip().lower() == label.lower():
                            target_row = cell.row + offset[0]
                            target_col = cell.column + offset[1]

                            if target_row > 0 and target_col > 0:
                                value = ws.cell(row=target_row, column=target_col).value
                                results.append({
                                    'sheet': sheet,
                                    'label_location': cell.coordinate,
                                    'value_location': ws.cell(row=target_row, column=target_col).coordinate,
                                    'value': value
                                })
            except Exception as e:
                print(f"Warning: Error processing sheet {sheet}: {str(e)}")

        return results

    def extract_by_cell_reference(self, references: Dict[str, List[str]]) -> Dict[str, Any]:
        """
        Extract specific cells by reference.

        Args:
            references: Dict mapping sheet names to list of cell references

        Returns:
            Dictionary of extracted values
        """
        results = {}

        for sheet_name, cells in references.items():
            try:
                ws = self.workbook[sheet_name]
                sheet_results = {}

                for cell_ref in cells:
                    try:
                        value = ws[cell_ref].value
                        sheet_results[cell_ref] = value
                    except:
                        sheet_results[cell_ref] = None

                results[sheet_name] = sheet_results
            except Exception as e:
                print(f"Warning: Could not access sheet {sheet_name}: {str(e)}")
                results[sheet_name] = {}

        return results


# Example usage functions
def example_basic_usage():
    """Example: Extract all tables from an Excel file (.xlsx or .xlsm)."""

    # Initialize detector (works with .xlsx, .xlsm, .xls)
    detector = ExcelTableDetector('your_file.xlsm', data_only=True)

    # Extract all tables
    all_tables = detector.extract_all_tables()

    # Save to CSV files
    detector.save_tables(all_tables, output_dir='extracted_tables')

    # Access specific table
    if 'Sheet1' in all_tables and len(all_tables['Sheet1']) > 0:
        first_table = all_tables['Sheet1'][0]
        print(first_table.head())


def example_xlsm_with_macros():
    """Example: Extract data from .xlsm file with macros."""

    print("\nNote: Macros are not executed, but calculated values are extracted.")
    print("Macros are preserved in the file but not run during data extraction.\n")

    detector = ExcelTableDetector('macro_enabled_file.xlsm', data_only=True)

    # Extract calculated values (macro results if they were run in Excel)
    all_tables = detector.extract_all_tables()

    # Or get the formulas
    formulas = detector.get_formulas('Sheet1')
    print(f"Found {len(formulas)} formulas in Sheet1")


def example_targeted_extraction():
    """Example: Extract specific values by label from .xlsx or .xlsm."""

    extractor = TargetedValueExtractor('your_file.xlsm')

    # Find value to the right of "Total Sales:"
    results = extractor.search_by_label('Total Sales:', search_direction='right')
    for result in results:
        print(f"Found in {result['sheet']} at {result['value_location']}: {result['value']}")

    # Extract specific cells
    cell_values = extractor.extract_by_cell_reference({
        'Sheet1': ['A1', 'B5', 'C10'],
        'Sheet2': ['D3', 'E7']
    })
    print(json.dumps(cell_values, indent=2))


def example_batch_processing():
    """Example: Process multiple Excel files (.xlsx and .xlsm mixed)."""

    from pathlib import Path

    input_folder = Path('excel_files')
    output_folder = Path('extracted_data')

    # Process all Excel files regardless of format
    for excel_file in input_folder.glob('*'):
        if excel_file.suffix.lower() in {'.xlsx', '.xlsm', '.xls'}:
            try:
                print(f"\nProcessing: {excel_file.name}")
                detector = ExcelTableDetector(str(excel_file))
                all_tables = detector.extract_all_tables()

                # Create subfolder for this file
                file_output = output_folder / excel_file.stem
                detector.save_tables(all_tables, output_dir=str(file_output))
            except Exception as e:
                print(f"Error processing {excel_file.name}: {str(e)}")


if __name__ == "__main__":
    print("Excel Universal Scraper (Enhanced)\n" + "="*50)
    print("Supported formats: .xlsx, .xlsm, .xls")
    print("\nTo use this script:")
    print("1. Install requirements: pip install pandas openpyxl numpy")
    print("2. Replace 'your_file.xlsm' with your actual file path")
    print("3. Run the appropriate example function")
    print("\nAvailable functions:")
    print("  - example_basic_usage(): Extract all tables")
    print("  - example_xlsm_with_macros(): Handle .xlsm files")
    print("  - example_targeted_extraction(): Find specific values")
    print("  - example_batch_processing(): Process multiple files")

    # Uncomment to run:
    # example_basic_usage()
    # example_xlsm_with_macros()
    # example_targeted_extraction()
    # example_batch_processing()
