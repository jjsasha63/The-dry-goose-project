"""
Universal Excel Scraper
========================
A robust Python script for extracting data from non-uniform Excel files with:
- Multiple sheets
- Formulas
- Tables in various orientations (horizontal/vertical)
- Multiple datasets per sheet
- Tables located anywhere in the sheet

Requirements:
pip install pandas openpyxl numpy

Author: Data Engineering Solution
Date: 2025-11-12
"""

import pandas as pd
import openpyxl
from openpyxl import load_workbook
import numpy as np
from typing import List, Tuple, Dict, Any, Optional
import json
from pathlib import Path


class ExcelTableDetector:
    """Detects and extracts tables from Excel sheets using multiple strategies."""

    def __init__(self, file_path: str, data_only: bool = True):
        """
        Initialize the Excel scraper.

        Args:
            file_path: Path to the Excel file
            data_only: If True, read formula results; if False, read formulas
        """
        self.file_path = file_path
        self.data_only = data_only
        self.workbook = load_workbook(file_path, data_only=data_only)

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
        df = pd.read_excel(self.file_path, sheet_name=sheet_name, header=None)

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
        self.file_path = file_path
        self.workbook = load_workbook(file_path, data_only=True)

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
            ws = self.workbook[sheet_name]
            sheet_results = {}

            for cell_ref in cells:
                try:
                    value = ws[cell_ref].value
                    sheet_results[cell_ref] = value
                except:
                    sheet_results[cell_ref] = None

            results[sheet_name] = sheet_results

        return results


# Example usage functions
def example_basic_usage():
    """Example: Extract all tables from an Excel file."""

    # Initialize detector
    detector = ExcelTableDetector('your_file.xlsx', data_only=True)

    # Extract all tables
    all_tables = detector.extract_all_tables()

    # Save to CSV files
    detector.save_tables(all_tables, output_dir='extracted_tables')

    # Access specific table
    if 'Sheet1' in all_tables and len(all_tables['Sheet1']) > 0:
        first_table = all_tables['Sheet1'][0]
        print(first_table.head())


def example_targeted_extraction():
    """Example: Extract specific values by label."""

    extractor = TargetedValueExtractor('your_file.xlsx')

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


def example_formula_handling():
    """Example: Extract formulas from Excel."""

    # Load with data_only=False to see formulas
    detector = ExcelTableDetector('your_file.xlsx', data_only=False)

    # Get all formulas from a sheet
    formulas = detector.get_formulas('Sheet1')
    for cell, formula in formulas.items():
        print(f"{cell}: {formula}")


if __name__ == "__main__":
    # Basic usage example
    print("Excel Universal Scraper\n" + "="*50)
    print("\nTo use this script:")
    print("1. Install requirements: pip install pandas openpyxl numpy")
    print("2. Replace 'your_file.xlsx' with your actual file path")
    print("3. Run the appropriate example function")
    print("\nAvailable functions:")
    print("  - example_basic_usage(): Extract all tables")
    print("  - example_targeted_extraction(): Find specific values")
    print("  - example_formula_handling(): Extract formulas")

    # Uncomment to run:
    # example_basic_usage()
    # example_targeted_extraction()
    # example_formula_handling()
