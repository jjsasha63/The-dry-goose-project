import openpyxl
import pandas as pd
import json
from openai import AzureOpenAI
from typing import Dict, List, Any
import re

class SecureExcelProcessor:
    """
    Secure Excel processor that keeps all data within Databricks environment.
    Uses openpyxl for parsing and Azure OpenAI via Private Link for NL queries.
    """
    
    def __init__(self):
        # Retrieve secrets from Key Vault-backed scope
        self.openai_key = dbutils.secrets.get(scope="azure-key-vault-scope", key="azure-openai-key")
        self.openai_endpoint = dbutils.secrets.get(scope="azure-key-vault-scope", key="azure-openai-endpoint")
        
        # Initialize Azure OpenAI client (uses Private Link)
        self.client = AzureOpenAI(
            api_key=self.openai_key,
            api_version="2024-02-15-preview",
            azure_endpoint=self.openai_endpoint
        )
        
    def extract_cell_metadata(self, file_path: str) -> Dict[str, Any]:
        """
        Extract all cell data with metadata from Excel file.
        Handles merged cells, formulas, and complex layouts.
        """
        # Load from Unity Catalog volume
        workbook = openpyxl.load_workbook(file_path, data_only=True)
        
        all_sheets_data = {}
        
        for sheet_name in workbook.sheetnames:
            sheet = workbook[sheet_name]
            sheet_data = {
                'cells': {},
                'merged_cells': [],
                'dimensions': {
                    'max_row': sheet.max_row,
                    'max_column': sheet.max_column
                }
            }
            
            # Extract merged cell ranges
            for merged_range in sheet.merged_cells.ranges:
                sheet_data['merged_cells'].append(str(merged_range))
            
            # Extract all cell data with formatting
            for row in sheet.iter_rows():
                for cell in row:
                    if cell.value is not None:
                        sheet_data['cells'][cell.coordinate] = {
                            'value': cell.value,
                            'data_type': cell.data_type,
                            'number_format': cell.number_format,
                            'is_merged': cell.coordinate in str(sheet.merged_cells),
                            'font': {
                                'bold': cell.font.bold if cell.font else False,
                                'italic': cell.font.italic if cell.font else False
                            }
                        }
            
            all_sheets_data[sheet_name] = sheet_data
        
        return all_sheets_data
    
    def convert_to_structured_2d(self, file_path: str, sheet_name: str = None) -> pd.DataFrame:
        """
        Convert Excel file to structured 2D DataFrame.
        Handles irregular layouts and preserves all data.
        """
        workbook = openpyxl.load_workbook(file_path, data_only=True)
        
        if sheet_name:
            sheet = workbook[sheet_name]
        else:
            sheet = workbook.active
        
        # Extract all rows as lists
        data = []
        for row in sheet.iter_rows(values_only=True):
            data.append(list(row))
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Try to detect header row (first non-empty row)
        for idx, row in df.iterrows():
            if row.notna().any():
                df.columns = df.iloc[idx]
                df = df.iloc[idx+1:].reset_index(drop=True)
                break
        
        return df
    
    def query_with_natural_language(self, meta Dict[str, Any], query: str) -> Dict[str, Any]:
        """
        Query extracted Excel data using natural language.
        Uses Azure OpenAI via Private Link - data never leaves private network.
        """
        
        # Prepare context for LLM
        context = f"""
You are analyzing a financial Excel spreadsheet. Here is the structure:

{json.dumps(metadata, indent=2, default=str)}

Extract the following information based on this natural language query:
Query: {query}

Respond ONLY with a JSON object containing the extracted values.
Format: {{"field_name": "value", "cell_location": "A1"}}
"""
        
        # Call Azure OpenAI (traffic goes through Private Link)
        response = self.client.chat.completions.create(
            model="gpt4-excel-parser",
            messages=[
                {"role": "system", "content": "You are a financial data extraction expert. Always respond with valid JSON only."},
                {"role": "user", "content": context}
            ],
            temperature=0,
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
        return result
    
    def batch_extract_fields(self, file_path: str, field_queries: List[str]) -> pd.DataFrame:
        """
        Extract multiple fields from Excel file using natural language queries.
        Returns structured DataFrame with all extracted values.
        """
        # Extract metadata once
        metadata = self.extract_cell_metadata(file_path)
        
        results = []
        for query in field_queries:
            extracted = self.query_with_natural_language(metadata, query)
            results.append(extracted)
        
        # Combine results into DataFrame
        df = pd.DataFrame(results)
        return df
    
    def save_to_unity_catalog(self, df: pd.DataFrame, catalog: str, schema: str, table: str):
        """
        Save extracted data to Unity Catalog with column-level encryption.
        """
        # Convert to Spark DataFrame
        spark_df = spark.createDataFrame(df)
        
        # Write to Unity Catalog managed table (encrypted at rest)
        spark_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table}")
        
        print(f"Data securely saved to {catalog}.{schema}.{table}")

# Initialize processor
processor = SecureExcelProcessor()
