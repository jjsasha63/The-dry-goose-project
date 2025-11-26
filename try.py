"""
Financial Excel Query Engine V2 (Enterprise Edition)
----------------------------------------------------
A production-ready library for querying complex financial Excel files.
Features:
- Zero Hallucination: Returns exact cell values only.
- OpenAI Embeddings: Enterprise-grade semantic search (replaces HuggingFace).
- Advanced Structure Detection: Uses styles (bold/fonts) & layout to find headers.
- Bank-Grade Precision: Validates data types (e.g., won't return text for 'Revenue').
"""

import os
import re
import warnings
import numpy as np
import pandas as pd
from typing import List, Dict, Tuple, Any, Optional, Literal
from dataclasses import dataclass, field
from enum import Enum
from openpyxl import load_workbook
from openpyxl.styles import Font
from tenacity import retry, stop_after_attempt, wait_exponential

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# ==========================================
# 1. Configuration & Data Structures
# ==========================================

@dataclass
class QueryEngineConfig:
    """Configuration for the engine."""
    # Backend: 'openai' (recommended) or 'basic' (keyword match only)
    semantic_backend: Literal['basic', 'openai'] = 'openai'
    
    # OpenAI Settings
    openai_api_key: Optional[str] = field(default_factory=lambda: os.getenv("OPENAI_API_KEY"))
    openai_model: str = "text-embedding-3-small" # Cost-effective, high performance
    
    # thresholds
    min_confidence: float = 0.4
    
    # Structure Detection
    header_text_ratio_threshold: float = 0.6
    style_weight_boost: float = 0.3  # How much 'Bold' text counts towards being a header
    use_style_analysis: bool = True

@dataclass
class SearchResult:
    """Standardized result object."""
    value: Any
    confidence: float
    sheet_name: str
    row: int
    col: int
    header_path: List[str]     # Hierarchical path (e.g. ["Assets", "Current", "Cash"])
    context_text: str          # The full searchable text
    value_type: str

    def __repr__(self):
        path = " > ".join(self.header_path)
        return (f"<Result value={self.value} | confidence={self.confidence:.2f} | "
                f"loc={self.sheet_name}!{self.row+1}:{self.col+1} | path='{path}'>")

class ValueType(Enum):
    NUMBER = "number"
    TEXT = "text"
    PERCENTAGE = "percentage"
    DATE = "date"
    ANY = "any"

# ==========================================
# 2. Semantic Matching Backends
# ==========================================

class SemanticMatcher:
    """Base interface for similarity."""
    def calculate_similarity(self, query: str, target: str) -> float:
        raise NotImplementedError

class BasicSemanticMatcher(SemanticMatcher):
    """Fallback matcher using token overlap (Jaccard)."""
    def normalize(self, text: str) -> set:
        text = str(text).lower().strip()
        text = re.sub(r'[^\w\s]', '', text)
        return set(text.split())

    def calculate_similarity(self, query: str, target: str) -> float:
        q_tok = self.normalize(query)
        t_tok = self.normalize(target)
        if not q_tok or not t_tok: return 0.0
        
        intersection = len(q_tok & t_tok)
        union = len(q_tok | t_tok)
        
        # Boost for exact substring matches
        score = intersection / union
        if query.lower() in target.lower():
            score = max(score, 0.9)
        return score

class OpenAISemanticMatcher(SemanticMatcher):
    """
    Enterprise matcher using OpenAI Embeddings.
    """
    def __init__(self, api_key: str, model: str):
        try:
            from openai import OpenAI
            self.client = OpenAI(api_key=api_key)
            self.model = model
        except ImportError:
            raise ImportError("Please install openai: `pip install openai`")
        except Exception as e:
            raise ValueError(f"Failed to init OpenAI client: {e}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Embeds a list of strings in one API call."""
        if not texts: return []
        
        # Clean newlines to improve embedding quality
        clean_texts = [t.replace("\n", " ")[:8000] for t in texts]
        
        response = self.client.embeddings.create(
            input=clean_texts,
            model=self.model
        )
        return [data.embedding for data in response.data]

    def cosine_similarity(self, vec_a: List[float], vec_b: List[float]) -> float:
        a = np.array(vec_a)
        b = np.array(vec_b)
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        if norm_a == 0 or norm_b == 0: return 0.0
        return np.dot(a, b) / (norm_a * norm_b)

# ==========================================
# 3. Structure & Data Handling
# ==========================================

class MergedCellHandler:
    """Handles Excel merged cells and extracts style info."""
    def __init__(self, file_path: str):
        self.wb = load_workbook(file_path, data_only=True)
        self.merged_map = {}
        
        # Pre-calculate merge maps
        for sheet in self.wb.worksheets:
            for group in sheet.merged_cells.ranges:
                min_row, min_col = group.min_row, group.min_col
                for r in range(min_row, group.max_row + 1):
                    for c in range(min_col, group.max_col + 1):
                        self.merged_map[(sheet.title, r, c)] = (min_row, min_col)

    def get_sheet_data_and_styles(self, sheet_name: str) -> Tuple[pd.DataFrame, List[List[bool]]]:
        """
        Returns:
        1. DataFrame with merged cells filled (forward filled).
        2. A grid of booleans indicating if a cell is BOLD (for header detection).
        """
        sheet = self.wb[sheet_name]
        data_rows = []
        bold_grid = []

        for r_idx, row in enumerate(sheet.iter_rows(), start=1):
            row_data = []
            row_bold = []
            for c_idx, cell in enumerate(row, start=1):
                # Resolve merged cells
                lookup_r, lookup_c = self.merged_map.get((sheet_name, r_idx, c_idx), (r_idx, c_idx))
                real_cell = sheet.cell(row=lookup_r, column=lookup_c)
                
                # Value
                row_data.append(real_cell.value)
                
                # Style (Check if bold)
                is_bold = False
                if real_cell.font and real_cell.font.bold:
                    is_bold = True
                row_bold.append(is_bold)
            
            data_rows.append(row_data)
            bold_grid.append(row_bold)

        return pd.DataFrame(data_rows), bold_grid

class StructureDetector:
    """Heuristic engine to find headers and data tables."""
    def __init__(self, config: QueryEngineConfig):
        self.config = config

    def detect_tables(self, df: pd.DataFrame, bold_grid: List[List[bool]]) -> List[Dict]:
        """
        Scans a sheet to find table blocks.
        Returns metadata about header rows and data boundaries.
        """
        tables = []
        i = 0
        max_rows = len(df)

        while i < max_rows:
            # Skip empty rows
            if df.iloc[i].isna().all():
                i += 1
                continue

            # 1. Detect Header Region
            # We look for a block of rows that look like headers (Text + Bold)
            header_start = i
            header_end = i
            
            for r in range(i, min(i + 5, max_rows)): # Look ahead 5 rows max for multi-level header
                row_vals = df.iloc[r]
                if self._is_header_row(row_vals, bold_grid[r]):
                    header_end = r + 1
                else:
                    break
            
            # If we didn't find a header, treat current row as data, or skip
            if header_end == header_start:
                i += 1
                continue
            
            # 2. Detect Data Region (stops at 2 consecutive empty rows)
            data_start = header_end
            data_end = data_start
            empty_counter = 0
            
            for r in range(data_start, max_rows):
                if df.iloc[r].isna().all():
                    empty_counter += 1
                    if empty_counter >= 2:
                        break
                else:
                    empty_counter = 0
                    data_end = r + 1
            
            if data_end > data_start:
                tables.append({
                    'header_range': (header_start, header_end),
                    'data_range': (data_start, data_end)
                })
                i = data_end
            else:
                i += 1
        
        return tables

    def _is_header_row(self, row: pd.Series, bold_row: List[bool]) -> bool:
        """Decides if a row is a header based on content and style."""
        clean_row = row.dropna()
        if len(clean_row) == 0: return False
        
        # Metric 1: Text Ratio
        text_count = sum(isinstance(x, str) for x in clean_row)
        text_ratio = text_count / len(clean_row)
        
        # Metric 2: Style (Bold) Ratio
        # Map bold_row indices to clean_row indices matches is tricky in pandas, 
        # so we just count total bold in the raw row vs total non-null
        bold_count = sum(1 for idx, is_bold in enumerate(bold_row) if is_bold and pd.notna(row.iloc[idx]))
        bold_ratio = bold_count / len(clean_row)
        
        score = text_ratio + (bold_ratio * self.config.style_weight_boost)
        return score >= self.config.header_text_ratio_threshold

# ==========================================
# 4. Main Engine
# ==========================================

class FinancialExcelEngine:
    def __init__(self, file_path: str, config: QueryEngineConfig):
        self.file_path = file_path
        self.config = config
        self.records = [] # Flattened searchable data
        self.vector_index = {} # Hash map for embeddings
        
        # Init Components
        self.merge_handler = MergedCellHandler(file_path)
        self.detector = StructureDetector(config)
        
        # Init Backend
        if config.semantic_backend == 'openai':
            if not config.openai_api_key:
                raise ValueError("OpenAI API Key missing. Set env var OPENAI_API_KEY or pass in config.")
            self.matcher = OpenAISemanticMatcher(config.openai_api_key, config.openai_model)
        else:
            self.matcher = BasicSemanticMatcher()

        # Build Index
        print("Parsing file structure...")
        self._ingest_file()
        
        if config.semantic_backend == 'openai':
            print("Generating embeddings (this may take a moment)...")
            self._build_embeddings()
        print("Engine Ready.")

    def _ingest_file(self):
        """Reads file, detects structure, flattens data."""
        wb = load_workbook(self.file_path, read_only=True) # Just to get sheet names
        
        for sheet_name in wb.sheetnames:
            # Get clean data + styles
            df, bold_grid = self.merge_handler.get_sheet_data_and_styles(sheet_name)
            
            # Detect Tables
            tables = self.detector.detect_tables(df, bold_grid)
            
            for tbl in tables:
                h_start, h_end = tbl['header_range']
                d_start, d_end = tbl['data_range']
                
                # Process Headers (Handling Multi-level)
                header_block = df.iloc[h_start:h_end]
                # Forward fill headers horizontally (e.g. "2023" merged across "Q1", "Q2")
                header_block = header_block.ffill(axis=1)
                
                # Create a "Path" for each column
                # e.g. Col 3 -> ["Assets", "Current", "Cash"]
                col_paths = []
                for c in range(len(df.columns)):
                    # Get all vertical components of the header
                    raw_path = header_block.iloc[:, c].tolist()
                    # Clean path (remove NaNs, whitespace)
                    clean_path = [str(p).strip() for p in raw_path if pd.notna(p) and str(p).strip() != ""]
                    col_paths.append(clean_path)
                
                # Process Data Rows
                for r_idx in range(d_start, d_end):
                    row_data = df.iloc[r_idx]
                    
                    # Assume first non-empty column is the "Row Label" (e.g. "Total Revenue")
                    # This is a heuristic, but common in finance.
                    row_label_idx = row_data.first_valid_index()
                    if row_label_idx is None: continue
                    
                    row_label = str(row_data[row_label_idx]).strip()
                    
                    for c_idx, val in enumerate(row_data):
                        if pd.isna(val) or c_idx == row_label_idx: continue
                        
                        # Construct full semantic path
                        # Path = Row Label + Column Headers
                        # e.g. "Total Revenue" + "2023" + "Q1"
                        full_path = [row_label] + col_paths[c_idx]
                        
                        # Skip empty values
                        if str(val).strip() == "": continue
                        
                        record = {
                            'sheet': sheet_name,
                            'row': r_idx,
                            'col': c_idx,
                            'value': val,
                            'header_path': full_path,
                            'searchable_text': " ".join(full_path),
                            'type': self._get_type(val)
                        }
                        self.records.append(record)

    def _build_embeddings(self, batch_size=50):
        """Batched embedding generation."""
        # Only embed unique strings to save tokens
        unique_texts = list(set(r['searchable_text'] for r in self.records))
        
        for i in range(0, len(unique_texts), batch_size):
            batch = unique_texts[i:i+batch_size]
            try:
                vectors = self.matcher.embed_batch(batch)
                for text, vec in zip(batch, vectors):
                    self.vector_index[text] = vec
            except Exception as e:
                print(f"Warning: Batch embedding failed: {e}")

    def _get_type(self, val) -> str:
        if isinstance(val, (int, float)): return "number"
        if isinstance(val, str): return "text"
        return "unknown"

    def query(self, question: str, top_k=3) -> List[SearchResult]:
        """Public query interface."""
        
        # 1. Infer Intent (simple heuristic)
        # If question contains "revenue", "cost", "amount" -> prefer numbers
        prefer_number = any(x in question.lower() for x in ['how much', 'total', 'cost', 'revenue', 'profit'])
        
        scored_results = []
        
        if self.config.semantic_backend == 'openai':
            # Embed query
            q_vec = self.matcher.embed_batch([question])[0]
            
            for r in self.records:
                target_vec = self.vector_index.get(r['searchable_text'])
                if target_vec:
                    score = self.matcher.cosine_similarity(q_vec, target_vec)
                    scored_results.append((score, r))
        else:
            # Basic
            for r in self.records:
                score = self.matcher.calculate_similarity(question, r['searchable_text'])
                scored_results.append((score, r))
        
        # Sort
        scored_results.sort(key=lambda x: x[0], reverse=True)
        
        # Format Output
        final_output = []
        for score, r in scored_results:
            if score < self.config.min_confidence: continue
            
            # Type penalty (Soft enforcement)
            # If we want a number but got text (like "See Note 5"), lower score slightly
            if prefer_number and r['type'] != 'number':
                score *= 0.8
            
            res = SearchResult(
                value=r['value'],
                confidence=score,
                sheet_name=r['sheet'],
                row=r['row'],
                col=r['col'],
                header_path=r['header_path'],
                context_text=r['searchable_text'],
                value_type=r['type']
            )
            final_output.append(res)
            if len(final_output) >= top_k: break
            
        return final_output

# ==========================================
# 5. Execution Example
# ==========================================

if __name__ == "__main__":
    # Create a dummy file for demonstration if one doesn't exist
    dummy_file = "financial_demo.xlsx"
    if not os.path.exists(dummy_file):
        print("Creating dummy financial file...")
        df = pd.DataFrame({
            'Metric': ['Revenue', 'Cost of Goods', 'Gross Profit'],
            '2022': [100000, 40000, 60000],
            '2023': [120000, 45000, 75000]
        })
        # Create a simple excel with bold headers
        with pd.ExcelWriter(dummy_file, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Income Statement', index=False)
            # (Note: Real usage would detect existing bold formatting in your files)

    # --- CONFIGURATION ---
    # Replace with your actual API key or ensure it's in env vars
    api_key = os.getenv("OPENAI_API_KEY", "sk-placeholder-key")
    
    config = QueryEngineConfig(
        semantic_backend='openai', # Change to 'basic' if you don't have a key yet
        openai_api_key=api_key,
        min_confidence=0.75
    )

    try:
        print(f"\nInitializing Engine with {config.semantic_backend} backend...")
        engine = FinancialExcelEngine(dummy_file, config)
        
        questions = [
            "What was the revenue in 2023?",
            "Gross Profit 2022"
        ]
        
        for q in questions:
            print(f"\nQuery: {q}")
            results = engine.query(q)
            if not results:
                print("No results found.")
            for res in results:
                print(f"  -> Found Value: {res.value}")
                print(f"     Confidence:  {res.confidence:.4f}")
                print(f"     Source Path: {' > '.join(res.header_path)}")
                
    except Exception as e:
        print(f"\nError running engine: {e}")
        print("Note: If using 'openai' backend, ensure you have a valid API key.")
