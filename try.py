"""
Financial Excel Query Engine V4 (Precision Edition)
---------------------------------------------------
A production-ready library for querying complex financial Excel files.

Features:
- Zero Hallucination: Returns exact cell values only.
- Constraint Enforcement: Strictly enforces years, quarters, and key financial terms.
- Hybrid Search: Combines OpenAI Embeddings + Weighted Keyword Coverage.
- Advanced Structure Detection: Uses styles & layout to find multi-level headers.
"""

import os
import re
import warnings
import numpy as np
import pandas as pd
from typing import List, Dict, Tuple, Any, Optional, Literal, Set
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
    openai_model: str = "text-embedding-3-small"
    
    # Search Thresholds
    min_confidence: float = 0.5  # Slightly higher default for precision
    
    # Structure Detection Settings
    header_text_ratio_threshold: float = 0.6
    style_weight_boost: float = 0.3
    use_style_analysis: bool = True

@dataclass
class SearchResult:
    """Standardized result object."""
    value: Any
    confidence: float
    sheet_name: str
    row: int
    col: int
    header_path: List[str]
    context_text: str
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
    """Fallback matcher using token overlap."""
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
        
        score = intersection / union if union > 0 else 0.0
        if query.lower() in target.lower():
            score = max(score, 0.9)
        return score

class OpenAISemanticMatcher(SemanticMatcher):
    """Enterprise matcher using OpenAI Embeddings."""
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
        if not texts: return []
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
        
        for sheet in self.wb.worksheets:
            for group in sheet.merged_cells.ranges:
                min_row, min_col = group.min_row, group.min_col
                for r in range(min_row, group.max_row + 1):
                    for c in range(min_col, group.max_col + 1):
                        self.merged_map[(sheet.title, r, c)] = (min_row, min_col)

    def get_sheet_data_and_styles(self, sheet_name: str) -> Tuple[pd.DataFrame, List[List[bool]]]:
        sheet = self.wb[sheet_name]
        data_rows = []
        bold_grid = []

        for r_idx, row in enumerate(sheet.iter_rows(), start=1):
            row_data = []
            row_bold = []
            for c_idx, cell in enumerate(row, start=1):
                lookup_r, lookup_c = self.merged_map.get((sheet_name, r_idx, c_idx), (r_idx, c_idx))
                real_cell = sheet.cell(row=lookup_r, column=lookup_c)
                
                row_data.append(real_cell.value)
                is_bold = bool(real_cell.font and real_cell.font.bold)
                row_bold.append(is_bold)
            
            data_rows.append(row_data)
            bold_grid.append(row_bold)

        return pd.DataFrame(data_rows), bold_grid

class StructureDetector:
    """Heuristic engine to find headers and data tables."""
    def __init__(self, config: QueryEngineConfig):
        self.config = config

    def detect_tables(self, df: pd.DataFrame, bold_grid: List[List[bool]]) -> List[Dict]:
        tables = []
        i = 0
        max_rows = len(df)

        while i < max_rows:
            if df.iloc[i].isna().all():
                i += 1
                continue

            header_start = i
            header_end = i
            
            for r in range(i, min(i + 5, max_rows)):
                row_vals = df.iloc[r]
                if self._is_header_row(row_vals, bold_grid[r]):
                    header_end = r + 1
                else:
                    break
            
            if header_end == header_start:
                i += 1
                continue
            
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
        clean_row = row.dropna()
        if len(clean_row) == 0: return False
        
        text_count = sum(isinstance(x, str) for x in clean_row)
        text_ratio = text_count / len(clean_row)
        
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
        self.records = []
        self.vector_index = {}
        
        self.merge_handler = MergedCellHandler(file_path)
        self.detector = StructureDetector(config)
        
        if config.semantic_backend == 'openai':
            if not config.openai_api_key:
                raise ValueError("OpenAI API Key missing.")
            self.matcher = OpenAISemanticMatcher(config.openai_api_key, config.openai_model)
        else:
            self.matcher = BasicSemanticMatcher()

        print("Parsing file structure...")
        self._ingest_file()
        
        if config.semantic_backend == 'openai':
            print("Generating embeddings...")
            self._build_embeddings()
        print("Engine Ready.")

    def _ingest_file(self):
        wb = load_workbook(self.file_path, read_only=True)
        
        for sheet_name in wb.sheetnames:
            df, bold_grid = self.merge_handler.get_sheet_data_and_styles(sheet_name)
            tables = self.detector.detect_tables(df, bold_grid)
            
            for tbl in tables:
                h_start, h_end = tbl['header_range']
                d_start, d_end = tbl['data_range']
                
                header_block = df.iloc[h_start:h_end].ffill(axis=1)
                col_paths = []
                for c in range(len(df.columns)):
                    raw_path = header_block.iloc[:, c].tolist()
                    clean_path = [str(p).strip() for p in raw_path if pd.notna(p) and str(p).strip() != ""]
                    col_paths.append(clean_path)
                
                for r_idx in range(d_start, d_end):
                    row_data = df.iloc[r_idx]
                    row_label_idx = row_data.first_valid_index()
                    if row_label_idx is None: continue
                    
                    row_label = str(row_data[row_label_idx]).strip()
                    
                    for c_idx, val in enumerate(row_data):
                        if pd.isna(val) or c_idx == row_label_idx: continue
                        if str(val).strip() == "": continue
                        
                        full_path = [row_label] + col_paths[c_idx]
                        
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
    
    def _extract_critical_tokens(self, query: str) -> Set[str]:
        """Identify tokens that MUST exist (Years, Quarters, specific Codes)."""
        tokens = set()
        parts = query.split()
        for p in parts:
            clean_p = p.strip().lower()
            # Regex for Years (19XX or 20XX)
            if re.match(r'^(19|20)\d{2}$', clean_p):
                tokens.add(clean_p)
            # Regex for Quarters (Q1-Q4)
            elif re.match(r'^q[1-4]$', clean_p):
                tokens.add(clean_p)
            # Structural Modifiers
            elif clean_p in ['total', 'net', 'gross', 'operating', 'ebitda']:
                tokens.add(clean_p)
        return tokens

    def query(self, question: str, top_k=5) -> List[SearchResult]:
        """Precision Query: Strict Constraint Enforcement + Hybrid Scoring."""
        query_tokens = set(question.lower().split())
        critical_tokens = self._extract_critical_tokens(question)
        prefer_number = any(x in question.lower() for x in ['how much', 'total', 'cost', 'revenue', 'profit', 'sum'])
        
        scored_results = []
        q_vec = None
        
        if self.config.semantic_backend == 'openai':
            q_vec = self.matcher.embed_batch([question])[0]

        for r in self.records:
            target_text = r['searchable_text'].lower()
            
            # --- A. Constraint Check ---
            constraint_penalty = 1.0
            if critical_tokens:
                missing_critical = [t for t in critical_tokens if t not in target_text]
                if missing_critical:
                    constraint_penalty = 0.1 # Severe penalty for missing year/type
            
            # --- B. Semantic Score ---
            semantic_score = 0.0
            if self.config.semantic_backend == 'openai' and q_vec:
                target_vec = self.vector_index.get(r['searchable_text'])
                if target_vec:
                    semantic_score = self.matcher.cosine_similarity(q_vec, target_vec)
            else:
                semantic_score = self.matcher.calculate_similarity(question, r['searchable_text'])
            
            # --- C. Keyword Overlap Score ---
            target_tokens = set(target_text.split())
            keyword_coverage = 0.0
            if query_tokens:
                intersection = query_tokens.intersection(target_tokens)
                keyword_coverage = len(intersection) / len(query_tokens)
            
            # --- D. Weighted Blend ---
            # 65% Semantic (Context) + 35% Keywords (Precision)
            base_score = (semantic_score * 0.65) + (keyword_coverage * 0.35)
            
            # Perfect substring boost
            if question.lower() in target_text:
                base_score = max(base_score, 0.98)
                
            final_score = base_score * constraint_penalty
            
            # --- E. Type Validation ---
            if prefer_number and r['type'] != 'number':
                final_score *= 0.85

            if final_score >= self.config.min_confidence:
                scored_results.append((final_score, r))
        
        scored_results.sort(key=lambda x: x[0], reverse=True)
        
        final_output = []
        for score, r in scored_results:
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
# 5. Example Usage
# ==========================================
if __name__ == "__main__":
    # Demo Setup
    dummy_file = "financial_demo.xlsx"
    if not os.path.exists(dummy_file):
        print("Creating dummy financial file...")
        df = pd.DataFrame({
            'Metric': ['Revenue', 'Net Income', 'Gross Profit'],
            '2022': [100, 20, 40],
            '2023': [120, 25, 50]
        })
        with pd.ExcelWriter(dummy_file, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='P&L', index=False)

    # Config
    api_key = os.getenv("OPENAI_API_KEY", "sk-placeholder-key")
    config = QueryEngineConfig(
        semantic_backend='openai', 
        openai_api_key=api_key,
        min_confidence=0.6  # High confidence for precision
    )

    try:
        print("\n--- Initializing Precision Engine ---")
        engine = FinancialExcelEngine(dummy_file, config)
        
        # Test Queries
        queries = [
            "Revenue 2023",      # Critical token: 2023
            "Net Income 2022",   # Critical token: Net, 2022
            "Total Assets"       # Semantic match (if present)
        ]
        
        for q in queries:
            print(f"\nQuery: '{q}'")
            results = engine.query(q)
            if not results:
                print("  No results found (Strict constraints applied).")
            for r in results:
                print(f"  [Score: {r.confidence:.2f}] {r.value} (Path: {' > '.join(r.header_path)})")
                
    except Exception as e:
        print(f"\nError: {e}")
        print("Ensure you have set OPENAI_API_KEY environment variable.")
