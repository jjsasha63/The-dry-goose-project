"""
Financial Excel Query Engine V5 (Fixed Division-by-Zero)
------------------------------------------------------------
All division errors fixed with safe guards.
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

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# ==========================================
# 1. Enhanced Data Structures (Fixed)
# ==========================================

@dataclass
class TableBounds:
    """Explicit table boundaries with location context."""
    sheet_name: str
    top_row: int
    bottom_row: int
    left_col: int
    right_col: int
    header_rows: int

@dataclass
class CellRole:
    """Cell classification for precise handling."""
    row: int
    col: int
    value: Any
    role: str  # 'column_header', 'row_header', 'value', 'empty'
    is_bold: bool
    table_id: Optional[int] = None

@dataclass
class QueryEngineConfig:
    semantic_backend: Literal['basic', 'openai'] = 'openai'
    openai_api_key: Optional[str] = field(default_factory=lambda: os.getenv("OPENAI_API_KEY"))
    openai_model: str = "text-embedding-3-small"
    min_confidence: float = 0.5
    header_text_ratio_threshold: float = 0.6
    style_weight_boost: float = 0.3
    numeric_row_string_threshold: float = 0.3

@dataclass
class SearchResult:
    value: Any
    confidence: float
    sheet_name: str
    row: int
    col: int
    header_path: List[str]
    context_text: str
    value_type: str
    table_bounds: Optional[TableBounds] = None

    def __repr__(self):
        path = " > ".join(self.header_path)
        bounds = f"{self.table_bounds.sheet_name}" if self.table_bounds else "unknown"
        return (f"<Result value={self.value} | conf={self.confidence:.2f} | "
                f"path='{path}' | table={bounds}>")

# ==========================================
# 2. Semantic Matching (Safe Division)
# ==========================================

class SemanticMatcher:
    def calculate_similarity(self, query: str, target: str) -> float:
        raise NotImplementedError

class BasicSemanticMatcher(SemanticMatcher):
    def normalize(self, text: str) -> set:
        text = str(text).lower().strip()
        text = re.sub(r'[^\w\s]', '', text)
        return set(text.split())

    def calculate_similarity(self, query: str, target: str) -> float:
        q_tok = self.normalize(query)
        t_tok = self.normalize(target)
        if not q_tok or not t_tok: 
            return 0.0
        
        intersection = len(q_tok & t_tok)
        union = len(q_tok | t_tok)
        score = intersection / union if union > 0 else 0.0
        if query.lower() in target.lower():
            score = max(score, 0.9)
        return score

class OpenAISemanticMatcher(SemanticMatcher):
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
        if not texts: 
            return []
        clean_texts = [t.replace("\n", " ")[:8000] for t in texts]
        response = self.client.embeddings.create(input=clean_texts, model=self.model)
        return [data.embedding for data in response.data]

    def cosine_similarity(self, vec_a: List[float], vec_b: List[float]) -> float:
        a = np.array(vec_a)
        b = np.array(vec_b)
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        if norm_a == 0 or norm_b == 0: 
            return 0.0
        return np.dot(a, b) / (norm_a * norm_b)

# ==========================================
# 3. Advanced Structure Detection (Fixed)
# ==========================================

class AdvancedStructureDetector:
    def __init__(self, config: QueryEngineConfig):
        self.config = config
    
    def analyze_sheet(self, df: pd.DataFrame, bold_grid: List[List[bool]]) -> List[TableBounds]:
        tables = []
        i = 0
        
        while i < len(df):
            header_info = self._detect_header_block(df, bold_grid, i)
            if not header_info:
                i += 1
                continue
            
            header_start, header_rows = header_info
            left_col, right_col = self._find_horizontal_span(df, header_start, header_rows)
            data_start = header_start + header_rows
            data_end = self._find_data_end(df, data_start)
            
            tables.append(TableBounds(
                sheet_name="",
                top_row=header_start,
                bottom_row=data_end,
                left_col=left_col,
                right_col=right_col,
                header_rows=header_rows
            ))
            i = data_end
        
        return tables
    
    def _detect_header_block(self, df: pd.DataFrame, bold_grid: List[List[bool]], start_row: int) -> Optional[Tuple[int, int]]:
        header_start = start_row
        header_end = start_row
        
        for r in range(start_row, min(start_row + 5, len(df))):
            row_vals = df.iloc[r]
            if self._is_header_row(row_vals, bold_grid[r]):
                header_end = r + 1
            else:
                break
        
        return (header_start, header_end - header_start) if header_end > header_start else None
    
    def _find_horizontal_span(self, df: pd.DataFrame, header_start: int, header_rows: int) -> Tuple[int, int]:
        header_block = df.iloc[header_start:header_start + header_rows]
        non_empty_cols = []
        
        for c in range(len(df.columns)):
            if header_block.iloc[:, c].notna().any():
                non_empty_cols.append(c)
        
        if not non_empty_cols:
            return 0, min(len(df.columns) - 1, 5)  # Default small span
        
        return min(non_empty_cols), max(non_empty_cols)
    
    def _find_data_end(self, df: pd.DataFrame, start_row: int) -> int:
        empty_count = 0
        for r in range(start_row, len(df)):
            if df.iloc[r].isna().all():
                empty_count += 1
                if empty_count >= 2:
                    return r
            else:
                empty_count = 0
        return len(df)
    
    def _is_header_row(self, row: pd.Series, bold_row: List[bool]) -> bool:
        clean_row = row.dropna()
        if len(clean_row) == 0: 
            return False
        
        text_count = sum(1 for x in clean_row if isinstance(x, str))
        text_ratio = text_count / len(clean_row)
        
        bold_count = sum(1 for idx, is_bold in enumerate(bold_row) 
                        if is_bold and pd.notna(row.iloc[idx]))
        bold_ratio = bold_count / max(len(clean_row), 1)
        
        score = text_ratio + (bold_ratio * self.config.style_weight_boost)
        return score >= self.config.header_text_ratio_threshold
    
    def classify_data_row(self, row: pd.Series, bold_row: List[bool], 
                         table_bounds: TableBounds) -> List[CellRole]:
        roles = []
        non_null_count = sum(1 for val in row if pd.notna(val))
        
        if non_null_count == 0:
            # All empty row
            for col_idx in range(len(row)):
                roles.append(CellRole(col_idx, col_idx, None, 'empty', 
                                    bold_row[col_idx] if col_idx < len(bold_row) else False))
            return roles
        
        # Count numeric vs string
        numeric_count = 0
        string_positions = []
        
        for idx, val in enumerate(row):
            if pd.notna(val):
                if self._is_numeric_like(val):
                    numeric_count += 1
                else:
                    string_positions.append(idx)
        
        # Safe division for numeric row detection
        string_ratio = len(string_positions) / non_null_count if non_null_count > 0 else 0
        is_numeric_row = (numeric_count / non_null_count > (1 - self.config.numeric_row_string_threshold)) if non_null_count > 0 else False
        
        for col_idx, val in enumerate(row):
            is_bold = bold_row[col_idx] if col_idx < len(bold_row) else False
            
            # Within table bounds?
            in_bounds = (table_bounds.left_col <= col_idx <= table_bounds.right_col)
            
            if pd.isna(val):
                role = 'empty'
            elif not in_bounds:
                role = 'empty'
            elif is_numeric_row and col_idx in string_positions:
                role = 'row_header'
            elif self._is_numeric_like(val):
                role = 'value'
            else:
                role = 'row_header'
        
            roles.append(CellRole(col_idx, col_idx, val, role, is_bold))
        
        return roles

    def _is_numeric_like(self, val: Any) -> bool:
        if pd.isna(val): 
            return False
        if isinstance(val, (int, float)): 
            return True
        if isinstance(val, str):
            cleaned = str(val).replace(',', '').strip()
            return bool(re.match(r'^-?\d+(?:\.\d+)?(?:[kmb]?)?:?$', cleaned))
        return False

# ==========================================
# 4. Enhanced Merged Cell Handler (Safe)
# ==========================================

class EnhancedMergedCellHandler:
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

        max_cols = sheet.max_column
        for r_idx, row in enumerate(sheet.iter_rows(), start=1):
            row_data = []
            row_bold = []
            for c_idx, cell in enumerate(row, start=1):
                lookup_r, lookup_c = self.merged_map.get((sheet_name, r_idx, c_idx), (r_idx, c_idx))
                real_cell = sheet.cell(row=lookup_r, column=lookup_c)
                
                row_data.append(real_cell.value)
                is_bold = bool(real_cell.font and real_cell.font.bold)
                row_bold.append(is_bold)
            
            # Pad if row is shorter than expected
            while len(row_data) < max_cols:
                row_data.append(None)
                row_bold.append(False)
            
            data_rows.append(row_data[:max_cols])
            bold_grid.append(row_bold[:max_cols])

        return pd.DataFrame(data_rows), bold_grid

# ==========================================
# 5. Main Engine V5 (Fixed)
# ==========================================

class FinancialExcelEngineV5:
    def __init__(self, file_path: str, config: QueryEngineConfig):
        self.file_path = file_path
        self.config = config
        self.records: List[Dict] = []
        self.tables: List[TableBounds] = []
        self.vector_index = {}
        
        self.handler = EnhancedMergedCellHandler(file_path)
        self.detector = AdvancedStructureDetector(config)
        
        if config.semantic_backend == 'openai':
            if not config.openai_api_key:
                raise ValueError("OpenAI API Key missing.")
            self.matcher = OpenAISemanticMatcher(config.openai_api_key, config.openai_model)
        else:
            self.matcher = BasicSemanticMatcher()

        print("🔍 Advanced structure analysis...")
        self._ingest_file_enhanced()
        
        if config.semantic_backend == 'openai':
            print("📊 Building vector index...")
            self._build_embeddings()
        print(f"✅ Engine ready: {len(self.records)} values, {len(self.tables)} tables")

    def _ingest_file_enhanced(self):
        wb = load_workbook(self.file_path, read_only=True)
        table_id = 0
        
        for sheet_name in wb.sheetnames:
            try:
                df, bold_grid = self.handler.get_sheet_data_and_styles(sheet_name)
                
                raw_tables = self.detector.analyze_sheet(df, bold_grid)
                
                for raw_table in raw_tables:
                    table = TableBounds(
                        sheet_name=sheet_name,
                        top_row=raw_table.top_row,
                        bottom_row=raw_table.bottom_row,
                        left_col=raw_table.left_col,
                        right_col=raw_table.right_col,
                        header_rows=raw_table.header_rows
                    )
                    self.tables.append(table)
                    
                    # Safe header processing
                    header_end = min(table.top_row + table.header_rows, len(df))
                    header_block = df.iloc[table.top_row:header_end].ffill(axis=1)
                    
                    col_paths = []
                    for c in range(table.left_col, table.right_col + 1):
                        if c < len(header_block.columns):
                            raw_path = header_block.iloc[:, c].tolist()
                            clean_path = [str(p).strip() for p in raw_path 
                                        if pd.notna(p) and str(p).strip()]
                            col_paths.append(clean_path)
                        else:
                            col_paths.append([])
                    
                    # Process data rows
                    data_start = table.top_row + table.header_rows
                    for r_idx in range(data_start, table.bottom_row):
                        if r_idx >= len(df) or r_idx >= len(bold_grid):
                            break
                            
                        row_data = df.iloc[r_idx]
                        row_roles = self.detector.classify_data_row(
                            row_data, bold_grid[r_idx], table
                        )
                        
                        row_header_candidates = [role for role in row_roles if role.role == 'row_header']
                        if not row_header_candidates:
                            continue
                        
                        row_label = str(row_header_candidates[0].value).strip()
                        
                        for cell_role in row_roles:
                            if (cell_role.role != 'value' or 
                                cell_role.col < table.left_col or 
                                cell_role.col > table.right_col):
                                continue
                            
                            col_idx_in_table = cell_role.col - table.left_col
                            if col_idx_in_table < len(col_paths):
                                col_path = col_paths[col_idx_in_table]
                                full_path = [row_label] + col_path
                                
                                record = {
                                    'sheet': sheet_name,
                                    'row': r_idx,
                                    'col': cell_role.col,
                                    'value': cell_role.value,
                                    'header_path': full_path,
                                    'searchable_text': " ".join(full_path),
                                    'type': self._get_type(cell_role.value),
                                    'table_id': table_id
                                }
                                self.records.append(record)
                    table_id += 1
            except Exception as e:
                print(f"Warning: Error processing sheet {sheet_name}: {e}")

    def _build_embeddings(self, batch_size=50):
        if not self.records:
            return
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
        if pd.isna(val):
            return "unknown"
        if isinstance(val, (int, float)): 
            return "number"
        if isinstance(val, str): 
            return "text"
        return "unknown"

    def _extract_critical_tokens(self, query: str) -> Set[str]:
        tokens = set()
        parts = query.split()
        for p in parts:
            clean_p = p.strip().lower()
            if re.match(r'^(19|20)\d{2}$', clean_p):
                tokens.add(clean_p)
            elif re.match(r'^q[1-4]$', clean_p):
                tokens.add(clean_p)
            elif clean_p in ['total', 'net', 'gross', 'operating', 'ebitda']:
                tokens.add(clean_p)
        return tokens

    def query(self, question: str, top_k=5) -> List[SearchResult]:
        if not self.records:
            return []
            
        query_tokens = set(question.lower().split())
        critical_tokens = self._extract_critical_tokens(question)
        prefer_number = any(x in question.lower() for x in ['how much', 'total', 'cost', 'revenue', 'profit'])
        
        scored_results = []
        q_vec = None
        
        if self.config.semantic_backend == 'openai':
            try:
                q_vec = self.matcher.embed_batch([question])[0]
            except:
                q_vec = None

        for r in self.records:
            target_text = r['searchable_text'].lower()
            
            # Safe constraint penalty
            constraint_penalty = 1.0
            if critical_tokens:
                missing = [t for t in critical_tokens if t not in target_text]
                if missing:
                    constraint_penalty = 0.1
            
            # Safe semantic score
            semantic_score = 0.0
            if self.config.semantic_backend == 'openai' and q_vec:
                target_vec = self.vector_index.get(r['searchable_text'])
                if target_vec:
                    semantic_score = self.matcher.cosine_similarity(q_vec, target_vec)
            else:
                semantic_score = self.matcher.calculate_similarity(question, r['searchable_text'])
            
            # Safe keyword coverage
            target_tokens = set(target_text.split())
            keyword_coverage = 0.0
            if query_tokens:
                intersection = query_tokens.intersection(target_tokens)
                keyword_coverage = len(intersection) / len(query_tokens)
            
            # Weighted score
            base_score = (semantic_score * 0.65) + (keyword_coverage * 0.35)
            if question.lower() in target_text:
                base_score = max(base_score, 0.98)
                
            final_score = base_score * constraint_penalty
            if prefer_number and r['type'] != 'number':
                final_score *= 0.85

            if final_score >= self.config.min_confidence:
                table = next((t for t in self.tables if t.sheet_name == r['sheet']), None)
                scored_results.append((final_score, {
                    **r, 'table_bounds': table
                }))

        scored_results.sort(key=lambda x: x[0], reverse=True)
        
        final_output = []
        for score, r in scored_results[:top_k]:
            final_output.append(SearchResult(
                value=r['value'],
                confidence=score,
                sheet_name=r['sheet'],
                row=r['row'],
                col=r['col'],
                header_path=r['header_path'],
                context_text=r['searchable_text'],
                value_type=r['type'],
                table_bounds=r['table_bounds']
            ))
        return final_output

# ==========================================
# Example Usage
# ==========================================
if __name__ == "__main__":
    dummy_file = "financial_demo.xlsx"
    if not os.path.exists(dummy_file):
        print("Creating demo file...")
        df = pd.DataFrame({
            'Category': ['Revenue', 'COGS', 'Gross Profit'],
            '2023 Q1': [100, 40, 60],
            '2023 Q2': [110, 42, 68],
            '2024 Q1': [120, 45, 75]
        })
        with pd.ExcelWriter(dummy_file, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='P&L', index=False)

    config = QueryEngineConfig(
        semantic_backend='basic',  # Safe for testing
        min_confidence=0.4
    )

    try:
        engine = FinancialExcelEngineV5(dummy_file, config)
        
        for q in ["Revenue 2023", "Gross Profit"]:
            print(f"\n🔍 '{q}' -> {[str(r) for r in engine.query(q)]}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
