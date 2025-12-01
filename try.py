"""
Financial Excel Query Engine V5 (Production-Grade Edition)
-----------------------------------------------------------
A production-ready library for querying complex financial Excel files with
advanced structure detection and multi-level header support.

Key Enhancements:
- Multi-table detection per sheet with smart boundary detection
- Multi-level HORIZONTAL and VERTICAL header flattening
- Cell classification (header vs data) using multiple signals
- Exact match prioritization with boosted scoring
- Enhanced type validation and constraint enforcement
- Robust handling of dirty data (empty rows, merged cells, inconsistent formatting)
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
from openpyxl.styles import Font, Alignment
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
    min_confidence: float = 0.5
    exact_match_boost: float = 0.25  # Boost for exact substring matches
    
    # Structure Detection Settings
    header_text_ratio_threshold: float = 0.55
    style_weight_boost: float = 0.35
    use_style_analysis: bool = True
    max_header_rows: int = 10
    max_empty_rows_between_tables: int = 3
    
    # Cell Classification
    min_numeric_cells_for_data_row: int = 2

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
    is_exact_match: bool = False

    def __repr__(self):
        path = " > ".join(self.header_path)
        exact_marker = " [EXACT]" if self.is_exact_match else ""
        return (f"<Result value={self.value} | confidence={self.confidence:.2f}{exact_marker} | "
                f"loc={self.sheet_name}!R{self.row+1}C{self.col+1} | path='{path}'>")

class CellType(Enum):
    """Cell classification types based on Microsoft Research taxonomy."""
    HEADER_HORIZONTAL = "header_horizontal"
    HEADER_VERTICAL = "header_vertical"
    DATA_VALUE = "data_value"
    INDEX = "index"
    METADATA = "metadata"
    EMPTY = "empty"

class ValueType(Enum):
    NUMBER = "number"
    TEXT = "text"
    PERCENTAGE = "percentage"
    DATE = "date"
    CURRENCY = "currency"
    ANY = "any"

# ==========================================
# 2. Semantic Matching Backends
# ==========================================

class SemanticMatcher:
    """Base interface for similarity."""
    def calculate_similarity(self, query: str, target: str) -> float:
        raise NotImplementedError

class BasicSemanticMatcher(SemanticMatcher):
    """Fallback matcher using token overlap with exact match detection."""
    def normalize(self, text: str) -> set:
        text = str(text).lower().strip()
        text = re.sub(r'[^\w\s]', '', text)
        return set(text.split())

    def calculate_similarity(self, query: str, target: str) -> float:
        q_lower = query.lower()
        t_lower = target.lower()
        
        # Exact substring match gets very high score
        if q_lower in t_lower or t_lower in q_lower:
            return 0.95
        
        q_tok = self.normalize(query)
        t_tok = self.normalize(target)
        if not q_tok or not t_tok: return 0.0
        
        intersection = len(q_tok & t_tok)
        union = len(q_tok | t_tok)
        
        jaccard = intersection / union if union > 0 else 0.0
        
        # Token coverage bonus
        coverage = intersection / len(q_tok) if q_tok else 0.0
        
        return max(jaccard, coverage * 0.9)

class OpenAISemanticMatcher(SemanticMatcher):
    """Enterprise matcher using OpenAI Embeddings with exact match detection."""
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
    def embed_batch(self, texts: List[str], batch_size: int = 100) -> List[List[float]]:
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
        return float(np.dot(a, b) / (norm_a * norm_b))

# ==========================================
# 3. Structure & Data Handling
# ==========================================

class MergedCellHandler:
    """Handles Excel merged cells and extracts comprehensive style info."""
    def __init__(self, file_path: str):
        self.wb = load_workbook(file_path, data_only=True)
        self.merged_map = {}
        
        for sheet in self.wb.worksheets:
            for group in sheet.merged_cells.ranges:
                min_row, min_col = group.min_row, group.min_col
                for r in range(min_row, group.max_row + 1):
                    for c in range(min_col, group.max_col + 1):
                        self.merged_map[(sheet.title, r, c)] = (min_row, min_col)

    def get_sheet_data_and_styles(self, sheet_name: str) -> Tuple[pd.DataFrame, List[List[Dict]]]:
        """Returns dataframe and style grid with bold, font_size, alignment info."""
        sheet = self.wb[sheet_name]
        data_rows = []
        style_grid = []

        for r_idx, row in enumerate(sheet.iter_rows(), start=1):
            row_data = []
            row_styles = []
            for c_idx, cell in enumerate(row, start=1):
                lookup_r, lookup_c = self.merged_map.get((sheet_name, r_idx, c_idx), (r_idx, c_idx))
                real_cell = sheet.cell(row=lookup_r, column=lookup_c)
                
                row_data.append(real_cell.value)
                
                style_info = {
                    'bold': bool(real_cell.font and real_cell.font.bold),
                    'font_size': real_cell.font.size if real_cell.font else 11,
                    'alignment': real_cell.alignment.horizontal if real_cell.alignment else None,
                    'is_merged': (sheet_name, r_idx, c_idx) in self.merged_map
                }
                row_styles.append(style_info)
            
            data_rows.append(row_data)
            style_grid.append(row_styles)

        return pd.DataFrame(data_rows), style_grid

class CellClassifier:
    """Classifies cells as headers vs data using multiple signals [web:1][web:2]."""
    def __init__(self, config: QueryEngineConfig):
        self.config = config
    
    def classify_cell(self, value: Any, style: Dict, row_idx: int, 
                     col_idx: int, row_ pd.Series, col_ pd.Series) -> CellType:
        """Classifies a single cell using style, position, and context."""
        if pd.isna(value) or str(value).strip() == "":
            return CellType.EMPTY
        
        # Style-based signals
        is_bold = style.get('bold', False)
        font_size = style.get('font_size', 11)
        is_merged = style.get('is_merged', False)
        alignment = style.get('alignment', None)
        
        # Content-based signals
        is_text = isinstance(value, str)
        is_numeric = isinstance(value, (int, float)) and not isinstance(value, bool)
        
        # Context-based signals
        row_has_mostly_text = self._row_has_mostly_text(row_data)
        col_has_mostly_numbers = self._col_has_mostly_numbers(col_data)
        
        # Header scoring [web:2][web:7]
        header_score = 0.0
        if is_bold: header_score += 0.3
        if font_size > 11: header_score += 0.2
        if is_merged: header_score += 0.15
        if is_text: header_score += 0.1
        if row_has_mostly_text: header_score += 0.15
        if alignment == 'center': header_score += 0.1
        
        # Classification logic
        if header_score >= 0.5:
            if col_idx <= 2 and not col_has_mostly_numbers:
                return CellType.HEADER_VERTICAL
            return CellType.HEADER_HORIZONTAL
        
        if is_numeric and col_has_mostly_numbers:
            return CellType.DATA_VALUE
        
        if is_text and col_idx == 0:
            return CellType.INDEX
        
        return CellType.DATA_VALUE
    
    def _row_has_mostly_text(self, row: pd.Series) -> bool:
        """Check if row contains mostly text values."""
        clean = row.dropna()
        if len(clean) == 0: return False
        text_count = sum(isinstance(x, str) for x in clean)
        return (text_count / len(clean)) >= 0.6
    
    def _col_has_mostly_numbers(self, col: pd.Series) -> bool:
        """Check if column contains mostly numeric values."""
        clean = col.dropna()
        if len(clean) == 0: return False
        num_count = sum(isinstance(x, (int, float)) and not isinstance(x, bool) for x in clean)
        return (num_count / len(clean)) >= 0.6

class AdvancedStructureDetector:
    """Enhanced table detection with multi-level header support [web:1][web:6]."""
    def __init__(self, config: QueryEngineConfig):
        self.config = config
        self.classifier = CellClassifier(config)

    def detect_tables(self, df: pd.DataFrame, style_grid: List[List[Dict]]) -> List[Dict]:
        """Detects multiple tables on a sheet with precise boundaries."""
        tables = []
        i = 0
        max_rows = len(df)

        while i < max_rows:
            # Skip empty rows
            if df.iloc[i].isna().all():
                i += 1
                continue

            # Detect header block
            header_start = i
            header_end = self._find_header_end(df, style_grid, i)
            
            if header_end == header_start:
                i += 1
                continue
            
            # Detect data block
            data_start = header_end
            data_end = self._find_data_end(df, data_start)
            
            if data_end > data_start:
                # Detect vertical headers (row labels)
                vertical_header_cols = self._detect_vertical_headers(
                    df.iloc[data_start:data_end], 
                    [style_grid[r] for r in range(data_start, data_end)]
                )
                
                tables.append({
                    'header_range': (header_start, header_end),
                    'data_range': (data_start, data_end),
                    'vertical_header_cols': vertical_header_cols,
                    'name': self._infer_table_name(df, header_start, header_end)
                })
                i = data_end
            else:
                i += 1
        
        return tables

    def _find_header_end(self, df: pd.DataFrame, style_grid: List[List[Dict]], start: int) -> int:
        """Find the end of multi-level header rows [web:2]."""
        end = start
        max_rows = min(start + self.config.max_header_rows, len(df))
        
        for r in range(start, max_rows):
            row_vals = df.iloc[r]
            row_styles = style_grid[r] if r < len(style_grid) else []
            
            if self._is_header_row(row_vals, row_styles, df):
                end = r + 1
            else:
                # Check if it's the start of data
                if self._is_data_row(row_vals):
                    break
                # Might be a separator row
                elif row_vals.isna().all():
                    continue
                else:
                    end = r + 1
        
        return end
    
    def _is_header_row(self, row: pd.Series, styles: List[Dict], df: pd.DataFrame) -> bool:
        """Determine if row is a header using multiple signals [web:2][web:7]."""
        clean_row = row.dropna()
        if len(clean_row) == 0: return False
        
        # Signal 1: Text ratio
        text_count = sum(isinstance(x, str) for x in clean_row)
        text_ratio = text_count / len(clean_row)
        
        # Signal 2: Bold ratio
        bold_count = sum(1 for idx, style in enumerate(styles) 
                        if idx < len(row) and pd.notna(row.iloc[idx]) and style.get('bold', False))
        bold_ratio = bold_count / len(clean_row) if len(clean_row) > 0 else 0
        
        # Signal 3: Merged cells
        merged_count = sum(1 for style in styles if style.get('is_merged', False))
        merged_ratio = merged_count / len(styles) if len(styles) > 0 else 0
        
        # Signal 4: Font size
        avg_font_size = np.mean([s.get('font_size', 11) for s in styles])
        font_boost = 0.1 if avg_font_size > 11 else 0
        
        # Composite score
        header_score = (text_ratio * 0.4) + (bold_ratio * 0.3) + (merged_ratio * 0.2) + font_boost
        
        return header_score >= self.config.header_text_ratio_threshold
    
    def _is_data_row(self, row: pd.Series) -> bool:
        """Check if row contains data values."""
        clean_row = row.dropna()
        if len(clean_row) == 0: return False
        
        num_count = sum(isinstance(x, (int, float)) and not isinstance(x, bool) for x in clean_row)
        return num_count >= self.config.min_numeric_cells_for_data_row
    
    def _find_data_end(self, df: pd.DataFrame, start: int) -> int:
        """Find the end of data block using empty row detection."""
        end = start
        empty_counter = 0
        max_rows = len(df)
        
        for r in range(start, max_rows):
            if df.iloc[r].isna().all():
                empty_counter += 1
                if empty_counter >= self.config.max_empty_rows_between_tables:
                    break
            else:
                empty_counter = 0
                end = r + 1
        
        return end
    
    def _detect_vertical_headers(self, data_block: pd.DataFrame, 
                                 style_block: List[List[Dict]]) -> List[int]:
        """Detect which columns are vertical headers (row labels) [web:1]."""
        vertical_cols = []
        
        for col_idx in range(min(3, len(data_block.columns))):  # Check first 3 columns
            col_data = data_block.iloc[:, col_idx]
            col_styles = [row[col_idx] if col_idx < len(row) else {} for row in style_block]
            
            # Text dominant
            text_ratio = sum(isinstance(x, str) for x in col_data.dropna()) / max(len(col_data.dropna()), 1)
            
            # Bold cells
            bold_count = sum(1 for s in col_styles if s.get('bold', False))
            bold_ratio = bold_count / max(len(col_styles), 1)
            
            # Indentation detection (for hierarchical row labels)
            has_indentation = any(isinstance(v, str) and (v.startswith('  ') or v.startswith('\t')) 
                                 for v in col_data if pd.notna(v))
            
            if text_ratio >= 0.7 or (text_ratio >= 0.5 and bold_ratio >= 0.3) or has_indentation:
                vertical_cols.append(col_idx)
        
        return vertical_cols if vertical_cols else [0]  # Default to first column
    
    def _infer_table_name(self, df: pd.DataFrame, header_start: int, header_end: int) -> str:
        """Try to infer a meaningful table name."""
        # Check rows above header for title
        for r in range(max(0, header_start - 3), header_start):
            row = df.iloc[r]
            non_empty = row.dropna()
            if len(non_empty) == 1:  # Single cell might be title
                return str(non_empty.iloc[0])
        
        # Use first header cell as fallback
        first_header = df.iloc[header_start].dropna()
        return str(first_header.iloc[0]) if len(first_header) > 0 else "Table"

# ==========================================
# 4. Main Engine
# ==========================================

class FinancialExcelEngine:
    """Production-grade Financial Excel Query Engine."""
    def __init__(self, file_path: str, config: QueryEngineConfig):
        self.file_path = file_path
        self.config = config
        self.records = []
        self.vector_index = {}
        
        self.merge_handler = MergedCellHandler(file_path)
        self.detector = AdvancedStructureDetector(config)
        
        if config.semantic_backend == 'openai':
            if not config.openai_api_key:
                raise ValueError("OpenAI API Key missing. Set OPENAI_API_KEY environment variable.")
            self.matcher = OpenAISemanticMatcher(config.openai_api_key, config.openai_model)
        else:
            self.matcher = BasicSemanticMatcher()

        print("🔍 Parsing file structure...")
        self._ingest_file()
        
        if config.semantic_backend == 'openai':
            print("🧠 Generating embeddings...")
            self._build_embeddings()
        
        print(f"✅ Engine Ready. Indexed {len(self.records)} data points across {len(set(r['sheet'] for r in self.records))} sheets.")

    def _ingest_file(self):
        """Parse all sheets and extract structured data with multi-level headers."""
        wb = load_workbook(self.file_path, read_only=True)
        
        for sheet_name in wb.sheetnames:
            df, style_grid = self.merge_handler.get_sheet_data_and_styles(sheet_name)
            tables = self.detector.detect_tables(df, style_grid)
            
            print(f"  📊 {sheet_name}: Found {len(tables)} table(s)")
            
            for tbl_idx, tbl in enumerate(tables):
                self._process_table(df, style_grid, sheet_name, tbl, tbl_idx)

    def _process_table(self, df: pd.DataFrame, style_grid: List[List[Dict]], 
                      sheet_name: str, table: Dict, table_idx: int):
        """Process a single table and extract all data points."""
        h_start, h_end = table['header_range']
        d_start, d_end = table['data_range']
        v_header_cols = table['vertical_header_cols']
        
        # Build horizontal header paths (multi-level)
        header_block = df.iloc[h_start:h_end]
        col_paths = self._build_column_paths(header_block)
        
        # Process data rows
        for r_idx in range(d_start, d_end):
            row_data = df.iloc[r_idx]
            
            # Build vertical header path (row labels)
            row_path = self._build_row_path(row_data, v_header_cols)
            
            if not row_path:
                continue
            
            # Extract values from non-header columns
            for c_idx, val in enumerate(row_data):
                if c_idx in v_header_cols or pd.isna(val) or str(val).strip() == "":
                    continue
                
                # Combine row path + column path
                full_path = row_path + col_paths[c_idx]
                
                record = {
                    'sheet': sheet_name,
                    'row': r_idx,
                    'col': c_idx,
                    'value': val,
                    'header_path': full_path,
                    'searchable_text': " ".join(full_path),
                    'type': self._get_value_type(val),
                    'table_name': table.get('name', f'Table{table_idx+1}')
                }
                self.records.append(record)
    
    def _build_column_paths(self, header_block: pd.DataFrame) -> List[List[str]]:
        """Build hierarchical paths for each column from multi-level headers [web:2]."""
        col_paths = []
        header_block_ffill = header_block.ffill(axis=1)  # Forward fill merged cells
        
        for c in range(len(header_block.columns)):
            col_headers = header_block_ffill.iloc[:, c].tolist()
            
            # Clean and deduplicate while preserving order
            clean_path = []
            for h in col_headers:
                h_str = str(h).strip()
                if pd.notna(h) and h_str != "" and (not clean_path or h_str != clean_path[-1]):
                    clean_path.append(h_str)
            
            col_paths.append(clean_path)
        
        return col_paths
    
    def _build_row_path(self, row_ pd.Series, v_header_cols: List[int]) -> List[str]:
        """Build hierarchical path from vertical headers (supports indented hierarchies)."""
        path = []
        
        for col_idx in v_header_cols:
            if col_idx < len(row_data):
                val = row_data.iloc[col_idx]
                if pd.notna(val):
                    val_str = str(val).strip()
                    if val_str:
                        # Handle indentation (indicates hierarchy level)
                        val_clean = val_str.lstrip()
                        indent_level = len(val_str) - len(val_clean)
                        
                        # Remove previous path elements if indented (new hierarchy level)
                        if indent_level > 0 and len(path) > 0:
                            path = path[:-1]
                        
                        path.append(val_clean)
        
        return path
    
    def _build_embeddings(self, batch_size=100):
        """Build vector index for semantic search."""
        unique_texts = list(set(r['searchable_text'] for r in self.records))
        
        for i in range(0, len(unique_texts), batch_size):
            batch = unique_texts[i:i+batch_size]
            try:
                vectors = self.matcher.embed_batch(batch)
                for text, vec in zip(batch, vectors):
                    self.vector_index[text] = vec
            except Exception as e:
                print(f"⚠️  Warning: Batch {i//batch_size + 1} embedding failed: {e}")

    def _get_value_type(self, val: Any) -> str:
        """Classify value type."""
        if isinstance(val, bool):
            return "boolean"
        if isinstance(val, (int, float)):
            # Check for percentage
            if isinstance(val, float) and 0 <= abs(val) <= 1:
                return "percentage"
            return "number"
        if isinstance(val, str):
            # Check for currency symbols
            if any(sym in val for sym in ['$', '€', '£', '¥']):
                return "currency"
            return "text"
        return "unknown"
    
    def _extract_critical_tokens(self, query: str) -> Set[str]:
        """Identify tokens that MUST exist (Years, Quarters, specific terms)."""
        tokens = set()
        query_lower = query.lower()
        parts = query_lower.split()
        
        for p in parts:
            clean_p = p.strip()
            # Years (1900-2099)
            if re.match(r'^(19|20)\d{2}$', clean_p):
                tokens.add(clean_p)
            # Quarters
            elif re.match(r'^q[1-4]$', clean_p):
                tokens.add(clean_p)
            # Months
            elif clean_p in ['january', 'february', 'march', 'april', 'may', 'june',
                           'july', 'august', 'september', 'october', 'november', 'december',
                           'jan', 'feb', 'mar', 'apr', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']:
                tokens.add(clean_p)
            # Financial modifiers
            elif clean_p in ['total', 'net', 'gross', 'operating', 'ebitda', 'ebit', 
                           'consolidated', 'adjusted', 'actual', 'budget', 'forecast']:
                tokens.add(clean_p)
        
        return tokens

    def query(self, question: str, top_k: int = 5, require_exact_tokens: bool = True) -> List[SearchResult]:
        """
        Precision Query with exact match prioritization and hybrid scoring.
        
        Args:
            question: Natural language query
            top_k: Number of results to return
            require_exact_tokens: If True, strictly enforce critical token matching
        """
        query_lower = question.lower()
        query_tokens = set(query_lower.split())
        critical_tokens = self._extract_critical_tokens(question)
        
        # Detect expected value type from query
        prefer_number = any(kw in query_lower for kw in 
                          ['how much', 'total', 'cost', 'revenue', 'profit', 'sum', 'amount', 'value'])
        
        scored_results = []
        q_vec = None
        
        if self.config.semantic_backend == 'openai':
            try:
                q_vec = self.matcher.embed_batch([question])[0]
            except Exception as e:
                print(f"⚠️  Warning: Query embedding failed: {e}")

        for r in self.records:
            target_text = r['searchable_text'].lower()
            
            # === A. EXACT MATCH CHECK (HIGHEST PRIORITY) ===
            is_exact = query_lower in target_text
            
            # === B. CONSTRAINT CHECK ===
            constraint_penalty = 1.0
            if critical_tokens:
                missing_critical = [t for t in critical_tokens if t not in target_text]
                if missing_critical:
                    if require_exact_tokens:
                        constraint_penalty = 0.05  # Severe penalty
                    else:
                        constraint_penalty = 0.4  # Moderate penalty
            
            # === C. SEMANTIC SIMILARITY ===
            semantic_score = 0.0
            if self.config.semantic_backend == 'openai' and q_vec:
                target_vec = self.vector_index.get(r['searchable_text'])
                if target_vec:
                    semantic_score = self.matcher.cosine_similarity(q_vec, target_vec)
            else:
                semantic_score = self.matcher.calculate_similarity(question, r['searchable_text'])
            
            # === D. KEYWORD COVERAGE ===
            target_tokens = set(target_text.split())
            keyword_coverage = 0.0
            if query_tokens:
                intersection = query_tokens.intersection(target_tokens)
                keyword_coverage = len(intersection) / len(query_tokens)
            
            # === E. WEIGHTED BLEND ===
            # 60% Semantic (context) + 40% Keywords (precision)
            base_score = (semantic_score * 0.6) + (keyword_coverage * 0.4)
            
            # === F. EXACT MATCH BOOST ===
            if is_exact:
                base_score = min(0.99, base_score + self.config.exact_match_boost)
            
            # Apply constraint penalty
            final_score = base_score * constraint_penalty
            
            # === G. TYPE VALIDATION ===
            if prefer_number and r['type'] not in ['number', 'percentage', 'currency']:
                final_score *= 0.75
            
            if final_score >= self.config.min_confidence:
                scored_results.append((final_score, r, is_exact))
        
        # Sort: Exact matches first, then by score
        scored_results.sort(key=lambda x: (not x[2], -x[0]))
        
        # Build results
        final_output = []
        for score, r, is_exact in scored_results[:top_k]:
            res = SearchResult(
                value=r['value'],
                confidence=score,
                sheet_name=r['sheet'],
                row=r['row'],
                col=r['col'],
                header_path=r['header_path'],
                context_text=r['searchable_text'],
                value_type=r['type'],
                is_exact_match=is_exact
            )
            final_output.append(res)
            
        return final_output
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get engine statistics."""
        sheets = set(r['sheet'] for r in self.records)
        tables = set((r['sheet'], r.get('table_name', '')) for r in self.records)
        
        return {
            'total_records': len(self.records),
            'sheets': len(sheets),
            'tables': len(tables),
            'value_types': {vtype: sum(1 for r in self.records if r['type'] == vtype) 
                          for vtype in set(r['type'] for r in self.records)}
        }

# ==========================================
# 5. Example Usage & Testing
# ==========================================
if __name__ == "__main__":
    # Demo Setup
    dummy_file = "financial_demo_advanced.xlsx"
    
    if not os.path.exists(dummy_file):
        print("📝 Creating advanced demo financial file...")
        
        # Create a realistic multi-level header P&L
        with pd.ExcelWriter(dummy_file, engine='openpyxl') as writer:
            # Sheet 1: P&L with multi-level headers
            data = {
                'Line Item': ['Revenue', '  Product Sales', '  Service Revenue', 'Total Revenue',
                             'Expenses', '  COGS', '  Operating Expenses', 'Total Expenses',
                             'Net Income'],
                'Q1 2023': [100, 60, 40, 100, 70, 40, 30, 70, 30],
                'Q2 2023': [120, 70, 50, 120, 80, 45, 35, 80, 40],
                'Q3 2023': [110, 65, 45, 110, 75, 42, 33, 75, 35],
                'Q4 2023': [130, 75, 55, 130, 85, 48, 37, 85, 45]
            }
            df = pd.DataFrame(data)
            df.to_excel(writer, sheet_name='P&L Statement', index=False)
            
            # Sheet 2: Balance Sheet
            bs_data = {
                'Account': ['Assets', '  Current Assets', '    Cash', '    Receivables',
                           '  Fixed Assets', 'Total Assets',
                           'Liabilities', '  Current Liabilities', '  Long-term Debt',
                           'Total Liabilities', 'Equity'],
                '2022': [500, 200, 100, 100, 300, 500, 200, 100, 100, 200, 300],
                '2023': [600, 250, 120, 130, 350, 600, 220, 110, 110, 220, 380]
            }
            df_bs = pd.DataFrame(bs_data)
            df_bs.to_excel(writer, sheet_name='Balance Sheet', index=False)

    # Configuration
    api_key = os.getenv("OPENAI_API_KEY", "")
    
    config = QueryEngineConfig(
        semantic_backend='openai' if api_key else 'basic',
        openai_api_key=api_key if api_key else None,
        min_confidence=0.45,
        exact_match_boost=0.30
    )

    try:
        print("\n" + "="*60)
        print("🚀 Initializing Production-Grade Financial Excel Engine")
        print("="*60 + "\n")
        
        engine = FinancialExcelEngine(dummy_file, config)
        
        # Display statistics
        stats = engine.get_statistics()
        print(f"\n📊 Engine Statistics:")
        print(f"   Total Records: {stats['total_records']}")
        print(f"   Sheets: {stats['sheets']}")
        print(f"   Tables: {stats['tables']}")
        print(f"   Value Types: {stats['value_types']}")
        
        # Test Queries
        print("\n" + "="*60)
        print("🔎 Running Test Queries")
        print("="*60)
        
        test_queries = [
            ("Revenue Q1 2023", "Should find exact Q1 2023 revenue"),
            ("Net Income Q4 2023", "Should find Q4 net income with exact match"),
            ("Total Assets 2023", "Should find 2023 total assets"),
            ("Service Revenue second quarter 2023", "Should match Q2 service revenue"),
            ("Cash position in 2022", "Should find 2022 cash"),
            ("What is the operating expenses?", "Should handle question format"),
        ]
        
        for query, description in test_queries:
            print(f"\n📌 Query: '{query}'")
            print(f"   Expected: {description}")
            results = engine.query(query, top_k=3)
            
            if not results:
                print("   ❌ No results found")
            else:
                for idx, r in enumerate(results, 1):
                    exact_marker = "🎯 EXACT" if r.is_exact_match else ""
                    path_str = " > ".join(r.header_path)
                    print(f"   [{idx}] {exact_marker}")
                    print(f"       Value: {r.value} ({r.value_type})")
                    print(f"       Confidence: {r.confidence:.3f}")
                    print(f"       Path: {path_str}")
                    print(f"       Location: {r.sheet_name}!R{r.row+1}C{r.col+1}")
                
    except Exception as e:
        print(f"\n❌ Error: {e}")
        if "API" in str(e):
            print("\n💡 Tip: Set OPENAI_API_KEY environment variable for semantic search.")
            print("   Falling back to basic keyword matching...")
            
            # Retry with basic backend
            config.semantic_backend = 'basic'
            engine = FinancialExcelEngine(dummy_file, config)
            print("\n✅ Engine initialized with basic keyword matching")
