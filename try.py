"""
Financial Excel Query Engine V7 (Complete Implementation)
---------------------------------------------------------
Enterprise-style table extraction with:

- Connected component table detection
- Multi-level header reconstruction (manual forward/backward fill)
- Bold formatting analysis
- Precise spatial table boundaries
- Basic or OpenAI-based semantic similarity
- No abstract methods, no NotImplementedError, all methods implemented
"""

import os
import re
import warnings
from dataclasses import dataclass
from typing import List, Dict, Tuple, Any, Optional, Literal, Set

import numpy as np
import pandas as pd
from openpyxl import load_workbook
from tenacity import retry, stop_after_attempt, wait_exponential

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


# ==========================================
# 1. Data Structures
# ==========================================

@dataclass
class TableRegion:
    sheet_name: str
    top_row: int
    bottom_row: int
    left_col: int
    right_col: int
    header_rows: int
    row_header_col: int
    density_score: float


@dataclass
class CellClassification:
    row: int
    col: int
    value: Any
    role: str  # 'table_header', 'row_header', 'data_value', 'annotation', 'empty'
    confidence: float
    is_bold: bool


@dataclass
class QueryEngineConfig:
    min_table_density: float = 0.4
    max_table_gap_cols: int = 3
    header_text_ratio: float = 0.7
    min_confidence: float = 0.5
    semantic_backend: Literal["basic", "openai"] = "basic"
    openai_api_key: Optional[str] = None
    openai_model: str = "text-embedding-3-small"


@dataclass
class SearchResult:
    value: Any
    confidence: float
    sheet_name: str
    row: int
    col: int
    row_header: str
    col_headers: List[str]
    full_path: str
    table_region: TableRegion


# ==========================================
# 2. Semantic Matchers (fully implemented)
# ==========================================

class BasicSemanticMatcher:
    """Simple keyword-based semantic matcher."""

    def normalize(self, text: str) -> Set[str]:
        text = str(text).lower().strip()
        text = re.sub(r"[^\w\s]", "", text)
        return set(text.split())

    def calculate_similarity(self, query: str, target: str) -> float:
        q = self.normalize(query)
        t = self.normalize(target)
        if not q or not t:
            return 0.0
        intersection = len(q & t)
        union = len(q | t)
        score = intersection / union if union > 0 else 0.0
        if query.lower() in target.lower():
            score = max(score, 0.9)
        return score


class OpenAISemanticMatcher(BasicSemanticMatcher):
    """
    OpenAI-based semantic matcher.

    Extends BasicSemanticMatcher so it ALSO has calculate_similarity.
    calculate_similarity will use embeddings, but if anything fails it
    cleanly falls back to the basic Jaccard similarity.
    """

    def __init__(self, api_key: str, model_name: str):
        from openai import OpenAI  # imported here to avoid hard dependency if unused
        self.client = OpenAI(api_key=api_key)
        self.model_name = model_name
        self._emb_cache: Dict[str, List[float]] = {}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _embed_batch(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        clean = [t.replace("\n", " ")[:8000] for t in texts]
        resp = self.client.embeddings.create(input=clean, model=self.model_name)
        return [d.embedding for d in resp.data]

    def _embed(self, text: str) -> List[float]:
        """Get cached embedding for text, computing if needed."""
        if text in self._emb_cache:
            return self._emb_cache[text]
        emb = self._embed_batch([text])[0]
        self._emb_cache[text] = emb
        return emb

    @staticmethod
    def _cosine(a: List[float], b: List[float]) -> float:
        va = np.array(a)
        vb = np.array(b)
        na = np.linalg.norm(va)
        nb = np.linalg.norm(vb)
        if na == 0 or nb == 0:
            return 0.0
        return float(np.dot(va, vb) / (na * nb))

    def calculate_similarity(self, query: str, target: str) -> float:
        """
        Compute semantic similarity between query and target via embeddings.
        Falls back to BasicSemanticMatcher if anything goes wrong.
        """
        try:
            q_vec = self._embed(query)
            t_vec = self._embed(target)
            return self._cosine(q_vec, t_vec)
        except Exception:
            # Fallback: use bag-of-words similarity
            return super().calculate_similarity(query, target)


# ==========================================
# 3. Excel Loader
# ==========================================

class ExcelLoader:
    """Load Excel data and bold style grid."""

    def __init__(self, file_path: str):
        self.wb = load_workbook(file_path, data_only=True)
        self.file_path = file_path

    def load_sheet(self, sheet_name: str) -> Tuple[pd.DataFrame, List[List[bool]]]:
        sheet = self.wb[sheet_name]
        max_cols = sheet.max_column
         List[List[Any]] = []
        bold_grid: List[List[bool]] = []

        for row in sheet.iter_rows():
            row_vals: List[Any] = []
            row_bolds: List[bool] = []
            for cell in row:
                row_vals.append(cell.value)
                row_bolds.append(bool(cell.font and cell.font.bold))
            while len(row_vals) < max_cols:
                row_vals.append(None)
                row_bolds.append(False)
            data.append(row_vals[:max_cols])
            bold_grid.append(row_bolds[:max_cols])

        df = pd.DataFrame(data)
        return df, bold_grid


# ==========================================
# 4. Table Detector
# ==========================================

class TableDetector:
    """Detect tables using connected component analysis."""

    def __init__(self, config: QueryEngineConfig):
        self.config = config

    def detect_tables(self, df: pd.DataFrame, bold_grid: List[List[bool]]) -> List[TableRegion]:
        occupancy = self._create_occupancy_grid(df, bold_grid)
        components = self._find_connected_components(occupancy)
        tables: List[TableRegion] = []
        for bounds in components:
            table = self._refine_table(df, bold_grid, bounds)
            if table.density_score >= self.config.min_table_density:
                tables.append(table)
        return tables

    def _create_occupancy_grid(self, df: pd.DataFrame, bold_grid: List[List[bool]]) -> np.ndarray:
        rows, cols = df.shape
        grid = np.zeros((rows, cols), dtype=int)
        for r in range(rows):
            for c in range(cols):
                val = df.iat[r, c]
                is_bold = r < len(bold_grid) and c < len(bold_grid[r]) and bold_grid[r][c]
                if self._is_relevant_cell(val) or is_bold:
                    grid[r, c] = 1
        return grid

    def _is_relevant_cell(self, val: Any) -> bool:
        if pd.isna(val):
            return False
        if isinstance(val, (int, float)):
            return True
        if isinstance(val, str):
            cleaned = val.strip()
            if len(cleaned.split()) <= 4 and len(cleaned) <= 40:
                return True
        return False

    def _find_connected_components(self, occupancy: np.ndarray) -> List[Tuple[int, int, int, int]]:
        seen = np.zeros_like(occupancy, dtype=bool)
        components: List[Tuple[int, int, int, int]] = []
        rows, cols = occupancy.shape
        for r in range(rows):
            for c in range(cols):
                if occupancy[r, c] == 1 and not seen[r, c]:
                    bounds = self._flood_fill(occupancy, seen, r, c)
                    components.append(bounds)
        return components

    def _flood_fill(
        self,
        occupancy: np.ndarray,
        seen: np.ndarray,
        start_r: int,
        start_c: int,
    ) -> Tuple[int, int, int, int]:
        stack = [(start_r, start_c)]
        min_r = max_r = start_r
        min_c = max_c = start_c
        rows, cols = occupancy.shape

        while stack:
            r, c = stack.pop()
            if r < 0 or r >= rows or c < 0 or c >= cols:
                continue
            if seen[r, c] or occupancy[r, c] == 0:
                continue
            seen[r, c] = True
            min_r = min(min_r, r)
            max_r = max(max_r, r)
            min_c = min(min_c, c)
            max_c = max(max_c, c)
            for dr in [-1, 0, 1]:
                for dc in [-1, 0, 1]:
                    if dr == 0 and dc == 0:
                        continue
                    stack.append((r + dr, c + dc))

        return min_r, min_c, max_r, max_c

    def _refine_table(
        self,
        df: pd.DataFrame,
        bold_grid: List[List[bool]],
        bounds: Tuple[int, int, int, int],
    ) -> TableRegion:
        min_r, min_c, max_r, max_c = bounds
        region = df.iloc[min_r:max_r + 1, min_c:max_c + 1]
        cells_count = region.size
        non_na = region.notna().sum().sum()
        density = non_na / cells_count if cells_count else 0.0

        header_rows = self._detect_header_rows(df, bold_grid, min_r, min_c, max_c)
        data_top = min_r + header_rows
        row_header_col = self._detect_row_header_col(df, data_top, max_r, min_c, max_c)

        return TableRegion(
            sheet_name="",
            top_row=min_r,
            bottom_row=max_r,
            left_col=min_c,
            right_col=max_c,
            header_rows=header_rows,
            row_header_col=row_header_col,
            density_score=density,
        )

    def _detect_header_rows(
        self,
        df: pd.DataFrame,
        bold_grid: List[List[bool]],
        top: int,
        left: int,
        right: int,
    ) -> int:
        max_header_rows = 5
        best_rows = 1
        n_rows = len(df)
        for rows_count in range(1, max_header_rows + 1):
            if top + rows_count > n_rows:
                break
            text_cells = 0
            bold_cells = 0
            total_cells = 0
            for r in range(top, top + rows_count):
                for c in range(left, min(right + 1, len(df.columns))):
                    total_cells += 1
                    val = df.iat[r, c]
                    if isinstance(val, str):
                        text_cells += 1
                    if r < len(bold_grid) and c < len(bold_grid[r]) and bold_grid[r][c]:
                        bold_cells += 1
            text_ratio = text_cells / total_cells if total_cells else 0
            bold_ratio = bold_cells / total_cells if total_cells else 0
            score = text_ratio + 0.3 * bold_ratio
            if score > 0.6:
                best_rows = rows_count
        return best_rows

    def _detect_row_header_col(
        self,
        df: pd.DataFrame,
        data_top: int,
        data_bottom: int,
        left: int,
        right: int,
    ) -> int:
        best_col = left
        best_text_ratio = 0.0
        if data_bottom < data_top:
            return best_col
        for c in range(left, min(right + 1, len(df.columns))):
            col_vals = df.iloc[data_top:data_bottom + 1, c]
            non_na = col_vals.notna().sum()
            if non_na == 0:
                continue
            text_count = col_vals.apply(lambda x: isinstance(x, str)).sum()
            ratio = text_count / non_na
            if ratio > best_text_ratio:
                best_text_ratio = ratio
                best_col = c
        return best_col


# ==========================================
# 5. Header Reconstruction
# ==========================================

class HeaderReconstructor:
    """Build column header paths inside a table region."""

    def reconstruct_column_headers(self, df: pd.DataFrame, table: TableRegion) -> List[List[str]]:
        header_top = table.top_row
        header_bottom = min(table.top_row + table.header_rows, len(df))

        header_ List[List[Any]] = []
        for r in range(header_top, header_bottom):
            row_ List[Any] = []
            for c in range(table.left_col, table.right_col + 1):
                if r < len(df) and c < len(df.columns):
                    row_data.append(df.iat[r, c])
                else:
                    row_data.append(None)
            header_data.append(row_data)

        # Forward fill horizontally
        for r_idx in range(len(header_data)):
            last_val = None
            for c_idx, val in enumerate(header_data[r_idx]):
                if pd.notna(val):
                    last_val = val
                elif last_val is not None:
                    header_data[r_idx][c_idx] = last_val

        # Backward fill horizontally
        for r_idx in range(len(header_data)):
            next_val = None
            for c_idx in range(len(header_data[r_idx]) - 1, -1, -1):
                val = header_data[r_idx][c_idx]
                if pd.notna(val):
                    next_val = val
                elif next_val is not None:
                    header_data[r_idx][c_idx] = next_val

        col_paths: List[List[str]] = []
        num_cols = table.right_col - table.left_col + 1
        for col_idx in range(num_cols):
            path: List[str] = []
            for row_idx in range(len(header_data)):
                if col_idx < len(header_data[row_idx]):
                    val = header_data[row_idx][col_idx]
                    if pd.notna(val):
                        s = str(val).strip()
                        if s:
                            path.append(s)
            col_paths.append(path)
        return col_paths


# ==========================================
# 6. Cell Classifier
# ==========================================

class CellClassifier:
    """Classify cells as headers, row headers, data values, or annotations."""

    def classify(
        self,
        df: pd.DataFrame,
        table: TableRegion,
        bold_grid: List[List[bool]],
    ) -> List[CellClassification]:
        out: List[CellClassification] = []
        for r in range(table.top_row, table.bottom_row + 1):
            for c in range(table.left_col, table.right_col + 1):
                if r >= len(df) or c >= len(df.columns):
                    continue
                val = df.iat[r, c]
                is_bold = r < len(bold_grid) and c < len(bold_grid[r]) and bold_grid[r][c]
                in_header = r < table.top_row + table.header_rows
                is_row_header_col = c == table.row_header_col
                role = self._classify_cell_role(val, in_header, is_row_header_col, is_bold)
                conf = 1.0 if role != "empty" else 0.0
                out.append(CellClassification(r, c, val, role, conf, is_bold))
        return out

    def _classify_cell_role(
        self,
        val: Any,
        in_header: bool,
        is_row_header_col: bool,
        is_bold: bool,
    ) -> str:
        if pd.isna(val):
            return "empty"
        if in_header:
            return "table_header"
        if is_row_header_col:
            return "row_header"
        if self._is_numeric_like(val):
            return "data_value"
        if isinstance(val, str) and len(val.split()) > 4:
            return "annotation"
        return "data_value" if is_bold else "annotation"

    def _is_numeric_like(self, val: Any) -> bool:
        if pd.isna(val):
            return False
        if isinstance(val, (int, float)):
            return True
        if isinstance(val, str):
            c = val.replace(",", "").replace("$", "").replace("%", "")
            c = c.replace("(", "").replace(")", "").strip()
            return bool(re.match(r"^-?\d+(\.\d+)?$", c))
        return False


# ==========================================
# 7. Data Indexer
# ==========================================

class EnterpriseDataIndexer:
    """Convert classified tables into a flat searchable index."""

    def __init__(self, config: QueryEngineConfig):
        self.config = config
        self.records: List[Dict[str, Any]] = []

    def build_index(
        self,
        df: pd.DataFrame,
        bold_grid: List[List[bool]],
        tables: List[TableRegion],
    ) -> None:
        reconstructor = HeaderReconstructor()
        classifier = CellClassifier()
        for table in tables:
            col_headers = reconstructor.reconstruct_column_headers(df, table)
            classified = classifier.classify(df, table, bold_grid)

            for cell in classified:
                if cell.role != "data_value":
                    continue
                r, c = cell.row, cell.col
                row_header_vals = [
                    cl.value for cl in classified
                    if cl.role == "row_header" and cl.row == r
                ]
                row_header = str(row_header_vals[0]) if row_header_vals else ""

                col_idx = c - table.left_col
                if table.row_header_col == table.left_col:
                    col_idx -= 1

                if 0 <= col_idx < len(col_headers):
                    col_path = col_headers[col_idx]
                else:
                    col_path = []

                full_path_list = [row_header] + col_path
                search_text = " ".join(full_path_list)

                self.records.append(
                    {
                        "value": cell.value,
                        "row": r,
                        "col": c,
                        "row_header": row_header,
                        "col_headers": col_path,
                        "full_path": full_path_list,
                        "search_text": search_text,
                        "table": table,
                    }
                )


# ==========================================
# 8. Main Engine
# ==========================================

class FinancialExcelEngineV7:
    """End-to-end financial Excel query engine."""

    def __init__(self, filepath: str, config: QueryEngineConfig = QueryEngineConfig()):
        self.filepath = filepath
        self.config = config
        self.loader = ExcelLoader(filepath)
        self.table_detector = TableDetector(config)
        self.indexer = EnterpriseDataIndexer(config)

        if config.semantic_backend == "openai":
            api_key = config.openai_api_key or os.getenv("OPENAI_API_KEY")
            if not api_key:
                raise ValueError("OPENAI_API_KEY is required for 'openai' backend")
            self.matcher: Any = OpenAISemanticMatcher(api_key, config.openai_model)
        else:
            self.matcher = BasicSemanticMatcher()

        print("🔍 Loading and indexing tables...")
        self._load_and_index()
        print(f"✅ Engine ready: {len(self.indexer.records)} values indexed")

    def _load_and_index(self) -> None:
        wb = load_workbook(self.filepath, data_only=True)
        for sheet_name in wb.sheetnames:
            try:
                df, bold_grid = self.loader.load_sheet(sheet_name)
                tables = self.table_detector.detect_tables(df, bold_grid)
                for t in tables:
                    t.sheet_name = sheet_name
                self.indexer.build_index(df, bold_grid, tables)
            except Exception as e:
                print(f"⚠️ Error processing sheet '{sheet_name}': {e}")

    def query(self, question: str, top_k: int = 5) -> List[SearchResult]:
        """Query over indexed records using the configured matcher."""
        q_tokens = set(question.lower().split())
        scored: List[Tuple[float, Dict[str, Any]]] = []

        for rec in self.indexer.records:
            sem_score = self.matcher.calculate_similarity(question, rec["search_text"])
            overlap = len(q_tokens & set(rec["search_text"].lower().split()))
            keyword_score = overlap / len(q_tokens) if q_tokens else 0.0
            score = 0.7 * sem_score + 0.3 * keyword_score
            if score >= self.config.min_confidence:
                scored.append((score, rec))

        scored.sort(key=lambda x: x[0], reverse=True)

        results: List[SearchResult] = []
        for s, rec in scored[:top_k]:
            results.append(
                SearchResult(
                    value=rec["value"],
                    confidence=s,
                    sheet_name=rec["table"].sheet_name,
                    row=rec["row"],
                    col=rec["col"],
                    row_header=rec["row_header"],
                    col_headers=rec["col_headers"],
                    full_path=" > ".join(rec["full_path"]),
                    table_region=rec["table"],
                )
            )
        return results


# ==========================================
# 9. Example Usage
# ==========================================

if __name__ == "__main__":
    demo_file = "enterprise_demo.xlsx"

    if not os.path.exists(demo_file):
        print("📁 Creating demo Excel file...")
        df1 = pd.DataFrame(
            {
                "Metric": ["Revenue", "EBITDA", "Net Income"],
                "2023 Q1": [1000, 250, 180],
                "2023 Q2": [1100, 275, 200],
            }
        )
        df2 = pd.DataFrame(
            {
                "Account": ["Cash", "Receivables", "Inventory"],
                "Balance": [500, 300, 200],
                "Note": ["See note 1", "Footnote 2", "See note 3"],
            }
        )
        with pd.ExcelWriter(demo_file, engine="openpyxl") as writer:
            df1.to_excel(writer, sheet_name="Financials", index=False)
            df2.to_excel(writer, sheet_name="BalanceSheet", index=False)
        print(f"✅ Created {demo_file}")

    cfg = QueryEngineConfig(
        semantic_backend="basic",  # switch to 'openai' when ready
        min_table_density=0.3,
        min_confidence=0.3,
    )
    engine = FinancialExcelEngineV7(demo_file, cfg)

    print("\n=== TEST QUERIES ===")
    for q in ["Revenue 2023", "Net Income Q2", "Cash balance", "EBITDA"]:
        print(f"\n🔍 Query: {q}")
        res = engine.query(q, top_k=3)
        if not res:
            print("  No results")
        for r in res:
            print(f"  Value: {r.value} | Path: {r.full_path} | Conf: {r.confidence:.3f}")
