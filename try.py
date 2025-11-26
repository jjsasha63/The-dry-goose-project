"""
CLEAN EXCEL QUERY ENGINE
Simple structure with:
- Linear flattening (straightforward, easy to debug)
- Hybrid search (BM25 + embeddings computed upfront)
- Smart exact matching (prefers exact, falls back to fuzzy)
"""

import openpyxl
from openpyxl.utils import get_column_letter
from typing import Dict, List, Tuple, Any, Optional, Set
from dataclasses import dataclass, field
import re
import openai
import numpy as np
from numpy.linalg import norm
from collections import Counter
from rank_bm25 import BM25Okapi


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class StructuredCell:
    """Cell with hierarchical context"""
    sheet: str
    cell_ref: str
    row: int
    col: int
    value: Any
    value_type: str
    row_headers: List[str] = field(default_factory=list)
    col_headers: List[str] = field(default_factory=list)
    full_context: str = ""
    search_tokens: List[str] = field(default_factory=list)


@dataclass
class QueryResult:
    """Query result with matches"""
    query: str
    result: Any
    matches: List[Dict[str, Any]]
    operation: str
    confidence: float = 1.0


# ============================================================================
# HELPERS
# ============================================================================

class CellTypeAnalyzer:
    """Simple cell type analysis"""
    
    @staticmethod
    def is_numeric_value(value: Any) -> bool:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return True
        if isinstance(value, str):
            try:
                float(value.replace(',', '').replace('$', '').replace('%', '').strip())
                return True
            except:
                return False
        return False
    
    @staticmethod
    def is_likely_header(value: Any) -> bool:
        if value is None or isinstance(value, bool):
            return False
        if CellTypeAnalyzer.is_numeric_value(value):
            return False
        if isinstance(value, str) and len(value) <= 100:
            return True
        return False
    
    @staticmethod
    def classify_value_type(value: Any) -> str:
        if value is None:
            return "empty"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, (int, float)):
            return "number"
        if isinstance(value, str):
            if '$' in value or '€' in value or '£' in value:
                return "currency"
            if '%' in value:
                return "percentage"
            if CellTypeAnalyzer.is_numeric_value(value):
                return "numeric_string"
            return "text"
        return "other"


class TextNormalizer:
    """Simple text normalization"""
    
    def __init__(self):
        self.abbreviations = {
            'q1': 'quarter 1', 'q2': 'quarter 2', 'q3': 'quarter 3', 'q4': 'quarter 4',
            'rev': 'revenue', 'exp': 'expense', 'avg': 'average', 'tot': 'total',
        }
    
    def normalize(self, text: str) -> str:
        if not isinstance(text, str):
            text = str(text)
        text = text.lower()
        text = re.sub(r'[^\w\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        words = text.split()
        return ' '.join([self.abbreviations.get(w, w) for w in words])
    
    def tokenize(self, text: str) -> List[str]:
        return self.normalize(text).split()


# ============================================================================
# LINEAR FLATTENER - SIMPLE AND CLEAR
# ============================================================================

class LinearFlattener:
    """
    Simple linear flattening:
    1. Detect header rows and columns
    2. Flatten data area cell by cell
    3. Build context for each cell
    """
    
    def __init__(self):
        self.analyzer = CellTypeAnalyzer()
        self.normalizer = TextNormalizer()
    
    def detect_header_rows(self, ws) -> List[int]:
        """Simple header row detection"""
        header_rows = []
        
        for row_num in range(1, min(10, ws.max_row + 1)):
            # Count text vs numbers in first 20 cells
            values = []
            for col in range(1, min(21, ws.max_column + 1)):
                val = ws.cell(row_num, col).value
                if val is not None:
                    values.append(val)
            
            if not values:
                continue
            
            # If mostly text, it's a header
            text_count = sum(1 for v in values if isinstance(v, str) and not self.analyzer.is_numeric_value(v))
            if text_count / len(values) > 0.6:
                header_rows.append(row_num)
            elif header_rows:  # Stop after first data row
                break
        
        return header_rows
    
    def detect_row_header_cols(self, ws, data_start_row: int) -> List[int]:
        """Simple row header column detection"""
        row_header_cols = []
        
        for col_num in range(1, min(6, ws.max_column + 1)):
            # Sample 5 data rows
            values = []
            for row in range(data_start_row, min(data_start_row + 5, ws.max_row + 1)):
                val = ws.cell(row, col_num).value
                if val is not None:
                    values.append(val)
            
            if not values:
                continue
            
            # If mostly text, it's a header column
            text_count = sum(1 for v in values if isinstance(v, str) and not self.analyzer.is_numeric_value(v))
            if text_count / len(values) > 0.6:
                row_header_cols.append(col_num)
            else:
                break
        
        return row_header_cols
    
    def get_col_headers(self, ws, col: int, header_rows: List[int]) -> List[str]:
        """Get column headers for a specific column"""
        headers = []
        for row in header_rows:
            val = ws.cell(row, col).value
            if val and self.analyzer.is_likely_header(val):
                headers.append(str(val).strip())
        return headers
    
    def get_row_headers(self, ws, row: int, row_header_cols: List[int]) -> List[str]:
        """Get row headers for a specific row"""
        headers = []
        for col in row_header_cols:
            val = ws.cell(row, col).value
            if val and self.analyzer.is_likely_header(val):
                headers.append(str(val).strip())
        return headers
    
    def build_context(self, sheet: str, row_headers: List[str], col_headers: List[str], value: Any) -> str:
        """Build searchable context string"""
        parts = [sheet]
        
        # Add row headers (exact + normalized)
        if row_headers:
            exact = " ".join(row_headers)
            parts.append(f"row {exact}")
            normalized = self.normalizer.normalize(exact)
            if exact.lower() != normalized:
                parts.append(f"row {normalized}")
        
        # Add column headers (exact + normalized)
        if col_headers:
            exact = " ".join(col_headers)
            parts.append(f"column {exact}")
            normalized = self.normalizer.normalize(exact)
            if exact.lower() != normalized:
                parts.append(f"column {normalized}")
        
        # Add value
        parts.append(f"value {value}")
        
        return " ".join(parts)
    
    def flatten_sheet(self, ws, ws_data, sheet_name: str, verbose: bool = True) -> List[StructuredCell]:
        """Linear flattening of one sheet"""
        
        # Step 1: Detect structure
        header_rows = self.detect_header_rows(ws)
        data_start_row = max(header_rows) + 1 if header_rows else 1
        row_header_cols = self.detect_row_header_cols(ws, data_start_row)
        data_start_col = max(row_header_cols) + 1 if row_header_cols else 1
        
        if verbose:
            print(f"  {sheet_name}: header rows {header_rows}, header cols {row_header_cols}")
        
        cells = []
        
        # Step 2: Iterate through data area
        for row in range(data_start_row, ws.max_row + 1):
            for col in range(data_start_col, ws.max_column + 1):
                
                # Get value
                value = ws_data.cell(row, col).value
                if value is None:
                    continue
                
                # Classify type
                value_type = self.analyzer.classify_value_type(value)
                
                # Get headers
                col_headers = self.get_col_headers(ws, col, header_rows)
                row_headers = self.get_row_headers(ws, row, row_header_cols)
                
                # Build context
                full_context = self.build_context(sheet_name, row_headers, col_headers, value)
                
                # Create cell
                cell = StructuredCell(
                    sheet=sheet_name,
                    cell_ref=f"{get_column_letter(col)}{row}",
                    row=row,
                    col=col,
                    value=value,
                    value_type=value_type,
                    row_headers=row_headers,
                    col_headers=col_headers,
                    full_context=full_context,
                    search_tokens=self.normalizer.tokenize(full_context)
                )
                
                cells.append(cell)
        
        if verbose:
            print(f"    → {len(cells)} cells")
        
        return cells
    
    def flatten_workbook(self, file_path: str, verbose: bool = True) -> List[StructuredCell]:
        """Flatten entire workbook"""
        import time
        start = time.time()
        
        if verbose:
            print(f"\nFlattening: {file_path}")
            print("="*80)
        
        # Load workbooks
        wb = openpyxl.load_workbook(file_path, data_only=False, read_only=True)
        wb_data = openpyxl.load_workbook(file_path, data_only=True, read_only=True)
        
        all_cells = []
        
        # Process each sheet
        for sheet_name in wb.sheetnames:
            cells = self.flatten_sheet(wb[sheet_name], wb_data[sheet_name], sheet_name, verbose)
            all_cells.extend(cells)
        
        wb.close()
        wb_data.close()
        
        elapsed = time.time() - start
        
        if verbose:
            print(f"\n✓ Flattened {len(all_cells)} cells in {elapsed:.1f}s")
            print("="*80)
        
        return all_cells


# ============================================================================
# HYBRID SEARCHER - BM25 + EMBEDDINGS
# ============================================================================

class HybridSearcher:
    """
    Hybrid search combining:
    - BM25 (keyword matching)
    - Embeddings (semantic understanding)
    - Exact matching (smart boosting)
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        openai.api_key = api_key
        self.normalizer = TextNormalizer()
        self.structured_ Optional[List[StructuredCell]] = None
        self.bm25: Optional[BM25Okapi] = None
        self.embeddings: Optional[np.ndarray] = None
    
    def build_indices(self, structured_ List[StructuredCell], verbose: bool = True):
        """Build BM25 and embedding indices"""
        self.structured_data = structured_data
        
        if verbose:
            print(f"\nBuilding search indices...")
        
        # BM25 index
        tokenized_corpus = [cell.search_tokens for cell in structured_data]
        self.bm25 = BM25Okapi(tokenized_corpus)
        
        if verbose:
            print(f"  ✓ BM25 index built")
            print(f"  Computing embeddings for {len(structured_data)} cells...")
        
        # Embedding index (batched)
        contexts = [cell.full_context for cell in structured_data]
        batch_size = 2048
        all_embeddings = []
        
        for i in range(0, len(contexts), batch_size):
            batch = contexts[i:i+batch_size]
            if verbose and len(contexts) > batch_size:
                print(f"    Batch {i//batch_size + 1}/{(len(contexts)-1)//batch_size + 1}")
            
            response = openai.embeddings.create(model="text-embedding-3-small", input=batch)
            all_embeddings.extend([np.array(item.embedding) for item in response.data])
        
        self.embeddings = np.array(all_embeddings)
        
        if verbose:
            print(f"  ✓ {len(all_embeddings)} embeddings computed")
    
    def cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        return np.dot(a, b) / (norm(a) * norm(b) + 1e-10)
    
    def calculate_exact_boost(self, cell: StructuredCell, query: str) -> float:
        """Smart exact matching boost"""
        query_words = set(query.lower().split())
        boost = 0.0
        
        for header in cell.col_headers + cell.row_headers:
            header_lower = header.lower()
            for qword in query_words:
                # Exact match
                if header_lower == qword:
                    boost += 0.5
                # Substring match
                elif len(qword) >= 2 and qword in header_lower:
                    boost += 0.3
        
        return boost
    
    def search(self, query: str, top_k: int = 10, semantic_weight: float = 0.5, verbose: bool = True) -> List[Tuple[StructuredCell, float]]:
        """Hybrid search with exact matching"""
        
        if verbose:
            print(f"\nSearching: '{query}'")
        
        normalized_query = self.normalizer.normalize(query)
        query_tokens = self.normalizer.tokenize(query)
        
        # BM25 scores
        bm25_scores = self.bm25.get_scores(query_tokens)
        bm25_scores = bm25_scores / (np.max(bm25_scores) + 1e-10)
        
        # Embedding scores
        query_embedding = self.get_embedding(normalized_query)
        embedding_scores = np.array([
            self.cosine_similarity(query_embedding, emb) for emb in self.embeddings
        ])
        
        # Hybrid scores
        hybrid_scores = (1 - semantic_weight) * bm25_scores + semantic_weight * embedding_scores
        
        # Apply exact matching boost
        final_scores = []
        for i, cell in enumerate(self.structured_data):
            boost = self.calculate_exact_boost(cell, query)
            final_score = hybrid_scores[i] + boost
            final_scores.append(final_score)
        
        final_scores = np.array(final_scores)
        
        # Get top K
        top_indices = np.argsort(final_scores)[-top_k:][::-1]
        matches = [(self.structured_data[idx], float(final_scores[idx])) for idx in top_indices]
        
        if verbose:
            print(f"\n✓ Top {min(5, len(matches))} results:")
            for i, (cell, score) in enumerate(matches[:5], 1):
                print(f"  {i}. Score: {score:.3f}")
                print(f"     Cell: {cell.sheet}!{cell.cell_ref}")
                print(f"     Headers: {cell.col_headers} / {cell.row_headers}")
                print(f"     Value: {cell.value}")
        
        return matches
    
    def get_embedding(self, text: str) -> np.ndarray:
        response = openai.embeddings.create(model="text-embedding-3-small", input=text)
        return np.array(response.data[0].embedding)


# ============================================================================
# VALUE EXTRACTOR
# ============================================================================

class ValueExtractor:
    """Simple value extraction"""
    
    @staticmethod
    def extract(matches: List[Tuple[StructuredCell, float]], operation: str = "return") -> Any:
        if not matches:
            return None
        
        if operation == "return":
            return matches[0][0].value
        
        elif operation in ["sum", "average", "max", "min"]:
            values = []
            for cell, _ in matches:
                if cell.value_type in ["number", "currency", "percentage", "numeric_string"]:
                    val = cell.value
                    if isinstance(val, str):
                        val = float(val.replace(',', '').replace('$', '').replace('%', '').strip())
                    values.append(val)
            
            if not values:
                return None
            
            if operation == "sum":
                return sum(values)
            elif operation == "average":
                return sum(values) / len(values)
            elif operation == "max":
                return max(values)
            elif operation == "min":
                return min(values)
        
        elif operation == "count":
            return len(matches)
        
        elif operation == "list":
            return [cell.value for cell, _ in matches]
        
        return None


# ============================================================================
# MAIN QUERY ENGINE
# ============================================================================

class ExcelQueryEngine:
    """
    Clean Excel query engine with:
    - Linear flattening (simple, debuggable)
    - Hybrid search (BM25 + embeddings)
    - Smart exact matching
    """
    
    def __init__(self, api_key: str):
        self.flattener = LinearFlattener()
        self.searcher = HybridSearcher(api_key)
        self.extractor = ValueExtractor()
        self.structured_ Optional[List[StructuredCell]] = None
    
    def load_workbook(self, file_path: str, verbose: bool = True) -> 'ExcelQueryEngine':
        """Load and index workbook"""
        import time
        start = time.time()
        
        # Flatten
        self.structured_data = self.flattener.flatten_workbook(file_path, verbose)
        
        # Build indices
        self.searcher.build_indices(self.structured_data, verbose)
        
        elapsed = time.time() - start
        if verbose:
            print(f"\n✓ Total load time: {elapsed:.1f}s\n")
        
        return self
    
    def query(self, 
              query: str,
              operation: str = "return",
              semantic_weight: float = 0.5,
              min_similarity: float = 0.3,
              top_k: int = 10,
              verbose: bool = True) -> QueryResult:
        """Query the workbook"""
        
        if verbose:
            print("="*80)
            print(f"QUERY: {query}")
            print(f"OPERATION: {operation}")
            print("="*80)
        
        # Search
        matches = self.searcher.search(query, top_k, semantic_weight, verbose)
        
        # Filter
        matches = [(cell, score) for cell, score in matches if score >= min_similarity]
        
        if not matches:
            if verbose:
                print("\n✗ No matches above threshold")
            return QueryResult(query=query, result=None, matches=[], operation=operation, confidence=0.0)
        
        # Extract
        result_value = self.extractor.extract(matches, operation)
        confidence = float(np.mean([score for _, score in matches]))
        
        if verbose:
            print(f"\n{'='*80}")
            print(f"RESULT: {result_value}")
            print(f"CONFIDENCE: {confidence:.3f}")
            print(f"{'='*80}\n")
        
        return QueryResult(
            query=query,
            result=result_value,
            matches=[{
                "cell": cell.cell_ref,
                "sheet": cell.sheet,
                "value": cell.value,
                "row_headers": cell.row_headers,
                "col_headers": cell.col_headers,
                "score": score
            } for cell, score in matches[:5]],
            operation=operation,
            confidence=confidence
        )


# ============================================================================
# USAGE
# ============================================================================

if __name__ == "__main__":
    # Initialize
    engine = ExcelQueryEngine(api_key="your-openai-api-key")
    
    # Load workbook
    engine.load_workbook("financial_report.xlsx")
    
    # Query
    result = engine.query(
        query="What is the GV revenue?",
        operation="return",
        semantic_weight=0.5,
        verbose=True
    )
    
    print(f"Answer: {result.result}")
