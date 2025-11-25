"""
ULTIMATE PRECISION EXCEL QUERY ENGINE
Complete production code with all precision techniques:
- Type-aware flattening
- Text normalization
- Hybrid search (BM25 + embeddings)
- Cross-encoder reranking
- Query expansion
- Metadata filtering
- Confidence validation
"""

import pandas as pd
import openpyxl
from openpyxl.utils import get_column_letter, column_index_from_string
from typing import Dict, List, Tuple, Any, Optional, Set
from dataclasses import dataclass
import re
import json
import openai
import numpy as np
from numpy.linalg import norm
from collections import defaultdict, Counter
from rank_bm25 import BM25Okapi
from sentence_transformers import CrossEncoder

class OpenAIReranker:
    """Rerank using OpenAI API instead of local model"""
    
    def __init__(self, api_key: str):
        openai.api_key = api_key
        print("Using OpenAI-based reranker")
    
    def rerank(self, query: str, matches: List[Tuple[FlattenedCell, float]], 
               top_k: int = 5, verbose: bool = True) -> List[Tuple[FlattenedCell, float]]:
        """Rerank using OpenAI relevance scoring"""
        
        if verbose:
            print(f"\nReranking {len(matches)} results with OpenAI...")
        
        # Create candidates text
        candidates = []
        for i, (entry, _) in enumerate(matches):
            candidates.append({
                "id": i,
                "text": f"{entry.path}: {entry.value}"
            })
        
        # Ask GPT to rank by relevance
        prompt = f"""Rank these {len(candidates)} results by relevance to the query: "{query}"

Results:
{json.dumps(candidates, indent=2)}

Return ONLY a JSON array of IDs in order from most to least relevant.
Example: [2, 0, 5, 1, 3]

JSON array:"""
        
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=200
        )
        
        # Parse ranking
        try:
            ranking = json.loads(response.choices[0].message.content)
        except:
            # Fallback: extract numbers
            ranking = [int(x) for x in re.findall(r'\d+', response.choices[0].message.content)]
        
        # Reorder matches
        reranked = []
        for idx in ranking[:top_k]:
            if 0 <= idx < len(matches):
                reranked.append(matches[idx])
        
        if verbose:
            print(f"✓ Reranked, returning top {len(reranked)}")
        
        return reranked


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class FlattenedCell:
    """A single flattened cell with full context path"""
    sheet: str
    path: str
    cell_ref: str
    value: Any
    value_type: str
    row: int
    col: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "sheet": self.sheet,
            "path": self.path,
            "cell_ref": self.cell_ref,
            "value": self.value,
            "value_type": self.value_type,
            "row": self.row,
            "col": self.col
        }


@dataclass
class QueryResult:
    """Result of a query operation"""
    query: str
    result: Any
    matches: List[Tuple[str, Any, str, str]]
    operation: str
    confidence: float = 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "result": self.result,
            "matches": self.matches,
            "operation": self.operation,
            "confidence": self.confidence
        }


# ============================================================================
# CELL TYPE ANALYZER
# ============================================================================

class CellTypeAnalyzer:
    """Analyzes cell types to distinguish headers from data"""
    
    @staticmethod
    def is_numeric_value(value: Any) -> bool:
        """Check if value is numeric or numeric string"""
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
        """Determine if a value is likely a header"""
        if value is None:
            return False
        
        if CellTypeAnalyzer.is_numeric_value(value):
            return False
        
        if isinstance(value, bool):
            return False
        
        if isinstance(value, str):
            if CellTypeAnalyzer.is_numeric_value(value):
                return False
            
            if len(value) > 100:
                return False
            
            return True
        
        return False
    
    @staticmethod
    def classify_value_type(value: Any) -> str:
        """Classify value into detailed type"""
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
            
            if re.match(r'\d{1,4}[-/]\d{1,2}[-/]\d{1,4}', value):
                return "date_string"
            
            return "text"
        
        if hasattr(value, 'year') and hasattr(value, 'month'):
            return "date"
        
        return "other"


# ============================================================================
# TEXT NORMALIZER
# ============================================================================

class TextNormalizer:
    """Normalizes and preprocesses text for maximum search precision"""
    
    def __init__(self):
        self.abbreviations = {
            # Financial
            'q1': 'quarter 1', 'q2': 'quarter 2', 'q3': 'quarter 3', 'q4': 'quarter 4',
            'fy': 'fiscal year', 'ytd': 'year to date', 'mtd': 'month to date',
            'yoy': 'year over year', 'mom': 'month over month',
            'rev': 'revenue', 'revs': 'revenue',
            'exp': 'expense', 'exps': 'expenses',
            'opex': 'operating expenses', 'capex': 'capital expenses',
            'cogs': 'cost of goods sold', 'sg&a': 'selling general administrative',
            'ebitda': 'earnings before interest tax depreciation amortization',
            'ebit': 'earnings before interest tax',
            'gross': 'gross profit', 'net': 'net profit',
            'op': 'operating', 'ops': 'operations',
            
            # Units
            'k': 'thousand', 'm': 'million', 'b': 'billion',
            'usd': 'dollars', '$': 'dollars', '€': 'euros', '£': 'pounds',
            'pct': 'percent', '%': 'percent',
            
            # Time periods
            'jan': 'january', 'feb': 'february', 'mar': 'march', 'apr': 'april',
            'jun': 'june', 'jul': 'july', 'aug': 'august', 'sep': 'september',
            'oct': 'october', 'nov': 'november', 'dec': 'december',
            
            # Common business terms
            'dept': 'department', 'hr': 'human resources',
            'it': 'information technology', 'r&d': 'research development',
            'mktg': 'marketing', 'mgmt': 'management',
            'acct': 'accounting', 'fin': 'finance',
            'avg': 'average', 'tot': 'total', 'ttl': 'total',
            'qty': 'quantity', 'amt': 'amount',
            'proj': 'project', 'est': 'estimated', 'act': 'actual',
        }
        
        self.stopwords = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'been',
            'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
            'could', 'should', 'may', 'might', 'must', 'can', 'this', 'that',
            'these', 'those', 'it', 'its'
        }
    
    def normalize_text(self, text: str, remove_stopwords: bool = False) -> str:
        """Comprehensive text normalization"""
        if not isinstance(text, str):
            text = str(text)
        
        # Lowercase
        text = text.lower()
        
        # Remove special characters but keep spaces and alphanumeric
        text = re.sub(r'[^\w\s&-]', ' ', text)
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Expand abbreviations
        words = text.split()
        expanded_words = []
        for word in words:
            if word in self.abbreviations:
                expanded_words.append(self.abbreviations[word])
            else:
                expanded_words.append(word)
        
        text = ' '.join(expanded_words)
        
        # Remove stopwords if requested
        if remove_stopwords:
            words = text.split()
            words = [w for w in words if w not in self.stopwords]
            text = ' '.join(words)
        
        # Remove duplicate spaces
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def normalize_path(self, path: str) -> str:
        """Normalize a flattened path"""
        parts = path.split('-')
        normalized_parts = []
        for part in parts:
            normalized = self.normalize_text(part, remove_stopwords=False)
            if normalized:
                normalized_parts.append(normalized)
        
        return ' | '.join(normalized_parts)
    
    def add_synonyms(self, text: str) -> str:
        """Add synonym variations"""
        synonyms = {
            'revenue': 'revenue sales income',
            'profit': 'profit earnings income',
            'expense': 'expense cost spending',
            'total': 'total sum aggregate',
            'average': 'average mean avg',
        }
        
        words = text.lower().split()
        expanded = []
        
        for word in words:
            expanded.append(word)
            if word in synonyms:
                for syn in synonyms[word].split():
                    if syn != word:
                        expanded.append(syn)
        
        return ' '.join(expanded)


# ============================================================================
# STRUCTURE DETECTOR
# ============================================================================

class ImprovedStructureDetector:
    """Detects headers and table structures using data type analysis"""
    
    def __init__(self, max_header_rows: int = 10, max_row_header_cols: int = 5):
        self.max_header_rows = max_header_rows
        self.max_row_header_cols = max_row_header_cols
        self.analyzer = CellTypeAnalyzer()
    
    def analyze_row_types(self, ws, row_num: int) -> Dict[str, Any]:
        """Analyze the types of values in a row"""
        values = []
        for col in range(1, ws.max_column + 1):
            cell = ws.cell(row_num, col)
            if cell.value is not None:
                values.append(cell.value)
        
        if not values:
            return {"empty": True}
        
        type_counts = Counter([self.analyzer.classify_value_type(v) for v in values])
        total = len(values)
        
        numeric_count = sum(type_counts.get(t, 0) for t in ["number", "currency", "percentage", "numeric_string"])
        text_count = type_counts.get("text", 0)
        
        return {
            "empty": False,
            "total_cells": total,
            "numeric_count": numeric_count,
            "text_count": text_count,
            "numeric_ratio": numeric_count / total if total > 0 else 0,
            "text_ratio": text_count / total if total > 0 else 0,
            "type_distribution": dict(type_counts)
        }
    
    def detect_header_rows(self, ws) -> List[int]:
        """Detect header rows using type analysis"""
        header_rows = []
        consecutive_data_rows = 0
        
        for row_num in range(1, min(self.max_header_rows + 1, ws.max_row + 1)):
            row_info = self.analyze_row_types(ws, row_num)
            
            if row_info["empty"]:
                continue
            
            first_cell = ws.cell(row_num, 1)
            is_bold = first_cell.font and first_cell.font.bold
            
            is_header = (
                row_info["text_ratio"] > 0.7 or
                (row_info["text_ratio"] > 0.4 and row_info["numeric_ratio"] < 0.3) or
                (is_bold and row_info["text_count"] > 0)
            )
            
            if is_header:
                header_rows.append(row_num)
                consecutive_data_rows = 0
            else:
                consecutive_data_rows += 1
                if consecutive_data_rows >= 3:
                    break
        
        return header_rows
    
    def analyze_column_types(self, ws, col_num: int, start_row: int = 1) -> Dict[str, Any]:
        """Analyze the types of values in a column"""
        values = []
        for row in range(start_row, min(start_row + 30, ws.max_row + 1)):
            cell = ws.cell(row, col_num)
            if cell.value is not None:
                values.append(cell.value)
        
        if not values:
            return {"empty": True}
        
        type_counts = Counter([self.analyzer.classify_value_type(v) for v in values])
        total = len(values)
        
        numeric_count = sum(type_counts.get(t, 0) for t in ["number", "currency", "percentage", "numeric_string"])
        text_count = type_counts.get("text", 0)
        
        return {
            "empty": False,
            "total_cells": total,
            "numeric_count": numeric_count,
            "text_count": text_count,
            "numeric_ratio": numeric_count / total if total > 0 else 0,
            "text_ratio": text_count / total if total > 0 else 0,
            "type_distribution": dict(type_counts)
        }
    
    def detect_row_header_cols(self, ws, header_rows: List[int]) -> List[int]:
        """Detect row header columns using type analysis"""
        row_header_cols = []
        data_start_row = max(header_rows) + 1 if header_rows else 1
        
        for col_num in range(1, min(self.max_row_header_cols + 1, ws.max_column + 1)):
            col_info = self.analyze_column_types(ws, col_num, data_start_row)
            
            if col_info["empty"]:
                continue
            
            if col_info["text_ratio"] > 0.6 and col_info["numeric_ratio"] < 0.4:
                row_header_cols.append(col_num)
            else:
                break
        
        return row_header_cols
    
    def build_column_header_path(self, ws, col: int, header_rows: List[int]) -> str:
        """Build hierarchical path from multi-level column headers"""
        parts = []
        for row in header_rows:
            cell = ws.cell(row, col)
            value = cell.value
            
            if value is not None and self.analyzer.is_likely_header(value):
                parts.append(str(value).strip())
        
        return "-".join(parts) if parts else f"Col{get_column_letter(col)}"
    
    def build_row_header_path(self, ws, row: int, row_header_cols: List[int]) -> str:
        """Build hierarchical path from multi-level row headers"""
        parts = []
        for col in row_header_cols:
            cell = ws.cell(row, col)
            value = cell.value
            
            if value is not None and self.analyzer.is_likely_header(value):
                parts.append(str(value).strip())
        
        return "-".join(parts) if parts else f"Row{row}"


# ============================================================================
# SHEET FLATTENER
# ============================================================================

class ImprovedSheetFlattener:
    """Flattens a single Excel sheet with proper header/data separation"""
    
    def __init__(self, detector: ImprovedStructureDetector):
        self.detector = detector
        self.analyzer = CellTypeAnalyzer()
    
    def flatten(self, ws, ws_data, sheet_name: str, verbose: bool = True) -> List[FlattenedCell]:
        """Flatten sheet into path-value pairs, excluding header cells"""
        flattened = []
        
        header_rows = self.detector.detect_header_rows(ws)
        row_header_cols = self.detector.detect_row_header_cols(ws, header_rows)
        
        if verbose:
            print(f"  Sheet '{sheet_name}':")
            print(f"    Header rows: {header_rows}")
            print(f"    Row header cols: {[get_column_letter(c) for c in row_header_cols]}")
        
        data_start_row = max(header_rows) + 1 if header_rows else 1
        data_start_col = max(row_header_cols) + 1 if row_header_cols else 1
        
        if verbose:
            print(f"    Data area starts at: Row {data_start_row}, Col {get_column_letter(data_start_col)}")
        
        col_headers = {}
        for col in range(data_start_col, ws.max_column + 1):
            col_headers[col] = self.detector.build_column_header_path(ws, col, header_rows)
        
        data_cell_count = 0
        skipped_header_cells = 0
        
        for row in range(data_start_row, ws.max_row + 1):
            row_path = self.detector.build_row_header_path(ws, row, row_header_cols)
            
            for col in range(data_start_col, ws.max_column + 1):
                cell = ws.cell(row, col)
                cell_data = ws_data.cell(row, col)
                
                value = cell_data.value if cell_data.value is not None else cell.value
                
                if value is None:
                    continue
                
                value_type = self.analyzer.classify_value_type(value)
                
                # Skip likely header cells in data area
                if value_type == "text" and not self.analyzer.is_numeric_value(value):
                    row_info = self.detector.analyze_row_types(ws, row)
                    if row_info.get("text_ratio", 0) > 0.8:
                        skipped_header_cells += 1
                        if verbose and skipped_header_cells <= 5:
                            print(f"      Skipping likely header cell: {get_column_letter(col)}{row} = '{value}'")
                        continue
                
                col_path = col_headers.get(col, f"Col{get_column_letter(col)}")
                full_path = f"{sheet_name}-{row_path}-{col_path}"
                cell_ref = f"{get_column_letter(col)}{row}"
                
                flattened.append(FlattenedCell(
                    sheet=sheet_name,
                    path=full_path,
                    cell_ref=cell_ref,
                    value=value,
                    value_type=value_type,
                    row=row,
                    col=col
                ))
                data_cell_count += 1
        
        if verbose:
            print(f"    → Extracted {data_cell_count} data values")
            if skipped_header_cells > 0:
                print(f"    → Skipped {skipped_header_cells} header/label cells")
        
        return flattened


# ============================================================================
# WORKBOOK FLATTENER
# ============================================================================

class ImprovedWorkbookFlattener:
    """Flattens entire Excel workbook with proper type validation"""
    
    def __init__(self, detector: Optional[ImprovedStructureDetector] = None):
        self.detector = detector or ImprovedStructureDetector()
        self.sheet_flattener = ImprovedSheetFlattener(self.detector)
        self.flattened_ List[FlattenedCell] = []
        self.file_path: Optional[str] = None
    
    def flatten(self, file_path: str, verbose: bool = True) -> List[FlattenedCell]:
        """Flatten entire workbook"""
        self.file_path = file_path
        
        if verbose:
            print(f"\nFlattening workbook: {file_path}")
            print("="*80)
        
        wb = openpyxl.load_workbook(file_path, data_only=False)
        wb_data = openpyxl.load_workbook(file_path, data_only=True)
        
        all_flattened = []
        
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            ws_data = wb_data[sheet_name]
            
            sheet_flattened = self.sheet_flattener.flatten(ws, ws_data, sheet_name, verbose)
            all_flattened.extend(sheet_flattened)
        
        wb.close()
        wb_data.close()
        
        self.flattened_data = all_flattened
        
        if verbose:
            print(f"\n✓ Total flattened DATA entries: {len(all_flattened)}")
            
            type_counts = Counter([entry.value_type for entry in all_flattened])
            print(f"\nValue type distribution:")
            for vtype, count in type_counts.most_common():
                print(f"  {vtype}: {count}")
            
            print("="*80 + "\n")
        
        return all_flattened
    
    def export_to_csv(self, output_path: str):
        """Export flattened data to CSV"""
        import csv
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Sheet", "Path", "Cell", "Value", "Type", "Row", "Col"])
            
            for entry in self.flattened_
                writer.writerow([
                    entry.sheet,
                    entry.path,
                    entry.cell_ref,
                    entry.value,
                    entry.value_type,
                    entry.row,
                    entry.col
                ])
        
        print(f"✓ Exported {len(self.flattened_data)} data entries to: {output_path}")


# ============================================================================
# EMBEDDING SEARCHER
# ============================================================================

class ImprovedEmbeddingSearcher:
    """Enhanced semantic search with text preprocessing"""
    
    def __init__(self, api_key: str, model: str = "text-embedding-3-small"):
        self.api_key = api_key
        self.model = model
        openai.api_key = api_key
        
        self.normalizer = TextNormalizer()
        self.path_embeddings: Optional[np.ndarray] = None
        self.flattened_ Optional[List[FlattenedCell]] = None
        self.normalized_paths: List[str] = []
    
    def get_embedding(self, text: str) -> np.ndarray:
        """Get embedding vector for text"""
        response = openai.embeddings.create(
            model=self.model,
            input=text
        )
        return np.array(response.data[0].embedding)
    
    def create_searchable_text(self, entry: FlattenedCell) -> str:
        """Create optimized searchable text from entry"""
        normalized_path = self.normalizer.normalize_path(entry.path)
        expanded_path = self.normalizer.add_synonyms(normalized_path)
        
        value_str = str(entry.value)
        
        if entry.value_type in ["number", "currency", "percentage"]:
            value_context = f"value: {value_str}"
        else:
            value_context = self.normalizer.normalize_text(value_str)
        
        searchable = f"{expanded_path} {value_context}"
        
        return searchable
    
    def compute_path_embeddings(self, flattened: List[FlattenedCell], verbose: bool = True):
        """Pre-compute embeddings with normalization"""
        if verbose:
            print(f"Computing normalized embeddings for {len(flattened)} paths...")
        
        texts = []
        self.normalized_paths = []
        
        for entry in flattened:
            searchable = self.create_searchable_text(entry)
            texts.append(searchable)
            self.normalized_paths.append(searchable)
        
        if verbose:
            print("Sample normalized paths:")
            for i in range(min(3, len(flattened))):
                print(f"  Original: {flattened[i].path}")
                print(f"  Normalized: {self.normalized_paths[i]}")
                print()
        
        batch_size = 2048
        all_embeddings = []
        
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i+batch_size]
            
            if verbose and len(texts) > batch_size:
                print(f"  Embedding batch {i//batch_size + 1}/{(len(texts)-1)//batch_size + 1}")
            
            response = openai.embeddings.create(
                model=self.model,
                input=batch
            )
            
            batch_embeddings = [np.array(item.embedding) for item in response.data]
            all_embeddings.extend(batch_embeddings)
        
        self.path_embeddings = np.array(all_embeddings)
        self.flattened_data = flattened
        
        if verbose:
            print(f"✓ Computed {len(all_embeddings)} normalized embeddings")
    
    def cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        """Compute cosine similarity"""
        return np.dot(a, b) / (norm(a) * norm(b))
    
    def preprocess_query(self, query: str) -> str:
        """Preprocess query with same normalization as paths"""
        normalized = self.normalizer.normalize_text(query, remove_stopwords=False)
        expanded = self.normalizer.add_synonyms(normalized)
        return expanded
    
    def search(self, query: str, top_k: int = 10, verbose: bool = True) -> List[Tuple[FlattenedCell, float]]:
        """Search with query preprocessing"""
        if self.path_embeddings is None or self.flattened_data is None:
            raise ValueError("Must call compute_path_embeddings() first")
        
        processed_query = self.preprocess_query(query)
        
        if verbose:
            print(f"\nOriginal query: '{query}'")
            print(f"Processed query: '{processed_query}'")
        
        query_embedding = self.get_embedding(processed_query)
        
        similarities = np.array([
            self.cosine_similarity(query_embedding, path_emb)
            for path_emb in self.path_embeddings
        ])
        
        top_indices = np.argsort(similarities)[-top_k:][::-1]
        
        matches = [
            (self.flattened_data[idx], float(similarities[idx]))
            for idx in top_indices
        ]
        
        if verbose:
            print(f"\n✓ Top {len(matches)} matches:")
            for i, (entry, score) in enumerate(matches[:5], 1):
                print(f"  {i}. Score: {score:.3f}")
                print(f"     Path: {entry.path}")
                print(f"     Value: {entry.value} ({entry.value_type}) [{entry.sheet}!{entry.cell_ref}]")
                print()
        
        return matches


# ============================================================================
# HYBRID SEARCHER (BM25 + EMBEDDINGS)
# ============================================================================

class HybridSearcher:
    """Combines BM25 keyword matching with semantic embeddings"""
    
    def __init__(self, embedding_searcher: ImprovedEmbeddingSearcher):
        self.embedding_searcher = embedding_searcher
        self.bm25 = None
        self.tokenized_corpus = []
    
    def build_bm25_index(self, flattened: List[FlattenedCell], verbose: bool = True):
        """Build BM25 index for keyword search"""
        if verbose:
            print("Building BM25 index for keyword search...")
        
        corpus = [self.embedding_searcher.create_searchable_text(entry) 
                  for entry in flattened]
        self.tokenized_corpus = [doc.split() for doc in corpus]
        self.bm25 = BM25Okapi(self.tokenized_corpus)
        
        if verbose:
            print(f"✓ BM25 index built for {len(corpus)} documents")
    
    def hybrid_search(self, query: str, top_k: int = 10, 
                     semantic_weight: float = 0.5, verbose: bool = True) -> List[Tuple[FlattenedCell, float]]:
        """
        Hybrid search combining BM25 and embeddings.
        semantic_weight: 0.0 = pure BM25, 1.0 = pure embeddings, 0.5 = balanced
        """
        if self.bm25 is None:
            raise ValueError("Must call build_bm25_index() first")
        
        if verbose:
            print(f"\nHybrid search (semantic_weight={semantic_weight})...")
        
        # Preprocess query
        processed_query = self.embedding_searcher.preprocess_query(query)
        tokenized_query = processed_query.split()
        
        # BM25 scores
        bm25_scores = self.bm25.get_scores(tokenized_query)
        bm25_scores = bm25_scores / (np.max(bm25_scores) + 1e-10)  # Normalize
        
        # Embedding scores
        query_embedding = self.embedding_searcher.get_embedding(processed_query)
        embedding_scores = np.array([
            self.embedding_searcher.cosine_similarity(query_embedding, path_emb)
            for path_emb in self.embedding_searcher.path_embeddings
        ])
        
        # Combine scores
        hybrid_scores = (1 - semantic_weight) * bm25_scores + semantic_weight * embedding_scores
        
        # Get top K
        top_indices = np.argsort(hybrid_scores)[-top_k:][::-1]
        
        matches = [(self.embedding_searcher.flattened_data[idx], float(hybrid_scores[idx]))
                   for idx in top_indices]
        
        if verbose:
            print(f"✓ Hybrid search returned {len(matches)} results")
        
        return matches


# ============================================================================
# RERANKER
# ============================================================================

class Reranker:
    """Rerank results using cross-encoder for maximum precision"""
    
    def __init__(self, model_name: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"):
        print(f"Loading reranker model: {model_name}...")
        self.model = CrossEncoder(model_name)
        print("✓ Reranker model loaded")
    
    def rerank(self, query: str, matches: List[Tuple[FlattenedCell, float]], 
               top_k: int = 5, verbose: bool = True) -> List[Tuple[FlattenedCell, float]]:
        """Rerank matches using cross-encoder"""
        if verbose:
            print(f"\nReranking top {len(matches)} results...")
        
        pairs = []
        for entry, _ in matches:
            searchable_text = f"{entry.path}: {entry.value}"
            pairs.append([query, searchable_text])
        
        scores = self.model.predict(pairs)
        
        reranked = sorted(zip([m[0] for m in matches], scores), 
                         key=lambda x: x[1], reverse=True)
        
        if verbose:
            print(f"✓ Reranked, returning top {top_k}")
        
        return reranked[:top_k]


# ============================================================================
# QUERY EXPANDER
# ============================================================================

class QueryExpander:
    """Expand query with synonyms and variations"""
    
    def __init__(self, api_key: str):
        openai.api_key = api_key
    
    def expand_query(self, query: str, verbose: bool = True) -> List[str]:
        """Generate alternative phrasings"""
        if verbose:
            print(f"\nExpanding query: '{query}'")
        
        prompt = f"""Generate 4 alternative phrasings of this query that mean the same thing:
"{query}"

Return as JSON array: ["variation1", "variation2", "variation3", "variation4"]"""
        
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3
        )
        
        variations = json.loads(response.choices[0].message.content)
        
        if verbose:
            print(f"✓ Generated {len(variations)} variations:")
            for v in variations:
                print(f"  - {v}")
        
        return [query] + variations
    
    def multi_query_search(self, searcher, query: str, top_k: int = 10, verbose: bool = True):
        """Search with all query variations and merge results"""
        variations = self.expand_query(query, verbose)
        
        all_matches = {}
        for var in variations:
            matches = searcher.search(var, top_k=top_k, verbose=False)
            for entry, score in matches:
                key = entry.cell_ref
                if key in all_matches:
                    all_matches[key] = (entry, max(all_matches[key][1], score))
                else:
                    all_matches[key] = (entry, score)
        
        sorted_matches = sorted(all_matches.values(), key=lambda x: x[1], reverse=True)
        
        if verbose:
            print(f"\n✓ Multi-query search merged to {len(sorted_matches)} unique results")
        
        return sorted_matches[:top_k]


# ============================================================================
# METADATA FILTER
# ============================================================================

class MetadataFilter:
    """Filter results by metadata before search"""
    
    @staticmethod
    def filter_by_criteria(flattened: List[FlattenedCell], 
                          sheet: Optional[str] = None,
                          value_type: Optional[str] = None,
                          value_range: Optional[Tuple[float, float]] = None,
                          verbose: bool = True) -> List[FlattenedCell]:
        """Pre-filter data by metadata"""
        original_count = len(flattened)
        filtered = flattened
        
        if sheet:
            filtered = [e for e in filtered if e.sheet == sheet]
            if verbose:
                print(f"  Filtered by sheet='{sheet}': {len(filtered)} entries")
        
        if value_type:
            filtered = [e for e in filtered if e.value_type == value_type]
            if verbose:
                print(f"  Filtered by value_type='{value_type}': {len(filtered)} entries")
        
        if value_range:
            min_val, max_val = value_range
            filtered = [e for e in filtered 
                       if isinstance(e.value, (int, float)) and min_val <= e.value <= max_val]
            if verbose:
                print(f"  Filtered by value_range={value_range}: {len(filtered)} entries")
        
        if verbose:
            print(f"✓ Metadata filtering: {original_count} → {len(filtered)} entries")
        
        return filtered


# ============================================================================
# CONFIDENCE HANDLER
# ============================================================================

class ConfidenceHandler:
    """Handle low-confidence results intelligently"""
    
    def validate_result(self, result: QueryResult, 
                       min_confidence: float = 0.5) -> Dict[str, Any]:
        """Validate and provide fallback options"""
        
        if result.confidence >= min_confidence:
            return {
                "status": "high_confidence",
                "result": result.result,
                "confidence": result.confidence,
                "message": "High confidence result"
            }
        else:
            return {
                "status": "low_confidence",
                "result": result.result,
                "confidence": result.confidence,
                "message": "Low confidence - please review alternatives",
                "alternatives": result.matches[:3]
            }


# ============================================================================
# VALUE EXTRACTOR
# ============================================================================

class ValueExtractor:
    """Extracts values algorithmically (GUARANTEED PRECISE)"""
    
    @staticmethod
    def extract(matches: List[Tuple[FlattenedCell, str]], operation: str = "return") -> Any:
        """Extract value(s) algorithmically - NO AI"""
        
        if not matches:
            return None
        
        if operation == "return":
            return matches[0][0].value
        
        elif operation == "sum":
            values = []
            for m, _ in matches:
                if m.value_type in ["number", "currency", "percentage", "numeric_string"]:
                    val = m.value
                    if isinstance(val, str):
                        val = float(val.replace(',', '').replace('$', '').replace('%', '').strip())
                    values.append(val)
            return sum(values) if values else None
        
        elif operation == "average":
            values = []
            for m, _ in matches:
                if m.value_type in ["number", "currency", "percentage", "numeric_string"]:
                    val = m.value
                    if isinstance(val, str):
                        val = float(val.replace(',', '').replace('$', '').replace('%', '').strip())
                    values.append(val)
            return sum(values) / len(values) if values else None
        
        elif operation in ["max", "min"]:
            values = []
            for m, _ in matches:
                if m.value_type in ["number", "currency", "percentage", "numeric_string"]:
                    val = m.value
                    if isinstance(val, str):
                        val = float(val.replace(',', '').replace('$', '').replace('%', '').strip())
                    values.append(val)
            return (max if operation == "max" else min)(values) if values else None
        
        elif operation == "count":
            return len(matches)
        
        elif operation == "list":
            return [m[0].value for m, _ in matches]
        
        else:
            raise ValueError(f"Unknown operation: {operation}")


# ============================================================================
# ULTIMATE PRECISION QUERY ENGINE
# ============================================================================

class PrecisionQueryEngine:
    """
    Ultimate precision query engine with all techniques:
    - Type-aware flattening
    - Text normalization
    - Hybrid search (BM25 + embeddings)
    - Cross-encoder reranking
    - Query expansion
    - Metadata filtering
    - Confidence validation
    """
    
    def __init__(self, api_key: str):
        self.flattener = ImprovedWorkbookFlattener()
        self.embedding_searcher = ImprovedEmbeddingSearcher(api_key)
        self.hybrid_searcher = None
        self.reranker = Reranker()
        self.query_expander = QueryExpander(api_key)
        self.metadata_filter = MetadataFilter()
        self.confidence_handler = ConfidenceHandler()
        self.extractor = ValueExtractor()
        
        self.flattened_ Optional[List[FlattenedCell]] = None
        self.file_path: Optional[str] = None
    
    def load_workbook(self, file_path: str, verbose: bool = True) -> 'PrecisionQueryEngine':
        """Load workbook with all indices"""
        self.file_path = file_path
        
        # Flatten
        self.flattened_data = self.flattener.flatten(file_path, verbose)
        
        # Compute embeddings
        if verbose:
            print("\nBuilding search indices...")
        self.embedding_searcher.compute_path_embeddings(self.flattened_data, verbose)
        
        # Build hybrid searcher
        self.hybrid_searcher = HybridSearcher(self.embedding_searcher)
        self.hybrid_searcher.build_bm25_index(self.flattened_data, verbose)
        
        if verbose:
            print("\n✓ Workbook loaded and indexed")
        
        return self
    
    def query(self, 
              query: str,
              operation: str = "return",
              use_hybrid: bool = True,
              use_reranking: bool = True,
              use_query_expansion: bool = False,
              metadata_filters: Optional[Dict] = None,
              semantic_weight: float = 0.5,
              min_confidence: float = 0.0,
              top_k: int = 10,
              verbose: bool = True) -> Tuple[QueryResult, Dict[str, Any]]:
        """
        Ultimate precision query with all techniques.
        
        Args:
            query: Natural language query
            operation: "return" | "sum" | "average" | "max" | "min" | "count" | "list"
            use_hybrid: Use hybrid search (BM25 + embeddings)
            use_reranking: Use cross-encoder reranking
            use_query_expansion: Generate query variations
            metadata_filters: Dict with 'sheet', 'value_type', 'value_range'
            semantic_weight: 0.0-1.0, balance between BM25 and embeddings
            min_confidence: Minimum confidence threshold
            top_k: Number of results to retrieve
            verbose: Print progress
            
        Returns:
            (QueryResult, validation_dict)
        """
        
        if self.flattened_data is None:
            raise ValueError("No workbook loaded. Call load_workbook() first.")
        
        if verbose:
            print("\n" + "="*80)
            print(f"PRECISION QUERY: {query}")
            print(f"Configuration:")
            print(f"  - Hybrid search: {use_hybrid}")
            print(f"  - Reranking: {use_reranking}")
            print(f"  - Query expansion: {use_query_expansion}")
            print(f"  - Metadata filters: {metadata_filters}")
            print("="*80)
        
        # Stage 1: Metadata filtering
        working_data = self.flattened_data
        if metadata_filters:
            if verbose:
                print("\n[Stage 1] Metadata filtering...")
            working_data = self.metadata_filter.filter_by_criteria(
                working_data, **metadata_filters, verbose=verbose
            )
        
        # Stage 2: Retrieval
        if verbose:
            print(f"\n[Stage 2] Retrieval...")
        
        if use_query_expansion:
            if verbose:
                print("Using query expansion...")
            matches = self.query_expander.multi_query_search(
                self.hybrid_searcher if use_hybrid else self.embedding_searcher,
                query, top_k=top_k*2, verbose=verbose
            )
        elif use_hybrid:
            if verbose:
                print(f"Using hybrid search (semantic_weight={semantic_weight})...")
            matches = self.hybrid_searcher.hybrid_search(
                query, top_k=top_k*2, semantic_weight=semantic_weight, verbose=verbose
            )
        else:
            if verbose:
                print("Using pure embedding search...")
            matches = self.embedding_searcher.search(query, top_k=top_k*2, verbose=verbose)
        
        # Stage 3: Reranking
        if use_reranking and len(matches) > 0:
            if verbose:
                print(f"\n[Stage 3] Reranking...")
            matches = self.reranker.rerank(query, matches, top_k=top_k, verbose=verbose)
        
        # Stage 4: Extract value
        if verbose:
            print(f"\n[Stage 4] Value extraction (operation: {operation})...")
        
        matches_for_extraction = [(m, f"Match #{i+1}") for i, (m, _) in enumerate(matches)]
        result = self.extractor.extract(matches_for_extraction, operation)
        
        # Calculate confidence
        if matches:
            avg_score = np.mean([score for _, score in matches])
            confidence = float(avg_score)
        else:
            confidence = 0.0
        
        query_result = QueryResult(
            query=query,
            result=result,
            matches=[(m.path, m.value, m.sheet, m.cell_ref) for m, _ in matches[:5]],
            operation=operation,
            confidence=confidence
        )
        
        # Stage 5: Confidence validation
        if verbose:
            print(f"\n[Stage 5] Confidence validation...")
        
        validation = self.confidence_handler.validate_result(query_result, min_confidence)
        
        if verbose:
            print(f"\n{'='*80}")
            print(f"RESULT: {result}")
            print(f"CONFIDENCE: {confidence:.3f}")
            print(f"STATUS: {validation['status']}")
            print(f"{'='*80}\n")
        
        return query_result, validation
    
    def export_flattened(self, output_path: str):
        """Export flattened data for inspection"""
        self.flattener.export_to_csv(output_path)


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

if __name__ == "__main__":
    # Initialize engine
    engine = PrecisionQueryEngine(api_key="your-openai-api-key")
    
    # Load workbook
    engine.load_workbook("financial_report.xlsx")
    
    # Example 1: Maximum precision query
    result, validation = engine.query(
        "What is Q4 2024 revenue?",
        operation="return",
        use_hybrid=True,
        use_reranking=True,
        use_query_expansion=False,
        min_confidence=0.5
    )
    
    print(f"\n=== EXAMPLE 1: Maximum Precision ===")
    print(f"Result: {result.result}")
    print(f"Confidence: {result.confidence:.2%}")
    print(f"Status: {validation['status']}")
    
    # Example 2: Fast query (less precision)
    result2, validation2 = engine.query(
        "What is total revenue?",
        operation="sum",
        use_hybrid=False,
        use_reranking=False,
        use_query_expansion=False,
        top_k=20
    )
    
    print(f"\n=== EXAMPLE 2: Fast Query ===")
    print(f"Result: {result2.result}")
    print(f"Confidence: {result2.confidence:.2%}")
    
    # Example 3: With metadata filtering
    result3, validation3 = engine.query(
        "average profit margin",
        operation="average",
        use_hybrid=True,
        use_reranking=True,
        metadata_filters={"value_type": "percentage"},
        min_confidence=0.4
    )
    
    print(f"\n=== EXAMPLE 3: Metadata Filtering ===")
    print(f"Result: {result3.result}")
    print(f"Confidence: {result3.confidence:.2%}")
    
    # Export flattened data for debugging
    engine.export_flattened("flattened_debug.csv")
    print("\n✓ Flattened data exported to flattened_debug.csv")
