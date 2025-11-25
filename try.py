# ============================================================================
# NEW: TEXT NORMALIZER FOR MAXIMUM PRECISION
# ============================================================================

import re
from typing import Dict, Set

class TextNormalizer:
    """
    Normalizes and preprocesses text for maximum search precision.
    Handles common Excel variations, abbreviations, and formatting.
    """
    
    def __init__(self):
        # Common abbreviations and their expansions
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
        
        # Stopwords that don't add semantic meaning
        self.stopwords = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'been',
            'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
            'could', 'should', 'may', 'might', 'must', 'can', 'this', 'that',
            'these', 'those', 'it', 'its'
        }
    
    def normalize_text(self, text: str, remove_stopwords: bool = False) -> str:
        """
        Comprehensive text normalization for search precision.
        """
        if not isinstance(text, str):
            text = str(text)
        
        # 1. Lowercase
        text = text.lower()
        
        # 2. Remove special characters but keep spaces and alphanumeric
        text = re.sub(r'[^\w\s&-]', ' ', text)
        
        # 3. Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # 4. Expand abbreviations
        words = text.split()
        expanded_words = []
        for word in words:
            # Check if word is an abbreviation
            if word in self.abbreviations:
                expanded_words.append(self.abbreviations[word])
            else:
                expanded_words.append(word)
        
        text = ' '.join(expanded_words)
        
        # 5. Remove stopwords if requested
        if remove_stopwords:
            words = text.split()
            words = [w for w in words if w not in self.stopwords]
            text = ' '.join(words)
        
        # 6. Remove duplicate spaces again
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def normalize_path(self, path: str) -> str:
        """
        Normalize a flattened path for better semantic matching.
        Handles the hierarchical structure specially.
        """
        # Split by delimiter
        parts = path.split('-')
        
        # Normalize each part
        normalized_parts = []
        for part in parts:
            # Remove common noise words
            normalized = self.normalize_text(part, remove_stopwords=False)
            if normalized:  # Only add non-empty parts
                normalized_parts.append(normalized)
        
        # Rejoin with consistent delimiter
        return ' | '.join(normalized_parts)
    
    def add_synonyms(self, text: str) -> str:
        """
        Add synonym variations to improve matching.
        Returns expanded text with synonyms.
        """
        # Common synonyms for financial terms
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
                # Add synonyms
                for syn in synonyms[word].split():
                    if syn != word:
                        expanded.append(syn)
        
        return ' '.join(expanded)


# ============================================================================
# IMPROVED: EMBEDDING SEARCHER WITH PREPROCESSING
# ============================================================================

class ImprovedEmbeddingSearcher:
    """
    Enhanced semantic search with text preprocessing for maximum precision.
    """
    
    def __init__(self, api_key: str, model: str = "text-embedding-3-small"):
        self.api_key = api_key
        self.model = model
        openai.api_key = api_key
        
        self.normalizer = TextNormalizer()
        
        # Cache
        self.path_embeddings: Optional[np.ndarray] = None
        self.flattened_ Optional[List[FlattenedCell]] = None
        self.normalized_paths: List[str] = []  # Store normalized versions
    
    def get_embedding(self, text: str) -> np.ndarray:
        """Get embedding vector for text"""
        response = openai.embeddings.create(
            model=self.model,
            input=text
        )
        return np.array(response.data[0].embedding)
    
    def create_searchable_text(self, entry: FlattenedCell) -> str:
        """
        Create optimized searchable text from flattened entry.
        Combines normalized path + value + synonyms.
        """
        # Normalize the path
        normalized_path = self.normalizer.normalize_path(entry.path)
        
        # Add synonyms for better matching
        expanded_path = self.normalizer.add_synonyms(normalized_path)
        
        # Add value context
        value_str = str(entry.value)
        
        # For numeric values, add semantic context
        if entry.value_type in ["number", "currency", "percentage"]:
            value_context = f"value: {value_str}"
        else:
            value_context = self.normalizer.normalize_text(value_str)
        
        # Combine everything
        searchable = f"{expanded_path} {value_context}"
        
        return searchable
    
    def compute_path_embeddings(self, flattened: List[FlattenedCell], verbose: bool = True):
        """
        Pre-compute embeddings with normalization and preprocessing.
        """
        if verbose:
            print(f"Computing normalized embeddings for {len(flattened)} paths...")
        
        # Create searchable texts with normalization
        texts = []
        self.normalized_paths = []
        
        for entry in flattened:
            searchable = self.create_searchable_text(entry)
            texts.append(searchable)
            self.normalized_paths.append(searchable)
        
        if verbose:
            print("Sample normalized paths:")
            for i, (original, normalized) in enumerate(zip(flattened[:3], self.normalized_paths[:3])):
                print(f"  Original: {original.path}")
                print(f"  Normalized: {normalized}")
                print()
        
        # Batch embed
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
        """
        Preprocess query with same normalization as paths.
        """
        # Normalize
        normalized = self.normalizer.normalize_text(query, remove_stopwords=False)
        
        # Add synonyms
        expanded = self.normalizer.add_synonyms(normalized)
        
        return expanded
    
    def search(self, query: str, top_k: int = 10, verbose: bool = True) -> List[Tuple[FlattenedCell, float]]:
        """
        Search with query preprocessing for maximum precision.
        """
        if self.path_embeddings is None or self.flattened_data is None:
            raise ValueError("Must call compute_path_embeddings() first")
        
        # Preprocess query
        processed_query = self.preprocess_query(query)
        
        if verbose:
            print(f"\nOriginal query: '{query}'")
            print(f"Processed query: '{processed_query}'")
        
        # Embed the processed query
        query_embedding = self.get_embedding(processed_query)
        
        # Compute similarities
        similarities = np.array([
            self.cosine_similarity(query_embedding, path_emb)
            for path_emb in self.path_embeddings
        ])
        
        # Get top K
        top_indices = np.argsort(similarities)[-top_k:][::-1]
        
        matches = [
            (self.flattened_data[idx], float(similarities[idx]))
            for idx in top_indices
        ]
        
        if verbose:
            print(f"\n✓ Top {len(matches)} matches:")
            for i, (entry, score) in enumerate(matches[:5], 1):
                print(f"  {i}. Score: {score:.3f}")
                print(f"     Original path: {entry.path}")
                print(f"     Normalized: {self.normalized_paths[top_indices[i-1]]}")
                print(f"     Value: {entry.value} ({entry.value_type}) [{entry.sheet}!{entry.cell_ref}]")
                print()
        
        return matches


# ============================================================================
# UPDATED: QUERY ENGINE WITH IMPROVED SEARCH
# ============================================================================

class ExcelQueryEngine:
    """Main query engine with normalized semantic search"""
    
    def __init__(self, api_key: str, embedding_model: str = "text-embedding-3-small"):
        self.flattener = ImprovedWorkbookFlattener()
        self.searcher = ImprovedEmbeddingSearcher(api_key, embedding_model)  # NEW
        self.extractor = ValueExtractor()
        self.flattened_ Optional[List[FlattenedCell]] = None
        self.file_path: Optional[str] = None
    
    def load_workbook(self, file_path: str, verbose: bool = True) -> 'ExcelQueryEngine':
        """Load and flatten workbook with normalized embeddings"""
        self.file_path = file_path
        self.flattened_data = self.flattener.flatten(file_path, verbose)
        
        if verbose:
            print("\nComputing normalized embeddings for semantic search...")
        self.searcher.compute_path_embeddings(self.flattened_data, verbose)
        
        return self
    
    def query(self, query: str, operation: str = "return", 
              top_k: int = 10, min_similarity: float = 0.0, 
              verbose: bool = True) -> QueryResult:
        """Query with normalized semantic search"""
        
        if self.flattened_data is None:
            raise ValueError("No workbook loaded. Call load_workbook() first.")
        
        if verbose:
            print("\n" + "="*80)
            print(f"QUERY: {query}")
            print(f"OPERATION: {operation}")
            print("="*80)
        
        # Search with preprocessing
        matches_with_scores = self.searcher.search(query, top_k, verbose)
        
        # Filter by similarity threshold
        if min_similarity > 0:
            matches_with_scores = [
                (entry, score) for entry, score in matches_with_scores 
                if score >= min_similarity
            ]
            if verbose:
                print(f"✓ Filtered to {len(matches_with_scores)} matches with similarity >= {min_similarity}")
        
        # Convert for extractor
        matches = [(entry, f"Match #{i+1}") for i, (entry, score) in enumerate(matches_with_scores)]
        
        # Extract value
        if verbose:
            print(f"\nExtracting value (operation: {operation})...")
        
        result = self.extractor.extract(matches, operation)
        
        # Calculate confidence
        if matches_with_scores:
            avg_similarity = np.mean([score for _, score in matches_with_scores])
            confidence = float(avg_similarity)
        else:
            confidence = 0.0
        
        if verbose:
            print(f"\n{'='*80}")
            print(f"RESULT: {result}")
            print(f"CONFIDENCE: {confidence:.3f}")
            print(f"{'='*80}\n")
        
        return QueryResult(
            query=query,
            result=result,
            matches=[(m.path, m.value, m.sheet, m.cell_ref) for m, _ in matches],
            operation=operation,
            confidence=confidence
        )
    
    def export_flattened(self, output_path: str, format: str = "csv"):
        """Export flattened data"""
        if format == "csv":
            self.flattener.export_to_csv(output_path)
        elif format == "json":
            self.flattener.export_to_json(output_path)
    
    def get_sample_paths(self, n: int = 20) -> List[str]:
        """Get sample paths"""
        if self.flattened_data is None:
            return []
        return [entry.path for entry in self.flattened_data[:n]]


# ============================================================================
# USAGE WITH MAXIMUM PRECISION
# ============================================================================

if __name__ == "__main__":
    engine = ExcelQueryEngine(api_key="your-key")
    engine.load_workbook("financial_report.xlsx")
    
    # Query with preprocessing
    result = engine.query(
        "What is Q4 revenue?",  # Works with abbreviations
        operation="return",
        min_similarity=0.4  # Higher threshold for precision
    )
    
    print(f"\nAnswer: {result.result}")
    print(f"Confidence: {result.confidence:.2%}")
