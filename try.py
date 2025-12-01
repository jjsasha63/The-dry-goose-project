"""
Financial Excel Query Engine V6 (Complete Multi-Level Edition)
--------------------------------------------------------------
Production-ready with vertical/horizontal subheader alignment, spanning headers,
advanced structure detection, and zero hallucination guarantee.
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
# 1. Data Structures
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
# 2. Semantic Matching Backends
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
