"""
Financial Excel Query Engine V6 (Multi-Level Header Fixed)
------------------------------------------------------------
Handles vertical/horizontal subheader alignment + spanning headers.
"""

# ... [Keep all imports and previous classes unchanged until AdvancedStructureDetector] ...

class AdvancedStructureDetector:
    def __init__(self, config: QueryEngineConfig):
        self.config = config
    
    def analyze_sheet(self, df: pd.DataFrame, bold_grid: List[List[bool]]) -> List[TableBounds]:
        """Enhanced multi-pass analysis with subheader detection."""
        tables = []
        i = 0
        
        while i < len(df):
            # Pass 1: Find potential header blocks (more aggressive)
            header_info = self._detect_extended_header_block(df, bold_grid, i)
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
    
    def _detect_extended_header_block(self, df: pd.DataFrame, bold_grid: List[List[bool]], start_row: int) -> Optional[Tuple[int, int]]:
        """Detect multi-level headers by pattern similarity + text density."""
        header_start = start_row
        
        # First find obvious header rows
        obvious_headers = []
        for r in range(start_row, min(start_row + 8, len(df))):
            row_vals = df.iloc[r]
            if self._is_header_row(row_vals, bold_grid[r]):
                obvious_headers.append(r)
        
        if not obvious_headers:
            return None
        
        # Extend upward/downward looking for pattern matches
        header_start = min(obvious_headers)
        header_end = max(obvious_headers) + 1
        
        # Extend downward if next rows have similar sparsity/structure
        current_sparsity = self._calculate_row_sparsity(df.iloc[header_end-1])
        for r in range(header_end, min(header_end + 3, len(df))):
            next_sparsity = self._calculate_row_sparsity(df.iloc[r])
            sparsity_similar = abs(current_sparsity - next_sparsity) < 0.3
            text_ratio = self._calculate_text_ratio(df.iloc[r])
            
            if sparsity_similar and text_ratio > 0.5:
                header_end = r + 1
            else:
                break
        
        return (header_start, header_end - header_start)
    
    def _calculate_row_sparsity(self, row: pd.Series) -> float:
        """Calculate how sparse a row is (non-null cells / total cells)."""
        non_null = row.notna().sum()
        total = len(row)
        return non_null / total if total > 0 else 0
    
    def _calculate_text_ratio(self, row: pd.Series) -> float:
        """Text vs total non-null ratio."""
        clean_row = row.dropna()
        if len(clean_row) == 0:
            return 0.0
        text_count = sum(1 for x in clean_row if isinstance(x, str))
        return text_count / len(clean_row)
    
    def build_hierarchical_headers(self, df: pd.DataFrame, table: TableBounds) -> List[List[str]]:
        """Build complete hierarchical paths for each column, handling spanning."""
        header_end = min(table.top_row + table.header_rows, len(df))
        header_block = df.iloc[table.top_row:header_end].copy()
        
        # Forward fill horizontally (handles spanning like "2023" over Q1,Q2,Q3)
        header_block = header_block.ffill(axis=1)
        
        # Backward fill to handle right-aligned headers
        header_block = header_block.bfill(axis=1)
        
        col_paths = []
        for c in range(table.left_col, table.right_col + 1):
            if c >= len(header_block.columns):
                col_paths.append([])
                continue
                
            # Build vertical path for this column
            raw_path = header_block.iloc[:, c].tolist()
            # Clean and filter
            clean_path = []
            for level_val in raw_path:
                if pd.notna(level_val) and str(level_val).strip():
                    clean_path.append(str(level_val).strip())
            
            col_paths.append(clean_path)
        
        return col_paths
    
    def classify_data_row(self, row: pd.Series, bold_row: List[bool], table_bounds: TableBounds) -> List[CellRole]:
        """Enhanced classification with subheader awareness."""
        roles = []
        non_null_count = sum(1 for val in row if pd.notna(val))
        
        if non_null_count == 0:
            for col_idx in range(len(row)):
                roles.append(CellRole(col_idx, col_idx, None, 'empty', 
                                    bold_row[col_idx] if col_idx < len(bold_row) else False))
            return roles
        
        # Enhanced numeric vs string analysis
        numeric_count = 0
        string_positions = []
        
        for idx, val in enumerate(row):
            if pd.notna(val):
                if self._is_numeric_like(val):
                    numeric_count += 1
                else:
                    string_positions.append(idx)
        
        string_ratio = len(string_positions) / non_null_count if non_null_count > 0 else 0
        is_numeric_row = (numeric_count / non_null_count > (1 - self.config.numeric_row_string_threshold)) if non_null_count > 0 else False
        
        for col_idx, val in enumerate(row):
            is_bold = bold_row[col_idx] if col_idx < len(bold_row) else False
            in_bounds = (table_bounds.left_col <= col_idx <= table_bounds.right_col)
            
            if pd.isna(val) or not in_bounds:
                role = 'empty'
            elif is_numeric_row and col_idx in string_positions:
                role = 'row_header'
            elif self._is_numeric_like(val):
                role = 'value'
            else:
                # Check if this looks like a continuation header (sparse row, bold, text)
                row_density = self._calculate_row_sparsity(pd.Series(row))
                if is_bold and row_density < 0.3:
                    role = 'row_header'
                else:
                    role = 'row_header'
            
            roles.append(CellRole(col_idx, col_idx, val, role, is_bold))
        
        return roles

# ==========================================
# 5. Enhanced Main Engine (Fixed Multi-Level)
# ==========================================

class FinancialExcelEngineV6:
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

        print("🔍 Multi-level header analysis...")
        self._ingest_file_enhanced()
        
        if config.semantic_backend == 'openai':
            print("📊 Building vector index...")
            self._build_embeddings()
        print(f"✅ Engine ready: {len(self.records)} values, {len(self.tables)} tables")

    def _ingest_file_enhanced(self):
        """Enhanced ingestion with proper multi-level header handling."""
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
                    
                    # Build PROPER hierarchical column paths
                    col_paths = self.detector.build_hierarchical_headers(df, table)
                    
                    # Process data rows
                    data_start = table.top_row + table.header_rows
                    for r_idx in range(data_start, table.bottom_row):
                        if r_idx >= len(df) or r_idx >= len(bold_grid):
                            break
                            
                        row_data = df.iloc[r_idx]
                        row_roles = self.detector.classify_data_row(
                            row_data, bold_grid[r_idx], table
                        )
                        
                        # Find row label (first row_header)
                        row_header_candidates = [role for role in row_roles if role.role == 'row_header']
                        if not row_header_candidates:
                            continue
                        
                        row_label = str(row_header_candidates[0].value).strip()
                        
                        # Process ONLY value cells
                        for cell_role in row_roles:
                            if cell_role.role != 'value':
                                continue
                            
                            col_idx_in_table = cell_role.col - table.left_col
                            if 0 <= col_idx_in_table < len(col_paths):
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

    # ... [Keep _build_embeddings, _get_type, _extract_critical_tokens, query methods unchanged from V5] ...

# ==========================================
# Demo with Multi-Level Headers
# ==========================================
if __name__ == "__main__":
    dummy_file = "financial_demo.xlsx"
    if not os.path.exists(dummy_file):
        print("Creating multi-level demo...")
        # Simulates: Year header spanning quarters
        df = pd.DataFrame({
            'Metric': ['Revenue', 'COGS', 'Gross Profit'],
            '2023': ['Q1', 'Q2', 'Q3'],  # Subheaders under 2023 span
            '100': [110, 120, None],
            '40': [42, 45, None],
            '60': [68, 75, None]
        })
        df.columns = ['Metric', '2023_Q1', '2023_Q2', '2023_Q3', 'Other']
        with pd.ExcelWriter(dummy_file, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='P&L', index=False)

    config = QueryEngineConfig(semantic_backend='basic', min_confidence=0.4)
    engine = FinancialExcelEngineV6(dummy_file, config)
    
    print("\nTesting multi-level header detection:")
    for q in ["Revenue 2023", "Gross Profit Q1"]:
        results = engine.query(q)
        print(f"  '{q}' -> {[r.header_path for r in results]}")
