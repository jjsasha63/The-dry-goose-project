import json
from typing import Dict, Any, List, Optional, Literal
import openpyxl
from openpyxl.utils import get_column_letter
import pandas as pd
import openai
import tiktoken

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------

OPENAI_API_KEY = "YOUR_KEY"
OPENAI_MODEL = "gpt-4.1-mini"  # or gpt-4.1 / Azure deployment

openai.api_key = OPENAI_API_KEY

try:
    encoding = tiktoken.encoding_for_model(OPENAI_MODEL)
except KeyError:
    encoding = tiktoken.get_encoding("cl100k_base")

def count_tokens(text: str) -> int:
    return len(encoding.encode(text))

# ------------------------------------------------------------
# 1. LOAD + INDEX WORKBOOK STRUCTURE (ALL SHEETS)
# ------------------------------------------------------------

def load_workbook_structure(path: str, max_rows_preview: int = 40, max_cols_preview: int = 20) -> Dict[str, Any]:
    """
    Load ALL sheets and build a compact structural index:
    - dimensions
    - header row candidates
    - bold / label-like cells
    - semantic preview grid
    """
    wb = openpyxl.load_workbook(path, data_only=True)
    structure = {
        "file_path": path,
        "sheets": {}
    }

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        max_row = min(ws.max_row, max_rows_preview)
        max_col = min(ws.max_column, max_cols_preview)

        headers = []
        bold_labels = []
        grid_preview = []

        for r in range(1, max_row + 1):
            row_values = []
            for c in range(1, max_col + 1):
                cell = ws.cell(r, c)
                val = cell.value
                if val is None:
                    row_values.append("")
                    continue

                text_val = str(val)
                row_values.append(text_val[:40])

                # simple header heuristic: top 3 rows with mostly text
                if r <= 3 and isinstance(val, str) and val.strip():
                    headers.append({
                        "cell": f"{get_column_letter(c)}{r}",
                        "value": text_val
                    })

                # label-like: bold or leftmost column text
                if isinstance(val, str) and (cell.font and cell.font.bold or c == 1):
                    bold_labels.append({
                        "cell": f"{get_column_letter(c)}{r}",
                        "value": text_val
                    })

            if any(x for x in row_values):
                grid_preview.append({
                    "row": r,
                    "cells": row_values
                })

        structure["sheets"][sheet_name] = {
            "dimensions": {"rows": ws.max_row, "cols": ws.max_column},
            "headers": headers[:40],
            "labels": bold_labels[:80],
            "grid_preview": grid_preview
        }

    wb.close()
    return structure

# ------------------------------------------------------------
# 2. LLM: PLAN GENERATION (WHAT / WHERE), NOT ANSWER
# ------------------------------------------------------------

PLAN_SYSTEM_PROMPT = """
You are a planning assistant for Excel workbooks.

You NEVER compute numeric answers yourself.
You ONLY produce a MACHINE-READABLE PLAN that tells a separate engine:
- which sheet(s) to read
- which columns/rows/cells to use
- what filters to apply
- what aggregation to perform

The engine will execute the plan directly on the workbook and compute the final answer.

You must ALWAYS return valid JSON with this exact schema:

{
  "targets": [
    {
      "sheet": "sheet name",
      "range": "e.g. B2:B13 or A2:D100 or single cell 'C7'",
      "role": "value" | "label" | "filter" | "lookup_key"
    }
  ],
  "operations": [
    {
      "type": "sum" | "avg" | "max" | "min" | "lookup" | "filter_then_sum" | "return_cell",
      "description": "natural language description of what to do algorithmically",
      "on": "reference to one or more ranges from targets, e.g. ['Sheet1!B2:B13']",
      "filters": [
        {
          "sheet": "sheet name",
          "column": "column letter or header name if known",
          "condition": "python-style boolean expression, e.g. value == 'Q4 2024' or value > 0"
        }
      ]
    }
  ],
  "expected_type": "number | currency | percentage | text | date",
  "notes": "any clarification that helps the engine, e.g. 'Q4 is the row where column A == Q4'"
}
"""

def build_structure_prompt(structure: Dict[str, Any], query: str, max_tokens: int = 8000) -> str:
    """
    Turn the workbook structural index into a compact prompt.
    Only previews + headers/labels, no full data dump.
    """
    parts: List[str] = []
    parts.append(f"FILE: {structure['file_path']}")
    parts.append("This is a structural summary of all sheets. Values are truncated previews.\n")

    for sheet_name, info in structure["sheets"].items():
        parts.append(f"=== SHEET: {sheet_name} ===")
        dims = info["dimensions"]
        parts.append(f"Dimensions: {dims['rows']} rows x {dims['cols']} cols")

        if info["headers"]:
            h_preview = [f"{h['cell']}='{h['value']}'" for h in info["headers"][:10]]
            parts.append("Headers: " + " | ".join(h_preview))

        if info["labels"]:
            l_preview = [f"{l['cell']}='{l['value']}'" for l in info["labels"][:10]]
            parts.append("Labels: " + " | ".join(l_preview))

        # brief grid preview
        parts.append("Preview rows:")
        for row in info["grid_preview"][:8]:
            # show first 6 non-empty cells
            cells = [v for v in row["cells"] if v][:6]
            if cells:
                parts.append(f"  Row {row['row']}: " + " | ".join(cells))

        parts.append("")  # blank line between sheets

        text_so_far = "\n".join(parts)
        if count_tokens(text_so_far) > max_tokens:
            parts.append("... (remaining sheets omitted to stay within context limit)")
            break

    parts.append("\nUSER QUERY:")
    parts.append(query)

    parts.append("""
Return ONLY the JSON plan (no explanation text).
Remember: you do NOT compute the numeric result, only tell the engine WHERE and WHAT to compute.
""")

    return "\n".join(parts)

def get_llm_plan(structure: Dict[str, Any], query: str, expected_type: str = "auto") -> Dict[str, Any]:
    prompt = build_structure_prompt(structure, query)

    messages = [
        {"role": "system", "content": PLAN_SYSTEM_PROMPT},
        {"role": "user", "content": prompt}
    ]

    resp = openai.ChatCompletion.create(
        model=OPENAI_MODEL,
        messages=messages,
        temperature=0.0,
        max_tokens=1024,
        response_format={"type": "json_object"}
    )

    plan_str = resp.choices[0].message.content
    plan = json.loads(plan_str)

    # inject expected_type if you want to override
    if expected_type != "auto":
        plan["expected_type"] = expected_type

    return plan

# ------------------------------------------------------------
# 3. EXECUTION ENGINE: ALGORITHMIC PLAN EXECUTION
# ------------------------------------------------------------

def range_to_indices(ws, rng: str):
    """
    Convert a range like 'B2:B10' or 'A2:D100' or 'C7' to (min_row, max_row, min_col, max_col).
    """
    if "!" in rng:
        rng = rng.split("!", 1)[1]

    if ":" in rng:
        start, end = rng.split(":")
    else:
        start = end = rng

    start_cell = ws[start]
    end_cell = ws[end]

    min_row = min(start_cell.row, end_cell.row)
    max_row = max(start_cell.row, end_cell.row)
    min_col = min(start_cell.column, end_cell.column)
    max_col = max(start_cell.column, end_cell.column)

    return min_row, max_row, min_col, max_col

def sheet_to_dataframe(ws) -> pd.DataFrame:
    """
    Convert an entire sheet to a pandas DataFrame with integer columns.
    No header inference here; we'll use indices or additional info from plan.
    """
    data = []
    max_row = ws.max_row
    max_col = ws.max_column

    for r in range(1, max_row + 1):
        row_vals = []
        for c in range(1, max_col + 1):
            row_vals.append(ws.cell(r, c).value)
        data.append(row_vals)

    df = pd.DataFrame(data)
    return df

def apply_filters(df: pd.DataFrame, flt: Dict[str, Any], header_row: Optional[int] = None) -> pd.DataFrame:
    """
    Apply a single filter described in the plan.
    - flt['column'] can be a column letter (A,B,...) or header name if header_row is known.
    - flt['condition'] is a python expression in terms of 'value'.
    """
    cond = flt["condition"]
    col_spec = flt["column"]

    if isinstance(col_spec, str) and len(col_spec) == 1 and col_spec.isalpha():
        col_idx = ord(col_spec.upper()) - ord("A")
    else:
        # assume header name; find in header row
        if header_row is None:
            raise ValueError("Header row not provided but filter uses header name")
        header_series = df.iloc[header_row - 1, :]
        matches = [i for i, v in enumerate(header_series) if str(v).strip().lower() == col_spec.strip().lower()]
        if not matches:
            raise ValueError(f"Header '{col_spec}' not found")
        col_idx = matches[0]

    series = df.iloc[:, col_idx]

    mask = []
    for v in series:
        value = v
        try:
            keep = bool(eval(cond, {"__builtins__": {}}, {"value": value}))
        except Exception:
            keep = False
        mask.append(keep)

    return df[pd.Series(mask).values]

def execute_plan_on_workbook(path: str, plan: Dict[str, Any]) -> Any:
    wb = openpyxl.load_workbook(path, data_only=True)

    # Pre-load all DFS for convenient pandas ops
    dfs: Dict[str, pd.DataFrame] = {name: sheet_to_dataframe(wb[name]) for name in wb.sheetnames}

    def parse_range_string(sheet_name: str, rng: str):
        ws = wb[sheet_name]
        r1, r2, c1, c2 = range_to_indices(ws, rng)
        df = dfs[sheet_name]
        sub = df.iloc[r1-1:r2, c1-1:c2]
        return sub

    result_value = None

    for op in plan.get("operations", []):
        op_type = op["type"]
        on_refs = op["on"]

        if op_type in ("sum", "filter_then_sum"):
            total = 0.0
            for ref in on_refs:
                # ref like "Sheet1!B2:B13" or just "B2:B13" with implicit sheet
                if "!" in ref:
                    sheet_name, rng = ref.split("!", 1)
                else:
                    # fall back to first sheet mentioned in targets
                    sheet_name = plan["targets"][0]["sheet"]
                    rng = ref

                sub = parse_range_string(sheet_name, rng)

                if op_type == "filter_then_sum" and op.get("filters"):
                    for flt in op["filters"]:
                        if flt["sheet"] != sheet_name:
                            continue
                        # assume first row header if header name based
                        sub = apply_filters(sub, flt, header_row=1)

                total += pd.to_numeric(sub.values.flatten(), errors="coerce").sum(skipna=True)

            result_value = total

        elif op_type == "return_cell":
            # Expect exactly one ref
            ref = on_refs[0]
            if "!" in ref:
                sheet_name, cell_ref = ref.split("!", 1)
            else:
                sheet_name = plan["targets"][0]["sheet"]
                cell_ref = ref
            ws = wb[sheet_name]
            result_value = ws[cell_ref].value

        elif op_type == "lookup":
            # Implement simple VLOOKUP-like op if needed
            # (left as extension point)
            raise NotImplementedError("lookup not implemented in this snippet")

        else:
            raise ValueError(f"Unsupported operation type: {op_type}")

    wb.close()
    return result_value

# ------------------------------------------------------------
# 4. END-TO-END USAGE
# ------------------------------------------------------------

def answer_query_from_excel(path: str, query: str, expected_type: str = "auto") -> Dict[str, Any]:
    # 1) index structure
    structure = load_workbook_structure(path)

    # 2) ask LLM for a plan (NO numeric answer)
    plan = get_llm_plan(structure, query, expected_type)

    # 3) execute deterministically
    value = execute_plan_on_workbook(path, plan)

    return {
        "query": query,
        "plan": plan,
        "value": value
    }

if __name__ == "__main__":
    path = "financial_report_2024.xlsx"
    query = "What is the total revenue for Q4 2024 across all sheets?"

    result = answer_query_from_excel(path, query, expected_type="currency")
    print("PLAN:")
    print(json.dumps(result["plan"], indent=2))
    print("\nRESULT VALUE (computed algorithmically):", result["value"])
