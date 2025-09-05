"""
Query execution tool with guardrails, schema qualification, and pagination.
"""

import re
from typing import Any
from psycopg2.extras import RealDictCursor
from connection import get_connection
from sql_guard import is_safe_sql, enforce_limit
import os
from dotenv import load_dotenv
from openai import OpenAI

# --- App tables (bare names only) ---
APP_TABLES = {"pull_request", "commit", "pr_diffs"}

# Metadata tables under "information_schema"
META_TABLES = {
    "information_schema.tables",
    "information_schema.columns",
}

# Schemas we care about
SCHEMAS = {"insightly", "hivel-code-review"}

# ✅ Build safe tables with schema qualification
SAFE_TABLES = {f"{schema}.{tbl}" for schema in SCHEMAS for tbl in APP_TABLES} | META_TABLES

DEFAULT_SCHEMA = "insightly"


def qualify_tables(sql: str) -> str:
    """
    Ensure bare table names are schema-qualified.
    - 'FROM pull_request' -> 'FROM insightly.pull_request'
    - Leaves already-qualified names (insightly.pull_request, hivel-code-review.pr_diff) untouched
    - Leaves metadata tables (information_schema.*) untouched
    """
    for table in APP_TABLES:
        # Match bare table names not preceded by a schema (insightly. or hivel-code-review.)
        pattern = rf"(?<!\.)\b{table}\b"
        replacement = f"{DEFAULT_SCHEMA}.{table}"
        sql = re.sub(pattern, replacement, sql)
    return sql


def run_query(sql: str, page: int = 1, page_size: int = 50) -> dict[str, Any]:
    """
    Safely run a SELECT/CTE query with pagination + row caps.
    """
    # Ensure schema qualification first
    sql = qualify_tables(sql)

    # Validate query
    safe, reason = is_safe_sql(sql, schema_guard=True, safe_tables=SAFE_TABLES)
    if not safe:
        return {"error": reason, "sql": sql}

    try:
        offset = max(0, (int(page) - 1)) * int(page_size)
    except Exception:
        return {"error": "Invalid page or page_size."}

    # Enforce per-page limit
    limited_sql = enforce_limit(sql, row_limit=page_size)
    paginated_sql = f"{limited_sql} OFFSET {offset}"

    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(paginated_sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        return {
            "rows": rows,
            "page": page,
            "page_size": page_size,
            "row_count": len(rows),
        }
    except Exception as e:
        return {"error": str(e), "sql": paginated_sql}


load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def get_diff_outline(user_prompt: str):
    """
    Uses OpenAI LLM to generate SQL for retrieving PR diffs
    from `hivel-code-review.pr_diffs` table,
    based on the user's natural language prompt.
    """

    llm_prompt = f"""
    Convert the following request into a PostgreSQL query.

    User request: "{user_prompt}"

    Constraints:
    - Schema: hivel-code-review, in query:..'hivel-code-review'.pr_diffs..
    - Table: pr_diffs
    - Only generate SQL (no explanation, no markdown).
    - When need to use id, use actualpullrequestid from pull_request table, pr_id from pr_diffs table
    - Must filter by pull_request_id if relevant.
    - When accessing-hivel-code-review, in query:..'hivel-code-review'.pr_diffs..

    Example queries:
    
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a SQL generator."},
            {"role": "user", "content": llm_prompt},
        ],
    )

    sql = response.choices[0].message.content.strip()

    # Safety check
    safe, reason = is_safe_sql(sql, schema_guard=True)
    if not safe:
        return {"error": reason, "query": sql}

    # Run query
    results = run_query(sql)
    return {"query": sql, "results": results}

