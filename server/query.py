"""
Query execution tool with guardrails, schema qualification, and pagination.
"""

import re
from typing import Any
from psycopg2.extras import RealDictCursor
from connection import get_connection
from sql_guard import is_safe_sql, enforce_limit

# Allowed tables and schema
SAFE_TABLES = {"pull_request", "commit", "pr_diff"}
SCHEMA = "insightly"


def qualify_tables(sql: str) -> str:
    """
    Ensure all safe tables are schema-qualified (insightly.table).
    Example: 'FROM pull_request' -> 'FROM insightly.pull_request'
    """
    for table in SAFE_TABLES:
        pattern = rf"\b{table}\b"
        sql = re.sub(pattern, f"{SCHEMA}.{table}", sql)
    return sql


def run_query(sql: str, page: int = 1, page_size: int = 50) -> dict[str, Any]:
    """
    Safely run a SELECT/CTE query with pagination + row caps.

    - Auto-qualifies bare table names with schema (insightly.*)
    - Validates SQL (read-only, allowed tables, single statement)
    - Enforces LIMIT (caps each page to page_size)
    - Applies OFFSET for true pagination

    Returns:
      {
        "rows": [ {...}, ... ],
        "page": int,
        "page_size": int,
        "row_count": int
      }
      or { "error": str }
    """
    # Ensure schema qualification first
    sql = qualify_tables(sql)

    safe, reason = is_safe_sql(sql, schema_guard=True)
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


def get_diff_outline(pr_id: int) -> dict:
    """
    Outline changed files in a PR from pr_diffs table.
    Returns filename, status, additions, deletions, review info.
    """
    sql = """
    SELECT filename, file_status, additions, deletions, changes,
           is_reviewed, reviewed_at
    FROM "hivel-code-review".pr_diffs
    WHERE pull_request_id = %s
    ORDER BY changes DESC
    LIMIT 100
"""

    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, (pr_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            return {"error": f"No diff records for PR id={pr_id}"}

        outline = []
        for r in rows:
            outline.append({
                "filename": r.get("filename"),
                "status": r.get("file_status"),
                "additions": r.get("additions"),
                "deletions": r.get("deletions"),
                "changes": r.get("changes"),
                "is_reviewed": r.get("is_reviewed"),
                "reviewed_at": r.get("reviewed_at"),
            })

        return {"pr_id": pr_id, "diff_outline": outline}

    except Exception as e:
        return {"error": str(e)}
