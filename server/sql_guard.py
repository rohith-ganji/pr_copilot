"""
SQL Guard: validates SQL queries for safety.
Ensures only SELECTs are allowed and only approved tables can be queried.
"""

import re

# Default safe tables (overridable by caller)
DEFAULT_SAFE_TABLES = {"insightly.pull_request",
    "insightly.commit",
    
    "hivel-code-review.pr_diffs",  "information_schema.tables", "information_schema.columns"}


def is_safe_sql(sql: str, schema_guard: bool = True, safe_tables: set[str] | None = None) -> tuple[bool, str]:
    """
    Validate SQL query safety.
    Returns (is_safe, reason).
    """
    sql = sql.strip().rstrip(";")

    # Normalize to lowercase for parsing (but don’t change actual SQL)
    lowered = sql.lower()

    # --- 1. Must start with SELECT or WITH
    if not (lowered.startswith("select") or lowered.startswith("with")):
        return False, "Only SELECT/CTE queries are allowed."

    # --- 2. Disallow multiple statements
    if ";" in lowered:
        return False, "Multiple SQL statements are not allowed."

    # --- 3. Allowed tables
    allowed_tables = safe_tables or DEFAULT_SAFE_TABLES

    # Extract possible table names with regex
    # This looks for "from <table>" and "join <table>"
    table_matches = re.findall(r"\b(from|join)\s+([a-zA-Z0-9_.]+)", lowered)

    for _, table in table_matches:
        if table not in allowed_tables:
            return False, f"Access to table '{table}' is not allowed."

    return True, "Safe"


def enforce_limit(sql: str, row_limit: int = 50) -> str:
    """
    Ensure query has a LIMIT. If present, cap it at row_limit.
    """
    sql = sql.strip().rstrip(";")
    lowered = sql.lower()

    limit_pattern = re.compile(r"limit\s+(\d+)", re.IGNORECASE)
    match = limit_pattern.search(sql)

    if match:
        existing = int(match.group(1))
        if existing > row_limit:
            sql = limit_pattern.sub(f"LIMIT {row_limit}", sql)
    else:
        sql = f"{sql} LIMIT {row_limit}"

    return sql
