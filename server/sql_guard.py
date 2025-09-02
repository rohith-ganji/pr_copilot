import re

# Only allow reading from these tables
SAFE_TABLES = {"pull_request", "commit", "pr_diff"}

# Block obvious destructive keywords
FORBIDDEN = {
    "insert", "update", "delete", "drop", "alter",
    "truncate", "create", "grant", "revoke"
}


def is_safe_sql(sql: str, schema_guard: bool = True) -> tuple[bool, str | None]:
    """
    Light-weight validator:
      - Only SELECT (or WITH ... SELECT) queries
      - No destructive keywords
      - Optional schema guard against unapproved tables
      - Single statement only
    Returns: (ok, reason)
    """
    if not sql or not sql.strip():
        return False, "Empty SQL."

    # Disallow multiple statements (e.g., "SELECT ...; DELETE ...")
    # Allow at most one trailing semicolon/spaces
    stmts = [s for s in sql.split(";") if s.strip()]
    if len(stmts) > 1:
        return False, "Multiple statements are not allowed."

    sql_lower = sql.strip().lower()

    # Must start with SELECT or WITH (CTE)
    if not (sql_lower.startswith("select") or sql_lower.startswith("with")):
        return False, "Only SELECT queries (or WITH CTEs) are allowed."

    # Block destructive tokens
    for word in FORBIDDEN:
        if re.search(rf"\b{word}\b", sql_lower):
            return False, f"Query contains forbidden keyword: {word.upper()}"

    # Guard table access
    if schema_guard:
        # Extract table names from FROM/JOIN
        for match in re.finditer(r"(from|join)\s+([a-zA-Z_][\w\.]*)", sql_lower):
            table = match.group(2).split(".")[-1]
            if table not in SAFE_TABLES:
                return False, f"Access to table '{table}' is not allowed."

    return True, None


def enforce_limit(sql: str, row_limit: int = 50) -> str:
    """
    Ensure the query has a LIMIT, and cap it to row_limit.
    - If no LIMIT, append 'LIMIT {row_limit}'
    - If LIMIT > row_limit, cap it
    Leaves any existing OFFSET intact.
    """
    # Strip trailing semicolon to make appending easier
    base = sql.strip().rstrip(";")
    lower = base.lower()

    # Find an existing LIMIT
    m = re.search(r"\blimit\s+(\d+)\b", lower)
    if m:
        # Cap existing limit if it's higher than allowed
        current = int(m.group(1))
        if current > row_limit:
            # Replace just the numeric part
            def _cap(match: re.Match) -> str:
                return match.group(0).replace(str(current), str(row_limit), 1)
            base = re.sub(r"\blimit\s+(\d+)\b", _cap, base, count=1, flags=re.IGNORECASE)
        return base

    # No LIMIT found → append one (before trailing spaces)
    return f"{base} LIMIT {row_limit}"
