# server/tables.py

import psycopg2
from psycopg2.extras import RealDictCursor
from connection import get_connection

# -------------------------
# Tool: list_tables
# -------------------------
def list_tables():
    """
    List only user tables in the 'insightly' schema with true row counts (COUNT(*)).
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Get table names
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'insightly'
          AND table_type = 'BASE TABLE';
    """)
    tables = cur.fetchall()

    results = []
    for t in tables:
        name = t["table_name"]
        cur.execute(f'SELECT COUNT(*) AS row_count FROM insightly."{name}";')
        count = cur.fetchone()["row_count"]
        results.append({"table_name": name, "row_count": count})

    cur.close()
    conn.close()

    return {"tables": results}


# -------------------------
# Tool: get_related_tables
# -------------------------
def get_related_tables(table_name: str):
    """
    Given a table name, find other tables in the schema that are related to it 
    based on column naming conventions for foreign keys.

    Logic:
    - Looks at all columns in the schemas 'insightly' and 'hivel-code-review'.
    - For each column in the given table:
        * If the column name matches the pattern `<other_table>id` or `<other_table>_id`,
          then that column is assumed to reference the `id` column of `<other_table>`.
    - Returns a list of related tables with the inferred join condition.

    Args:
        table_name (str): The name of the table for which to find related tables.

    Returns:
        dict: {
            "table": <input table name>,
            "related": [
                {"table": <related_table>, "via": "<table.column = related_table.id>"},
                ...
            ]
        }

    Example:
        >>> get_related_tables("pr_update")
        {
            "table": "pr_update",
            "related": [
                {"table": "pull_request", "via": "pr_update.pullrequestid = pull_request.id"}
            ]
        }
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Get all columns in the schema
    cur.execute("""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema IN ('insightly', 'hivel-code-review');
    """)
    cols = cur.fetchall()
    cur.close()
    conn.close()

    # Normalize
    tables = set([c["table_name"] for c in cols])
    related = []

    for col in [c for c in cols if c["table_name"] == table_name]:
        cname = col["column_name"].lower()
        for t in tables:
            if cname == f"{t}id" or cname == f"{t}_id":
                related.append({"table": t, "via": f"{table_name}.{cname} = {t}.id"})

    return {"table": table_name, "related": related}


