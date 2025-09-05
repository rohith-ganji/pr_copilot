# server/fastmcp_server.py

"""
Main MCP server entry point.
Registers all DB-related tools and runs via stdio.
"""

from mcp.server.fastmcp import FastMCP
from tables import list_tables, get_related_tables
from query import run_query
from metrics import get_metric
from pr_risk import get_pr_summary, get_pr_risk
from query import get_diff_outline


# Initialize MCP server
mcp = FastMCP("DBServer")

# -------------------------------
# DB-related tools
# -------------------------------

@mcp.tool()
def mcp_list_tables(user_prompt: str) -> dict:
    """
    Tool: List Database Tables

    Description:
    - Converts natural language requests into a PostgreSQL query that lists available tables.
    - Scope: Works only on the `insightly` schema.
    - Input: A user request in plain English (e.g., "Show me all tables in the insightly schema").
    - Output: SQL string and query results.
    - Notes: Generates *only SQL* (no explanations, no markdown).

    
    Example Usage:
    >>> list_tables("Show me all tables in the insightly schema")
    {
        "query": "SELECT table_name FROM information_schema.tables WHERE table_schema = 'insightly';",
        "results": [...]
    }
    """
    return list_tables(user_prompt)


@mcp.tool()
def mcp_get_related_tables(user_prompt: str) -> dict:
    """
    Tool: Get Related Tables

    Description:
    - Converts a natural language request into a PostgreSQL query
      that searches for related tables across the `insightly` and `hivel-code-review` schemas.
    - Useful when exploring database relationships, dependencies, or table references.
    - Input: A user request in plain English (e.g., "Find all tables related to pull requests").
    - Output: SQL query and results.
    - Notes: The tool only generates SQL (no explanation, no markdown).

    Example Usage:
    >>> get_related_tables("Find all tables related to pull requests")
    {
        "query": "SELECT table_name FROM information_schema.tables WHERE table_schema IN ('insightly', 'hivel-code-review') AND table_name ILIKE '%pull_request%';",
        "results": [...]
    }
    """
    return get_related_tables(user_prompt)


@mcp.tool()
def mcp_run_query(sql: str, page: int = 1, page_size: int = 50) -> dict:
    """
    MCP Tool: Execute a validated SQL query with pagination.

    Features:
    - Accepts a full SQL `SELECT` (or CTE-based) query as input.
    - Ensures schema-qualified table names via `qualify_tables`.
    - Validates query safety using `is_safe_sql` (schema + safe table guard).
    - Applies pagination with `OFFSET` and enforces a row cap (`page_size`).
    - Returns results as JSON with metadata.

    Args:
        sql (str): A full PostgreSQL SELECT query.
        page (int, optional): Page number (1-based). Defaults to 1.
        page_size (int, optional): Rows per page. Defaults to 50.

    Returns:
        dict[str, Any]:
        - On success:
            {
                "rows": [ {...}, {...} ],
                "page": <int>,
                "page_size": <int>,
                "row_count": <int>
            }
        - On failure:
            {
                "error": "...",
                "sql": "..."   # query attempted (after modification)
            }

    Constraints:
    - Only SELECT/CTE queries are allowed (no INSERT/UPDATE/DELETE).
    - Pagination is always applied (OFFSET + LIMIT).
    - Maximum rows per page = `page_size`.
    - Only tables in `SAFE_TABLES` and approved schemas are permitted.

    Examples:
    >>> run_query("SELECT id, title FROM insightly.pull_request ORDER BY createdon DESC", page=1, page_size=5)
    {
        "rows": [
            {"id": 341, "title": "Fix pagination edge case"},
            {"id": 340, "title": "Improve caching in Redis"},
            ...
        ],
        "page": 1,
        "page_size": 5,
        "row_count": 5
    }

    >>> run_query("DELETE FROM insightly.pull_request")
    {
        "error": "Only SELECT statements are allowed.",
        "sql": "DELETE FROM insightly.pull_request"
    }
    """
    return run_query(sql, page, page_size)


# -------------------------------
# PR / Metrics tools
# -------------------------------

@mcp.tool()
def mcp_get_metric(user_prompt: str) -> dict:
    """
    Compute PR metrics dynamically using a natural language prompt.

    Args:
        user_prompt: Natural language request describing the metric(s) to compute.
                     Examples: 
                     - "Average cycle time for repo 5 in last 30 days"
                     - "Count of PRs grouped by author in last 14 days"

    Returns:
        dict containing:
            - metric: metric key or description
            - window_days: lookback period used
            - sql: generated SQL query
            - params: query parameters
            - explanation: human-readable explanation of what was computed
            - data: list of result rows
            - error: optional error message if failed
    """
    return get_metric(user_prompt)


@mcp.tool()
def mcp_get_pr_summary(user_prompt: str) -> dict:
    """
    Retrieve Pull Request summary by querying `insightly.pull_request`
    and optionally joining with `hivel-code-review.pr_diffs`.

    Capabilities:
    - Converts user requests into SQL queries.
    - Works across `insightly.pull_request` and `hivel-code-review.pr_diffs`.
    - Can filter by PR ID, author, title, repo, state, branches, or date.
    - Dynamically selects relevant columns based on the request.

    Available columns in `pull_request`:
    - id, actualpullrequestid, title, authorid, createdon, description,
      destinationbranch, sourcebranch, state, repoid, linesadded,
      linesremoved, htmllink, commentcount, commitscount, modifiedfilescount,
      labels, organizationid, workspaceid, prsentiment, mergedon,
      declinedon, reviewedon, approvedon, createddate, modifieddate,
      is_deployment_pr, deployment_record_id, etc.

    Available columns in `pr_diffs` (if needed for joins):
    - id, pr_id, filename, file_status, additions, deletions, changes, patch, reviewed_at, review_data

    Example:
    - User: "Get the title, author, and state of PR 341"
    - SQL: SELECT title, authorid, state FROM insightly.pull_request WHERE actualpullrequestid = 341;

    - User: "List filenames and lines changed for PR 341"
    - SQL: SELECT filename, additions, deletions FROM "hivel-code-review".pr_diffs WHERE pr_id = 341;
    """
    return get_pr_summary(user_prompt)



@mcp.tool()
def mcp_get_pr_risk(pr_id: int) -> dict:
    """Estimate the risk of a Pull Request (PR) using LLM analysis.

    This tool evaluates risk based on:
    1. PR summary (from `pull_request` table)
    2. Diff outline (from `pr_diffs` table)

    Capabilities:
    - Fetches relevant PR metadata and diff files.
    - Passes this context to an LLM for structured risk evaluation.
    - Produces a JSON response with a risk score and actionable review comments.

    Expected LLM Output:
    {
        "risk_score": float (0.0–1.0, higher = riskier),
        "comments": [
            "Up to 3 short actionable comments highlighting risk factors."
        ]
    }

    Example:
    >>> get_pr_risk(330)
    {
        "pr_id": 330,
        "risk_score": 0.65,
        "comments": [
            "High number of modified files may increase review complexity.",
            "Check for potential merge conflicts in source branch.",
            "Ensure test coverage for newly added modules."
        ]
    }"""
    return get_pr_risk(pr_id)


@mcp.tool()
def mcp_get_diff_outline(user_prompt: str) -> dict:
    """Retrieve Pull Request diffs from the `hivel-code-review.pr_diffs` table using natural language.

    Capabilities:
    - Converts user requests into SQL queries.
    - Works on the `hivel-code-review.pr_diffs` table.
    - Can filter by pull_request_id, repository_id, filename, status, or date ranges.
    - Dynamically selects relevant columns based on the request.

    Available columns in `pr_diffs`:
    - id, pr_id, repository_id, base_sha, created_date, updated_date,
      pull_request_id, organizationid, userintegrationid, filename,
      file_status, additions, deletions, changes, patch, raw_url,
      blob_url, is_reviewed, last_reviewed_patch, reviewed_at, review_data

    Example:
    - User: "Show me filenames and number of lines changed for PR 123"
    - SQL: SELECT filename, additions, deletions, changes FROM "hivel-code-review".pr_diffs WHERE pr_id = 123;"""
    return get_diff_outline(user_prompt)


# -------------------------------
# Run MCP server
# -------------------------------
if __name__ == "__main__":
    mcp.run(transport="stdio")
