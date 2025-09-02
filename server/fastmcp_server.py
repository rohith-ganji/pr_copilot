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

# Register tools
@mcp.tool()
def mcp_list_tables() -> dict:
    """List all user tables with row counts."""
    return list_tables()


@mcp.tool()
def mcp_get_related_tables(table_name: str) -> dict:
    """Return known relationships for a given table."""
    return get_related_tables(table_name)


@mcp.tool()
def mcp_run_query(sql: str, page: int = 1, page_size: int = 50) -> dict:
    """Run a safe SQL SELECT query with pagination."""
    return run_query(sql, page, page_size)


@mcp.tool()
def mcp_get_metric(metric_name: str, window_days: int = 30, filters: dict | None = None) -> dict:
    """Compute derived metrics on pull requests (cycle_time, review_latency, churn, throughput)."""
    return get_metric(metric_name, window_days, filters)


@mcp.tool()
def mcp_get_pr_summary(pr_id: int) -> dict:
    """Return a structured summary of the given pull request."""
    return get_pr_summary(pr_id)

@mcp.tool()
def mcp_get_pr_risk(pr_id: int) -> dict:
    """Evaluate risk heuristics for a pull request (size, churn, delays)."""
    return get_pr_risk(pr_id)

@mcp.tool()
def mcp_get_diff_outline(pr_id: int) -> dict:
    """Get changed files outline (filename, status, size, review flags) for a PR."""
    return get_diff_outline(pr_id)


if __name__ == "__main__":
    mcp.run(transport="stdio")
