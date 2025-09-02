"""
PR metrics calculations (enhanced).

Features:
- Named metric mapping (METRIC_MAP).
- Aggregations: avg/min/max/sum/count.
- Percentiles: p50/p75/p90/p95/p99 (via PERCENTILE_CONT).
- Grouping support (group_by column name).
- Top-N ranking support (top_n).
- Windowing by createdon (window_days).
- Parameterized filters (dict) to avoid SQL injection.
- Uses is_safe_sql(...) to validate generated SQL.
- Returns {"metric", "window_days", "sql", "params", "explanation", "data"}.
"""

from typing import Any, Dict, List, Optional, Tuple
from psycopg2.extras import RealDictCursor
from connection import get_connection
from sql_guard import is_safe_sql

METRIC_MAP = {
    "cycle_time": "cycletimeduration",
    "review_latency": "opentoreviewduration",
    "approval_latency": "reviewedtoapprovedduration",
    "churn": "(linesadded + linesremoved)",
    "throughput": "1"  # throughput is COUNT(*); we handle specially
}

AGG_FUNCS = {"avg", "min", "max", "sum", "count"}
PERCENTILE_MAP = {"p50": 0.5, "p75": 0.75, "p90": 0.9, "p95": 0.95, "p99": 0.99}


def _build_where_clause(window_days: int, filters: Optional[Dict[str, Any]]) -> Tuple[str, List[Any]]:
    clauses: List[str] = [f"createdon >= NOW() - INTERVAL %s"]
    params: List[Any] = [f"{int(window_days)} days"]

    if filters:
        for k, v in filters.items():
            # allow equality filters only for now; parameterized
            clauses.append(f"{k} = %s")
            params.append(v)

    where_sql = " AND ".join(clauses)
    return where_sql, params


def _explain_text(metric_name: str, agg: Optional[str], percentiles: Optional[List[str]], group_by: Optional[str]) -> str:
    parts: List[str] = []
    if percentiles:
        parts.append(", ".join(percentiles).upper() + " (percentiles)")
    if agg:
        parts.append(agg.upper())
    metric_desc = metric_name.replace("_", " ")
    if group_by:
        return f"Computed {', '.join(parts)} of `{metric_desc}` grouped by `{group_by}`."
    return f"Computed {', '.join(parts)} of `{metric_desc}` overall."


def get_metric(
    metric_name: str,
    window_days: int = 30,
    filters: Optional[Dict[str, Any]] = None,
    agg: Optional[str] = "avg",
    percentiles: Optional[List[str]] = None,
    group_by: Optional[str] = None,
    top_n: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Flexible metric tool.

    Args:
        metric_name: logical metric key from METRIC_MAP (e.g., "cycle_time", "churn", "throughput").
        window_days: lookback window on pull_request.createdon (default 30).
        filters: optional dict of equality filters (e.g., {"repoid": 5, "authorid": 123}).
        agg: aggregation function as string (avg, min, max, sum, count). Ignored if percentiles provided or metric is throughput.
        percentiles: list like ["p50","p75"] to compute percentiles instead of simple agg.
        group_by: column to group by (e.g., "team_id", "authorid", "repoid").
        top_n: if set and group_by is provided, limit results to top N ordered by primary stat (p50 if percentiles else agg).

    Returns:
        dict with keys:
            - metric, window_days, sql, params, explanation, data (list of rows)
        or {"error": "..."} on failure.
    """

    # Validate metric
    if metric_name not in METRIC_MAP:
        return {"error": f"Unknown metric: {metric_name}"}

    metric_expr = METRIC_MAP[metric_name]
    is_throughput = metric_name == "throughput"

    # Build WHERE clause and params
    where_sql, params = _build_where_clause(window_days, filters)

    # Build SELECT expressions
    select_parts: List[str] = []
    order_by_expr = None  # used for top_n ordering

    # Percentiles requested
    if percentiles:
        # Validate percentiles
        pct_values = []
        for p in percentiles:
            if p not in PERCENTILE_MAP:
                return {"error": f"Unsupported percentile: {p}. Supported: {list(PERCENTILE_MAP.keys())}"}
            pct_values.append(PERCENTILE_MAP[p])

        # Single percentile per column or multiple as array
        # We'll use separate percentile_cont calls so returned columns are explicit
        for p in percentiles:
            expr = f"PERCENTILE_CONT({PERCENTILE_MAP[p]}) WITHIN GROUP (ORDER BY {metric_expr}) AS {p}"
            select_parts.append(expr)

        # Use first percentile for ordering if grouping & top_n
        order_by_expr = percentiles[0] if percentiles else None

    # If throughput, compute COUNT(*)
    if is_throughput:
        select_parts.append("COUNT(*) AS pr_count")
        if not percentiles and not agg:
            # default aggregator would be count
            agg = "count"

        if not order_by_expr:
            order_by_expr = "pr_count"

    # If not percentiles and not throughput, use agg function
    if (not percentiles) and (not is_throughput):
        if not agg or agg.lower() not in AGG_FUNCS:
            return {"error": f"Unsupported agg: {agg}. Supported: {sorted(AGG_FUNCS)}"}
        select_parts.append(f"{agg.upper()}({metric_expr}) AS {agg}_{metric_name}")
        order_by_expr = f"{agg}_{metric_name}" if not order_by_expr else order_by_expr

    # Build final SQL
    schema_table = "insightly.pull_request"  # explicit schema.table as required
    if group_by:
        select_clause = f"{group_by}, " + ", ".join(select_parts)
        group_clause = f"GROUP BY {group_by}"
        order_clause = f"ORDER BY {order_by_expr} DESC" if order_by_expr else ""
        limit_clause = f"LIMIT {int(top_n)}" if top_n else ""
        sql = f"SELECT {select_clause} FROM {schema_table} WHERE {where_sql} {group_clause} {order_clause} {limit_clause};"
    else:
        select_clause = ", ".join(select_parts)
        sql = f"SELECT {select_clause} FROM {schema_table} WHERE {where_sql};"

    # Validate SQL using guard
    safe, reason = is_safe_sql(sql, schema_guard=True)
    if not safe:
        return {"error": f"Generated SQL is not allowed: {reason}"}

    # Execute the query
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        return {"error": str(e), "sql": sql, "params": params}

    # Normalize output shape
    data = []
    if group_by:
        # rows are dicts: {group_by: value, <stat1>: val1, ...}
        for r in rows:
            data.append(dict(r))
    else:
        # single-row aggregates (or percentiles) -> return as dict
        for r in rows:
            data.append(dict(r))

    explanation = _explain_text(metric_name, agg if not percentiles else None, percentiles, group_by)

    return {
        "metric": metric_name,
        "window_days": window_days,
        "sql": sql,
        "params": params,
        "explanation": explanation,
        "data": data,
    }
