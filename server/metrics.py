"""
PR metrics via LLM-generated SQL.

Features:
- LLM generates full SQL based on user natural language prompt.
- SQL is validated with is_safe_sql.
- Returns {"sql", "params", "data"} or {"error": "..."}.
"""

from typing import Any, Dict
from psycopg2.extras import RealDictCursor
from connection import get_connection
from sql_guard import is_safe_sql
import os, json
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Columns of pull_request table (for LLM context)
PR_COLUMNS = [
    "id","actualpullrequestid","title","authorid","createdon","description",
    "destinationbranch","sourcebranch","firstcommitid","sourcecommitid",
    "destinationcommitid","state","repoid","linesadded","linesremoved",
    "htmllink","commentcount","commitscount","modifiedfilescount","reason",
    "updatedon","mergecommit","mergedby","firstreviewedby","approvedby",
    "originalmergedby","mergedon","declinedon","reviewedon","approvedon",
    "firstcommittedon","committoopenduration","opentoreviewduration",
    "reviewedtoapprovedduration","reviewedtomergeeduration",
    "approvedtomergedduration","reviewedtodeclineduration","opentodeclineduration",
    "opentomergedduration","cycletimeduration","deploytimeduration",
    "cycletimeoverflow","declinedby","remark","originalauthorid","createddate",
    "modifieddate","originalapprovedby","originalfirstreviewedby",
    "originaldeclinedby","processed","hotfixpr","reviewbranchpr",
    "releasebranchpr","excludepr","flashyreviewedpr","jiramappingprocessed",
    "labels","organizationid","workspaceid","prsentiment","issourcebranchdeleted",
    "userintegrationid","jiradatacollected","reviewcyclecount","autoexcludepr",
    "opentofirstcommentduration","firstcommenttoapproved","estimated_storypoints",
    "comment_sentiment_count","is_deployment_pr","mergetodeployduration",
    "deployment_record_id"
]

SYSTEM_PROMPT = f"""
You are an expert SQL generator for PR metrics from the "insightly.pull_request" table.
Columns available: {', '.join(PR_COLUMNS)}

Rules for generating SQL:

1. Always generate a **full PostgreSQL SELECT query**; do NOT explain it.
2. Only use table "insightly.pull_request".
3. Only SELECT statements allowed; no INSERT/UPDATE/DELETE.
4. Always use psycopg2-style placeholders (%s) for values.
5. Infer the correct aggregation based on the user request:
   - If the user asks cycle time, churn etc, fetch required columns.
   - If the user asks for averages, latencies, or durations, use AVG or PERCENTILE_CONT.
   - If the user asks for counts, throughput, or number of PRs, use COUNT(*).
   - For percentiles, use PERCENTILE_CONT and return named columns like p50, p75.
6. Support optional grouping if user mentions "by team", "by author", "by repo", etc.
7. Apply optional filtering if the user mentions criteria like repoid, authorid, date range, state.
8. Support top-N results if user requests "top" PRs, e.g., top 5 slowest, top 10 churn PRs.
9. Return a JSON object with:
   - sql: string with the query
   - params: list of values for parameters
"""

def llm_generate_sql(user_prompt: str) -> Dict[str, Any]:
    """Ask LLM to generate full SQL + params as JSON."""
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0
        )
        return json.loads(response.choices[0].message.content.strip())
    except Exception as e:
        return {"error": str(e)}

def get_metric(user_prompt: str) -> Dict[str, Any]:
    """
    MCP tool: user prompt -> LLM SQL -> validate -> run -> return data.
    """
    # Step 1: Generate SQL
    sql_obj = llm_generate_sql(user_prompt)
    if "error" in sql_obj:
        return sql_obj

    sql = sql_obj.get("sql")
    params = sql_obj.get("params", [])

    # Step 2: Validate
    safe, reason = is_safe_sql(sql, schema_guard=True)
    if not safe:
        return {"error": f"Unsafe SQL: {reason}", "sql": sql, "params": params}

    # Step 3: Execute
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        return {"error": str(e), "sql": sql, "params": params}

    return {"sql": sql, "params": params, "data": [dict(r) for r in rows]}
