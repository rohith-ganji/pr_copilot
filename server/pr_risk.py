import os
import json
import re
from dotenv import load_dotenv
from openai import OpenAI
from psycopg2.extras import RealDictCursor
from query import get_diff_outline, run_query
from sql_guard import is_safe_sql, enforce_limit

# ✅ Load API key from .env
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def get_pr_summary(user_prompt: str):
    """
    Uses OpenAI LLM to generate SQL for fetching PR summary
    from:
      - insightly.pull_request
      - hivel-code-review.pr_diff
    """

    llm_prompt = f"""
    Convert the following request into a PostgreSQL query.

    User request: "{user_prompt}"

    Constraints:
    - Schemas: insightly, hivel-code-review
    - Use only available columns (see below).
    - Tables: pull_request-insightly schema, pr_diffs- hivel-code-review schema
    - Available columns in pull_request: id, actualpullrequestid, title, authorid, createdon, description,
      destinationbranch, sourcebranch, state, repoid, linesadded, linesremoved, htmllink,
      commentcount, commitscount, modifiedfilescount, labels, organizationid, workspaceid,
      prsentiment, mergedon, declinedon, reviewedon, approvedon, createddate, modifieddate,
      is_deployment_pr, deployment_record_id
    - Available columns in pr_diffs: id, pr_id, filename, file_status, additions, deletions, changes, patch, reviewed_at, review_data
    - Use actualpullrequestid (pull_request) and pr_id (pr_diffs) when joining.
    - Only generate SQL (no explanation, no markdown).
    - When using hivel-code-review schema in query:..'hivel-code-review'.pr_diffs..
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a SQL generator to fetch PR summary."},
            {"role": "user", "content": llm_prompt},
        ],
    )

    sql = response.choices[0].message.content.strip()

    # ✅ Validate SQL
    safe, reason = is_safe_sql(
        sql
    )
    if not safe:
        return {"error": reason, "query": sql}

    # ✅ Enforce safety row limit
    sql = enforce_limit(sql, row_limit=5)

    # ✅ Run query
    results = run_query(sql)

    return {"query": sql, "results": results}


def get_pr_risk(pr_id: int):
    """
    Estimate the risk of a Pull Request by analyzing both its summary and diff outline.
    Uses LLM instead of static heuristics.
    """
    summary_data = get_pr_summary(f"Get summary for PR {pr_id}")
    outline_data = get_diff_outline(f"Get diff outline for PR {pr_id}")

    if "error" in summary_data:
        return {"error": summary_data["error"]}
    if "error" in outline_data:
        return {"error": outline_data["error"]}

    # Extract results properly
    summary = summary_data.get("results", [])
    outline = outline_data.get("results", [])

    # Convert query results into text
    summary_text = " ".join(map(str, summary))
    outline_text = "\n".join([str(f.get("filename", "")) for f in outline if isinstance(f, dict)])

    # --- LLM Risk Evaluation ---
    llm_prompt = f"""
    You are a senior code reviewer. Based on the PR summary and file diffs below,
    assess the potential RISK of this Pull Request.

    STRICT INSTRUCTIONS:
    - Output must be valid JSON (no markdown fences, no extra text).
    - Include only the fields: risk_score (float 0–1) and comments (list of strings).
    - risk_score: float, higher means riskier.
    - comments: up to 3 short, actionable points (e.g., performance, merge conflicts, test coverage).

    PR Summary:
    {summary_text}


    Diff Outline:
    {outline_text}
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a strict PR risk evaluator. Always respond in JSON only."},
            {"role": "user", "content": llm_prompt},
        ],
        temperature=0.3,
    )

    raw_content = response.choices[0].message.content.strip()

    # --- Clean JSON (strip markdown fences if present) ---
    cleaned = re.sub(r"^```(json)?", "", raw_content.strip(), flags=re.IGNORECASE)
    cleaned = re.sub(r"```$", "", cleaned.strip())
    cleaned = cleaned.strip()

    try:
        parsed = json.loads(cleaned)
        return {
            "pr_id": pr_id,
            "risk_score": float(parsed.get("risk_score", 0.0)),
            "comments": parsed.get("comments", []),
            "summary_query": summary_data.get("query"),
            "outline_query": outline_data.get("query"),
        }
    except Exception as e:
        return {
            "pr_id": pr_id,
            "error": f"Failed to parse LLM output: {e}",
            "raw_response": raw_content
        }
