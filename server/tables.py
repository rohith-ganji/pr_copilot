# server/tables.py

import os
from dotenv import load_dotenv
from openai import OpenAI
from sql_guard import is_safe_sql
from query import run_query

# Load env vars
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# -------------------------
# Tool: list_tables
# -------------------------
def list_tables(user_prompt: str):
    """
    Uses OpenAI LLM to generate SQL for listing tables with row counts
    in the 'insightly' schema, based on the user's natural language prompt.
    """

    llm_prompt = f"""
    You are an expert SQL generator. Convert the following user request into a valid PostgreSQL query.

    User request: "{user_prompt}"

    Constraints:
    - Schema: insightly
    - Output must be raw SQL only (no explanation, no markdown, no extra text).
    - Only list tables available in the given schema.
    Example Input:
    "Show me all tables in the insightly schema"

    Example Output:
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'insightly';
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",   # or whichever model you use
        messages=[
            {"role": "system", "content": "You are a SQL generator."},
            {"role": "user", "content": llm_prompt},
        ],
    )

    sql = response.choices[0].message.content.strip()

    if not is_safe_sql(sql):
        return {"error": "Unsafe SQL detected", "query": sql}

    results = run_query(sql)
    return {"query": sql, "results": results}


# -------------------------
# Tool: get_related_tables
# -------------------------
def get_related_tables(user_prompt: str):
    """
    Uses OpenAI LLM to generate SQL for finding related tables
    in the 'insightly' and 'hivel-code-review' schemas, based on the user's prompt.
    """

    llm_prompt = f"""
    You are an expert SQL generator. Convert the following natural language request into a PostgreSQL query.

    User request: "{user_prompt}"

    Constraints:
    - Schemas: insightly, hivel-code-review
    - Only generate SQL (no explanation, no markdown, no extra text).
    - Ensure that the SQL checks for related tables by matching keywords in table names.

    Example Input:
    "Find all tables related to pull requests"

    Example Output:
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema IN ('insightly', 'hivel-code-review')
      AND table_name ILIKE '%pull_request%';
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a SQL generator."},
            {"role": "user", "content": llm_prompt},
        ],
    )

    sql = response.choices[0].message.content.strip()

    if not is_safe_sql(sql):
        return {"error": "Unsafe SQL detected", "query": sql}

    results = run_query(sql)
    return {"query": sql, "results": results}
