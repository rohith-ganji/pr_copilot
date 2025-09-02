from psycopg2.extras import RealDictCursor
from connection import get_connection
from query import get_diff_outline


def get_pr_summary(pr_id: int) -> dict:
    """
    Return a structured summary of a PR from insightly.pull_request table.
    """
    sql = """
        SELECT id, actualpullrequestid, title, authorid, state,
               createdon, mergedon, declinedon, updatedon,
               linesadded, linesremoved, commitscount, modifiedfilescount,
               cycletimeduration, opentoreviewduration, reviewedtoapprovedduration,
               reviewedtomergedduration, approvedtomergedduration,
               labels, hotfixpr, releasebranchpr, reviewbranchpr, excludepr
        FROM insightly.pull_request
        WHERE actualpullrequestid = %s
    """
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, (pr_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return {"error": f"No PR found with id={pr_id}"}

        return {"pr_summary": row}
    except Exception as e:
        return {"error": str(e)}


def get_pr_risk(pr_id: int):
    """
    Estimate the risk of a Pull Request by analyzing both its summary and diff outline.
    Returns:
        dict: {
            "pr_id": int,
            "risk_score": float (0–1),
            "comments": List[str]  # always 3 concise items
        }
    """
    summary_data = get_pr_summary(pr_id)
    outline_data = get_diff_outline(pr_id)

    # Handle error cases
    if "error" in summary_data:
        return {"error": summary_data["error"]}
    if "error" in outline_data:
        return {"error": outline_data["error"]}

    summary = summary_data.get("pr_summary", {})
    outline = outline_data.get("diff_outline", [])

    # Convert dicts/lists into strings for text-based heuristics
    summary_text = (summary.get("title") or "") + " " + " ".join(summary.get("labels") or [])
    outline_text = "\n".join([f['filename'] for f in outline if f.get("filename")])

    # Naive heuristics
    risk = 0.0
    comments = []

    if "security" in summary_text.lower() or "auth" in summary_text.lower():
        risk += 0.3
        comments.append("Touches security-sensitive code")

    if "database" in outline_text.lower() or "schema" in outline_text.lower():
        risk += 0.3
        comments.append("Modifies database schema or queries")

    if len(outline) > 30:  # heuristic: many files changed
        risk += 0.2
        comments.append("Large number of files increases review risk")

    if "deprecated" in summary_text.lower():
        risk += 0.2
        comments.append("Deprecation may affect backward compatibility")

    # Normalize to 0–1
    risk_score = min(1.0, risk)

    # Always return 3 comments (pad if needed)
    while len(comments) < 3:
        comments.append("No additional major risk factors detected")
    comments = comments[:3]

    return {
        "pr_id": pr_id,
        "risk_score": round(risk_score, 2),
        "comments": comments
    }
