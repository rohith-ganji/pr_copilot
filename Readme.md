# PR Copilot

This project demonstrates a **LangGraph-based system** powered by **MCP server tools** and **OpenAI models**.

---

## 🚀 Functionalities
You can prompt this system with queries related to the **Insightly DB on PRs** and **metrics-related context only**.  
Using this system, you can:

- View tables, columns, and data available  
- Get related tables for a prompted table  
- Prompt to implement your custom safe query  
- Ensure every query is safeguarded before execution  
- Get PR diffs for a pull request  
- Review a PR with its **risk score** and **actionable comments**  
- Ask for a **summary of a PR**  
- Retrieve metrics like **cycle time** and **churn** for a PR within a given time window  
- Auto-generate a **graph workflow PNG**

---

## 🛠 Workflow
When a user gives a prompt:

1. **Intent Classifier Node**  
   - Classifies the prompt’s intent as **Metric**, **PR-related**, **Both**, or **Unrelated**.  
   - If unrelated → execution bypasses to the **END node**.  

2. **Router Node**  
   - Runs respective agents for Metric/PR queries.  
   - Stores results in `subagent_result` of State.  

3. **Subagents**  
   - Chooses the appropriate tool(s) from available **MCP server tools**.  
   - Generate the required SQL query.  
   - Queries are validated for **SQL safety** before execution.  
   - If safe → `run_query` tool executes the query and returns results.  

4. **Summarizer Node**  
   - Takes `subagent_result`.  
   - Summarizes the final answer for the client.  

---

## 🔑 Client Nodes
- `classify_intent`  
- `multi_router`  
- `summarizer`  

---

## 🧰 MCP Server Tools
- `list_tables`  
- `get_related_tables`  
- `run_query`  
- `get_diff_outline`  
- `get_pr_summary`  
- `get_pr_risk`  
- `get_metric`  

---

## ⚡️ Running the Project

### Setup
```bash
cd pr_2
pip install -r requirements.txt



To Run:
cd pr_2
Install dependencies from requirements.txt

 To run client:
 python client.py

 To run MCP Server:
 uv run server/fastmcp_server.py
