PR COPILOT:
This project demonstrates a LangGraph based system powered by MCP server tools and openAI 
models.

Functionalities:
You can prompt this system with the queries related to insightly db on PR and Metrics related 
context only. Using this system you can:
-View tables, columns , data etc available
-Get related tables for prompted table
-Can prompt to implement your custom safe query
-Every query is safe guarded before implementing
-Get PR Diffs for a pr
-Review a PR with its risk score and actionable comments
-Ask for the summary of a PR
-Know metrics like Cycle time and churn for a pr for a time window.
-Auto generates Graph workflow png.

Workflow:
When user gives a prompt:
- Intent Classifier Node classifies the prompt's intent - Metric or PR related or Both or
 Unrelated!
- If its unrelated execution is bypassed to END node.
- If its related to Metric/PR or both, Router node executes, runs the respective agents and 
store the results in subagent_result of State.
- Each subagent chooses tool/tools from available server tools and call them with the required prompt.
- Called tool will take the agent prompt, Generates required sql query to fetch required details from db.
- Then generated query is checked if its safe sql or no and if yes, using run query tool query
 is implemented and sends back the response to client agent.
- Client subagent updates subagent_result of State and pass this to summarizer node
- Then Summarizer node takes the subagent_result and summarizes the final answer.
 
Client Nodes:
-classify_intent
-multi_router
-summarizer

 MCP server tools:
 - list_tables
 - get_related_tables
 - run_query
 - get_diff_outline
 - get_pr_summary
 - get_pr_risk
 - get_metric



To Run:
cd pr_2
Install dependencies from requirements.txt

 To run client:
 python client.py

 To run MCP Server:
 uv run server/fastmcp_server.py


 test