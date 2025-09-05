import asyncio
from dotenv import load_dotenv
import os
import subprocess
import json

from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.prebuilt import create_react_agent
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, END
from typing import TypedDict, List, Dict, Any

import sys
import platform


# ---------------------------
# Load environment variables
# ---------------------------
load_dotenv()
print(f"📊 LangSmith project: {os.getenv('LANGCHAIN_PROJECT')}")

# ---------------------------
# Load system prompts
# ---------------------------
def load_prompt(name: str) -> str:
    path = f"prompts/{name}.txt"
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()

intent_system_prompt = load_prompt("intent_classification")
metrics_system_prompt = load_prompt("metrics_agent")
pr_risk_system_prompt = load_prompt("pr_risk_agent")
summarizer_system_prompt = load_prompt("summarizer_agent")

# ---------------------------
# Helper functions
# ---------------------------
def extract_text(agent_response):
    """Safely extract text from LangGraph / LLM responses."""
    if isinstance(agent_response, dict):
        if "messages" in agent_response and agent_response["messages"]:
            last_msg = agent_response["messages"][-1]
            return getattr(last_msg, "content", str(last_msg))
        return agent_response.get("output") or agent_response.get("text") or str(agent_response)
    elif hasattr(agent_response, "content"):
        return agent_response.content
    return str(agent_response)

def truncate_text(text: str, max_chars: int = 4000) -> str:
    """Truncate long text to avoid exceeding token limits."""
    return text if len(text) <= max_chars else text[:max_chars] + "\n...[truncated]"

def save_workflow_visualization(app):
    """Save workflow graph as PNG (via API retries) and DOT/Graphviz fallback with error handling."""
    try:
        graph = app.get_graph()
        
        # Always save Mermaid text
        try:
            mermaid_text = graph.draw_mermaid()
            with open("workflow_graph.mmd", "w", encoding="utf-8") as f:
                f.write(mermaid_text)
            print("✅ Workflow graph saved as workflow_graph.mmd (Mermaid text)")
        except Exception as mmd_error:
            print(f"❌ Mermaid text generation failed: {mmd_error}")
        
        # PNG generation via API retries
        try:
            png_bytes = graph.draw_mermaid_png(max_retries=5, retry_delay=2.0)
            with open("workflow_graph.png", "wb") as f:
                f.write(png_bytes)
            print("✅ Workflow graph saved as workflow_graph.png (using API with retries)")

            # Auto-open PNG (best-effort)
            try:
                if platform.system() == "Windows":
                    os.startfile("workflow_graph.png")
                elif platform.system() == "Darwin":  # macOS
                    subprocess.run(["open", "workflow_graph.png"], check=False)
                else:  # Linux
                    subprocess.run(["xdg-open", "workflow_graph.png"], check=False)
            except Exception as open_error:
                print(f"⚠️ Could not auto-open PNG: {open_error}")

        except Exception as api_error:
            print(f"❌ PNG generation failed with API retries: {api_error}")
        
        # Graphviz alternative
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.dot', delete=False, encoding="utf-8") as f:
                dot_content = graph.to_dot()
                f.write(dot_content)
                dot_file = f.name

            try:
                subprocess.run(['dot', '-Tpng', dot_file, '-o', 'workflow_graph_alt.png'], 
                             check=True, capture_output=True)
                print("✅ Alternative workflow graph saved as workflow_graph_alt.png (using Graphviz)")
            except (subprocess.CalledProcessError, FileNotFoundError):
                print("ℹ️ Graphviz not available. Install with: apt-get install graphviz (Linux) or brew install graphviz (Mac)")
            finally:
                if os.path.exists(dot_file):
                    os.unlink(dot_file)

        except Exception as dot_error:
            print(f"ℹ️ Graphviz alternative not available: {dot_error}")
            
    except Exception as e:
        print(f"❌ Graph visualization failed: {e}")
        print("💡 Solutions:")
        print("   1. Install Graphviz: apt-get install graphviz (Linux) or brew install graphviz (Mac)")
        print("   2. Use the .mmd file with online Mermaid editor: https://mermaid.live/")
# ---------------------------
# State Definition
# ---------------------------
class AgentState(TypedDict):
    user_input: str
    intents: List[Dict[str, str]]   # <-- now list of objects: [{"name": ..., "query": ...}]
    subagent_results: List[Dict[str, Any]]
    final_result: str | None
    agent_client: Any

# ---------------------------
# Routing Helper Functions
# ---------------------------
def route_after_intent_classification(state: AgentState) -> str:
    """Route after intent classification - go to summarizer if unrelated, otherwise to multi_router."""
    intents = state.get("intents", [])
    
    # Check if all intents are unrelated or no valid intents found
    valid_intents = [intent for intent in intents if intent.get("name") not in ["Unrelated", "Unknown", None]]
    
    if not valid_intents:
        print("🚫 No valid intents detected. Routing directly to summarizer.")
        return "summarizer"
    
    print("✅ Valid intents found. Proceeding to multi_router.")
    return "multi_router"

# ---------------------------
# Node Definitions
# ---------------------------
async def classify_intent_node(state: AgentState) -> AgentState:
    """LLM predicts one or more intents with sub-queries and returns JSON list."""
    intent_model = ChatOpenAI(model="gpt-4.1-mini")
    messages = [
        SystemMessage(content=intent_system_prompt),
        HumanMessage(content=state["user_input"])
    ]
    response = await intent_model.ainvoke(messages)
    raw_text = extract_text(response).strip()

    try:
        parsed = json.loads(raw_text)
        intents = parsed.get("intents", [])
    except Exception:
        # fallback: treat entire input as Unrelated
        intents = [{"name": "Unrelated", "query": state["user_input"]}]

    print(f"🔎 Detected intents: {intents}")
    return {**state, "intents": intents}

async def multi_router_node(state: AgentState) -> AgentState:
    """Handles one or multiple intents by routing only the relevant sub-query to each agent."""
    intents = state.get("intents", [])
    results = state.get("subagent_results", [])
    agent_client = state.get("agent_client")

    print("🔀 Multi-router: Processing valid intents...")

    for intent in intents:
        name = intent.get("name")
        query = intent.get("query", state["user_input"])

        if name == "MetricsQuery":
            print(f"📊 Processing Metrics Query: {query}")
            metrics_agent = create_react_agent(
                model="openai:gpt-4.1-mini",
                tools=agent_client.tools
            )
            metrics_result = await metrics_agent.ainvoke({
                "messages": [
                    SystemMessage(content=metrics_system_prompt),
                    HumanMessage(content=query)
                ]
            })
            results.append({"agent": "MetricsQuery", "output": extract_text(metrics_result)})

        elif name == "PRRiskReview":
            print(f"🔐 Processing PR Risk Query: {query}")
            pr_risk_agent = create_react_agent(
                model="openai:gpt-4.1-mini",
                tools=agent_client.tools
            )
            pr_result = await pr_risk_agent.ainvoke({
                "messages": [
                    SystemMessage(content=pr_risk_system_prompt),
                    HumanMessage(content=query)
                ]
            })
            results.append({"agent": "PRRiskReview", "output": extract_text(pr_result)})

    return {**state, "subagent_results": results}

async def summarizer_node(state: AgentState) -> AgentState:
    """Summarizes results from agents OR handles unrelated queries."""
    intents = state.get("intents", [])
    subagent_results = state.get("subagent_results", [])
    
    # Check if this is an unrelated query (no valid intents and no subagent results)
    valid_intents = [intent for intent in intents if intent.get("name") not in ["Unrelated", "Unknown", None]]
    
    if not valid_intents and not subagent_results:
        print("📝 Summarizer: Handling unrelated query...")
        return {
            **state, 
            "final_result": """❌ **Unrelated Query**

I can only help with:
• 📊 **Metrics Queries**: Database performance, system metrics, analytics
• 🔐 **PR Risk Reviews**: Pull request security analysis, code review

**Examples of valid queries:**
- "Show me database performance metrics"
- "Review the security risks in the latest PR"
- "What are the current system metrics?"
- "Analyze the pull request for potential vulnerabilities"

Please ask me something related to these topics."""
        }
    
    # Handle normal summarization for valid queries
    print("📝 Summarizer: Processing results from agents...")
    
    if not subagent_results:
        return {**state, "final_result": "No results to summarize."}
    
    summarizer_agent = create_react_agent(
        model="openai:gpt-4.1-mini",
        tools=[]
    )

    combined_input = "\n".join(
        [truncate_text(r["output"], max_chars=4000) for r in subagent_results]
    )

    summary_result = await summarizer_agent.ainvoke({
        "messages": [
            SystemMessage(content=summarizer_system_prompt),
            HumanMessage(content=combined_input)
        ]
    })

    final_text = extract_text(summary_result)
    return {**state, "final_result": final_text}

# ---------------------------
# Graph Construction
# ---------------------------
workflow = StateGraph(AgentState)

# Add nodes
workflow.add_node("classify_intent", classify_intent_node)
workflow.add_node("multi_router", multi_router_node)
workflow.add_node("summarizer", summarizer_node)

# Set entry point
workflow.set_entry_point("classify_intent")

# Routing logic
workflow.add_conditional_edges(
    "classify_intent",
    route_after_intent_classification,
    {
        "multi_router": "multi_router",  # Valid intents go to router
        "summarizer": "summarizer"       # Unrelated intents go directly to summarizer
    }
)

# Always flow from multi_router to summarizer
workflow.add_edge("multi_router", "summarizer")

# Always end after summarizer
workflow.add_edge("summarizer", END)

app = workflow.compile()

# ---------------------------
# Generate and save workflow visualization
# ---------------------------
print("🖼 Generating workflow visualization...")
save_workflow_visualization(app)

# ---------------------------
# Main Loop
# ---------------------------
async def main():
    client = MultiServerMCPClient({
        "db": {
            "command": "python",
            "args": ["server/fastmcp_server.py"],
            "transport": "stdio",
        },
    })
    tools = await client.get_tools()
    client.tools = tools

    print("🤖 LangGraph MCP Agent is ready! Type your question (or 'exit' to quit).")
    while True:
        user_input = input("\nYou: ")
        if user_input.lower() in ["exit", "quit"]:
            print("👋 Goodbye!")
            break
        try:
            result = await app.ainvoke({
                "user_input": user_input,
                "intents": [],
                "subagent_results": [],
                "final_result": None,
                "agent_client": client
            })

            # Display results
            subagent_results = result.get("subagent_results", [])
            for r in subagent_results:
                print(f"\n### 🔹 {r['agent']} Result")
                print(f"{r['output']}")
            
            if result.get("final_result"):
                if subagent_results:  # If we have subagent results, this is a summary
                    print(f"\n### 📋 Final Summary")
                else:  # If no subagent results, this is likely an unrelated response
                    print(f"\n### 💬 Response")
                print(f"{result['final_result']}")
            else:
                print("⚠️ No result generated.")
                
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())