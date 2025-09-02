import asyncio
from dotenv import load_dotenv
import os

from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.prebuilt import create_react_agent
from langsmith import traceable
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, END
from typing import TypedDict

# Load environment variables from .env
load_dotenv()

print(f"📊 LangSmith project: {os.getenv('LANGCHAIN_PROJECT')}")

# Load system prompt for intent classification
with open("prompt.txt", "r", encoding="utf-8") as f:
    intent_system_prompt = f.read().strip()

# Intent classifier model
intent_model = ChatOpenAI(model="gpt-4.1-mini")


# ---------------------------
# STATE DEFINITION
# ---------------------------
class AgentState(TypedDict):
    user_input: str
    intent: str | None
    result: str | None


# ---------------------------
# NODES
# ---------------------------
async def classify_intent_node(state: AgentState) -> AgentState:
    """Node: classify intent"""
    response = await intent_model.ainvoke([
        SystemMessage(content=intent_system_prompt),
        HumanMessage(content=state["user_input"]),
    ])
    intent = response.content.strip()
    print(f"🔎 Detected intent: {intent}")
    return {**state, "intent": intent}


@traceable(run_type="chain", name="pr_copilot_agent")
async def run_agent(agent, user_input: str):
    """Helper: run MCP agent with tracing"""
    response = await agent.ainvoke({"messages": user_input})
    if "messages" in response:
        return response["messages"][-1].content
    return "⚠️ No response generated."


async def main_agent_node(state: AgentState) -> AgentState:
    """Node: run main MCP agent"""
    final_message = await run_agent(agent, f"[Intent: {state['intent']}] {state['user_input']}")
    return {**state, "result": final_message}


# ---------------------------
# GRAPH CONSTRUCTION
# ---------------------------
workflow = StateGraph(AgentState)

workflow.add_node("classify_intent", classify_intent_node)
workflow.add_node("main_agent", main_agent_node)

# Start at intent classifier
workflow.set_entry_point("classify_intent")


def route_after_intent(state: AgentState):
    """Route after classification"""
    if state["intent"] in ["MetricsQuery", "PRRiskReview"]:
        return "main_agent"
    else:
        print("⚠️ Sorry, that request is out of scope. Please ask something related to metrics or PR risk review.")
        return END


workflow.add_conditional_edges(
    "classify_intent",
    route_after_intent,
    {
        "main_agent": "main_agent",
        END: END,
    }
)

workflow.add_edge("main_agent", END)

# Compile graph
app = workflow.compile()


# ---------------------------
# MAIN LOOP
# ---------------------------
async def main():
    global agent

    # Initialize MCP client
    client = MultiServerMCPClient({
        "db": {
            "command": "python",
            "args": ["server/fastmcp_server.py"],
            "transport": "stdio",
        },
    })

    # Load tools
    tools = await client.get_tools()

    # Create MCP agent
    agent = create_react_agent(
        model="openai:gpt-4.1-mini",
        tools=tools,
    )

    print("🤖 LangGraph MCP Agent is ready! Type your question (or 'exit' to quit).")

    while True:
        user_input = input("\nYou: ")
        if user_input.lower() in ["exit", "quit"]:
            print("👋 Goodbye!")
            break

        try:
            # Run graph
            result = await app.ainvoke({"user_input": user_input, "intent": None, "result": None})

            if result.get("result"):
                print(f"Agent: {result['result']}")

        except Exception as e:
            print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
