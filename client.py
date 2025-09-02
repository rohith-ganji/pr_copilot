import asyncio
from dotenv import load_dotenv
import os

from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.prebuilt import create_react_agent
from langsmith import traceable
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage

# Load environment variables from .env
load_dotenv()

# Optional: confirm LangSmith is wired
print(f"📊 LangSmith project: {os.getenv('LANGCHAIN_PROJECT')}")

# Load system prompt from file
with open("prompt.txt", "r", encoding="utf-8") as f:
    intent_system_prompt = f.read().strip()

# Intent classifier model (no tools)
intent_model = ChatOpenAI(model="gpt-4.1-mini")


async def classify_intent(user_input: str) -> str:
    """Classify intent using system prompt from prompt.txt"""
    response = await intent_model.ainvoke([
        SystemMessage(content=intent_system_prompt),
        HumanMessage(content=user_input),
    ])
    return response.content.strip()


# Wrap the agent in LangSmith tracing
@traceable(run_type="chain", name="pr_copilot_agent")
async def run_agent(agent, user_input: str):
    """Run the MCP agent with tracing enabled via LangSmith."""
    response = await agent.ainvoke({"messages": user_input})
    if "messages" in response:
        return response["messages"][-1].content
    return "⚠️ No response generated."


async def main():
    # Initialize MultiServerMCPClient
    client = MultiServerMCPClient(
        {
            "db": {
                "command": "python",
                "args": ["server/fastmcp_server.py"],
                "transport": "stdio",
            },
        }
    )

    # Load tools from MCP servers
    tools = await client.get_tools()

    # Create OpenAI-powered main agent
    agent = create_react_agent(
        model="openai:gpt-4.1-mini",  # switch to gpt-4.1 if needed
        tools=tools,
    )

    print("🤖 MCP Agent is ready! Type your question (or 'exit' to quit).")

    while True:
        user_input = input("\nYou: ")
        if user_input.lower() in ["exit", "quit"]:
            print("👋 Goodbye!")
            break

        try:
            # Step 1: classify intent
            intent = await classify_intent(user_input)
            print(f"🔎 Detected intent: {intent}")

            # Step 2: check intent
            if intent not in ["MetricsQuery", "PRRiskReview"]:
                print("⚠️ Sorry, that request is out of scope. Please ask something related to metrics or PR risk review.")
                continue

            # Step 3: Run with LangSmith tracing
            final_message = await run_agent(agent, f"[Intent: {intent}] {user_input}")
            print(f"Agent: {final_message}")

        except Exception as e:
            print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
