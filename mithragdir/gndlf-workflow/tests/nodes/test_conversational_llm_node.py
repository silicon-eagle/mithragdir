from langchain_core.messages import HumanMessage
from workflow.graph.state import GraphState
from workflow.nodes.conversational_llm_node import ConversationalLLMNode


async def test_conversational_llm_node_integration() -> None:
    node = ConversationalLLMNode()
    state = GraphState(
        messages=[HumanMessage(content='Reply with a short greeting.')],
        query='Reply with a short greeting.',
    )

    result = await node(state)

    assert 'generation' in result
    assert isinstance(result['generation'], str)
    assert result['generation'].strip()
