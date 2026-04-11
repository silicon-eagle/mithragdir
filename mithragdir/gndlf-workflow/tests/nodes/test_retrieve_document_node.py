from workflow.graph.state import GraphState
from workflow.nodes.retrieve_document_node import RetrieveDocumentNode


async def test_retrieve_document_node_integration() -> None:
    node = RetrieveDocumentNode()
    state = GraphState(
        query='Where is Rivendell located?',
        generated_query='Rivendell location Last Homely House geography',
    )

    result = await node(state)

    assert 'documents' in result
    assert isinstance(result['documents'], list)
