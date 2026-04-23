from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from typing import TypedDict

import pytest
from core.config import DEFAULT_QDRANT_URL
from core.db import RedbookDatabase
from core.schemas import Text as TextSchema
from pipeline.processing.chunker import Chunker
from pipeline.processing.embedding import ChunkEmbedder
from workflow.graph.state import GraphState
from workflow.nodes.retrieve_document_node import RetrieveDocumentNode


class SeededRetrievalDocument(TypedDict):
    document_id: int
    title: str
    chunk_count: int
    point_count: int
    collection_name: str


@pytest.fixture
def seeded_retrieval_document() -> Iterator[SeededRetrievalDocument]:
    collection_name = f'workflow_retrieve_{uuid.uuid4().hex[:8]}'

    db = RedbookDatabase(db_url=os.getenv('DATABASE_URL'))
    db.truncate_all_tables()
    source_path = f'/tmp/workflow-retrieve-{uuid.uuid4().hex}.txt'
    seed_text = TextSchema(
        title='Workflow Retrieval Test Document',
        content=('Rivendell is the Last Homely House east of the Misty Mountains. Elrond keeps this refuge hidden in a steep valley.'),
        author='J.R.R. Tolkien',
        source_path=source_path,
        file_format='txt',
    )

    document_id = db.insert_text(seed_text)
    chunker = Chunker(db=db, chunk_size=120, chunk_overlap=0)
    chunk_count = chunker.chunk_document(
        document_id=document_id,
        content=seed_text.content,
        metadata={'document_id': document_id, 'title': seed_text.title, 'url': seed_text.source_path},
    )

    embedder = ChunkEmbedder(db=db, collection_name=collection_name, qdrant_url=os.getenv('QDRANT_URL', DEFAULT_QDRANT_URL))
    point_count = embedder.encode_and_upsert_hybrid_chunks(document_id=document_id, batch_size=8, show_progress=False)

    try:
        yield {
            'document_id': document_id,
            'title': seed_text.title,
            'chunk_count': chunk_count,
            'point_count': point_count,
            'collection_name': collection_name,
        }
    finally:
        embedder.qdrant_client.delete_collection(collection_name=collection_name)
        db.truncate_all_tables()
        db.close()


async def test_retrieve_document_node_integration(seeded_retrieval_document: SeededRetrievalDocument) -> None:
    node = RetrieveDocumentNode(collection_name=seeded_retrieval_document['collection_name'])
    state = GraphState(
        query='Where is the Last Homely House east of the Misty Mountains?',
        generated_query='Rivendell Last Homely House east of the Misty Mountains Elrond valley',
    )

    result = await node(state)

    assert 'documents' in result
    assert isinstance(result['documents'], list)
    assert result['documents']
    assert seeded_retrieval_document['chunk_count'] == seeded_retrieval_document['point_count']
    assert any(document.document_id == seeded_retrieval_document['document_id'] for document in result['documents'])
    assert any(document.title == seeded_retrieval_document['title'] for document in result['documents'])
    assert all(document.document_id > 0 for document in result['documents'])
    assert all(document.title.strip() for document in result['documents'])
    assert all(document.raw_content.strip() for document in result['documents'])
