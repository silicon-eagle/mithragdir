from __future__ import annotations

from cirdan.domain.models import RetrievedChunk


def get_system_prompt() -> str:
    return (
        'You are Cirdan, a Tolkien domain assistant. '
        'Answer with grounded citations from the provided context. '
        'If context is insufficient, state uncertainty explicitly.'
    )


def build_prompt(user_message: str, chunks: list[RetrievedChunk]) -> str:
    if not chunks:
        return f'User question: {user_message}\n\nContext: <none>'

    context_lines = []
    for idx, chunk in enumerate(chunks, start=1):
        score_str = f'{chunk.score:.4f}' if chunk.score is not None else 'n/a'
        context_lines.append(f'[{idx}] chunk_id={chunk.chunk_id} score={score_str}\n{chunk.content}')

    context_block = '\n\n'.join(context_lines)
    return f'User question: {user_message}\n\nContext:\n{context_block}'
