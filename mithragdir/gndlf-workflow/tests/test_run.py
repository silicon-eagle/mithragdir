import pytest
from loguru import logger
from workflow.run import extract_answer, run_graph


@pytest.mark.usefixtures('prod_env')
async def test_run_graph_integration() -> None:
    state = await run_graph('Hello there! Who is Mithrandir? What did he do in the third age in the tolkien universe?')

    answer = extract_answer(state)

    logger.info(f'Extracted answer: {answer}')
    assert answer
    assert isinstance(answer, str)
    assert answer.strip()
