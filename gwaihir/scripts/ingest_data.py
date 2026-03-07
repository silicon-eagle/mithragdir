from pathlib import Path

from gwaihir.retriever.text_client import TextClient
from tests.setup_logger import setup_logger

from gwaihir import RedbookDatabase, TolkienGatewayClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]
setup_logger(log_dir=PROJECT_ROOT / '.log', level='DEBUG')
db = RedbookDatabase(db_path=PROJECT_ROOT / 'database' / 'redbook.db')
db.deploy()
wiki_client = TolkienGatewayClient(
    base_url='https://tolkiengateway.net',
    db=db,
    batch_size=50,
    timeout_seconds=30,
)
text_client = TextClient(
    db=db,
    source_folder=PROJECT_ROOT / 'database' / 'books',
)

if __name__ == '__main__':
    index = wiki_client.get_index(batch_size=250, pause_seconds=0.5)
    wiki_client.crawl(index=index, pause_seconds=0.5, nr_attempts=2, retry_sleep_seconds=30.0)
    text_client.ingest()
    text_client.close()
    db.close()
