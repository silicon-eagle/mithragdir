from pathlib import Path

from tests.setup_logger import setup_logger

from gwaihir import RedbookDatabase, TolkienGatewayClient

setup_logger(log_dir=Path().cwd() / '.log', level='DEBUG')
db = RedbookDatabase(db_path=Path().cwd() / 'database' / 'redbook.db')
db.deploy()
client = TolkienGatewayClient(
    base_url='https://tolkiengateway.net',
    db=db,
    batch_size=50,
    timeout_seconds=30,
)

if __name__ == '__main__':
    index = client.get_index(batch_size=250, pause_seconds=1.0)
    client.crawl(index=index, pause_seconds=1.0, nr_attempts=3, retry_sleep_seconds=30.0)
