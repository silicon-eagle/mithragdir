from datetime import datetime
from pathlib import Path
from sys import stderr

from loguru import logger


def default_logger_format() -> str:
    return (
        '<green>{time:YYYY-MM-DD HH:mm:ss}</green> | '
        '<level>{level: <8}</level> | '
        '<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - '
        '<level>{message}</level>'
    )


def setup_logger(level: str = 'DEBUG', log_dir: Path = Path.cwd() / '.log', fmt: str = default_logger_format()) -> None:
    log_dir.mkdir(exist_ok=True)

    date_str = datetime.now().strftime('%Y-%m-%d')
    log_file = log_dir / f'{date_str}_logfile.log'

    logger.remove()
    logger.add(
        stderr,
        format=fmt,
        level=level,
        colorize=True,
    )
    logger.add(
        log_file,
        rotation='10 MB',
        level=level,
        format=fmt,
        enqueue=True,
        backtrace=True,
        diagnose=True,
    )
