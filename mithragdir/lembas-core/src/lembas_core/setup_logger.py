from datetime import datetime
from pathlib import Path
from sys import stderr

from loguru import logger


def default_logger_format() -> str:
    """Return the default Loguru format string.

    Returns:
        str: Format string used for both console and file sinks.
    """
    return (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    )


def setup_logger(
    level: str = "DEBUG",
    log_dir: Path = Path.cwd() / ".log",
    fmt: str = default_logger_format(),
) -> None:
    """Configure Loguru with console and rotating file sinks.

    Creates the log directory (``.log`` by default) if it does not yet exist
    and installs two sinks: a colourised stderr output and a rotating file
    (10 MB chunks) with backtrace / diagnose enabled for richer error output.

    Args:
        level: Minimum log level name to emit (e.g. ``DEBUG``).
        log_dir: Directory in which to create the dated log file.
        fmt: Loguru format string.
    """
    log_dir.mkdir(exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"{date_str}_logfile.log"

    # Remove all default Loguru sinks
    logger.remove()

    # Add console sink
    logger.add(
        stderr,
        format=fmt,
        level=level,
        colorize=True,
    )

    # Add file sink with structured logging
    logger.add(
        log_file,
        rotation="10 MB",
        level=level,
        format=fmt,
        enqueue=True,
        backtrace=True,
        diagnose=True,
    )
