# mm/logging_config.py

import logging
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from logging.handlers import TimedRotatingFileHandler


def setup_logging(
    level: str = "INFO",
    component: str = "recorder",
    tz: str = "Europe/Berlin",
) -> Path:
    """
    Configure logging:
      - Console (stdout)
      - Daily log file in logs/<component>/YYYY-MM-DD.log (Berlin date)
      - Also keeps rotated backups if the process spans midnight

    Returns:
      Path to the "current" daily log file.
    """
    log_dir = Path("logs") / component
    log_dir.mkdir(parents=True, exist_ok=True)

    now_local = datetime.now(ZoneInfo(tz))
    daily_name = f"{now_local.strftime('%Y-%m-%d')}.log"
    log_path = log_dir / daily_name

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")

    # Console handler
    console = logging.StreamHandler()
    console.setFormatter(fmt)

    # File handler: rotates at midnight (local time for naming, rotation uses system time)
    # We name the base file using Berlin date; TimedRotatingFileHandler will rotate if it crosses midnight.
    file_handler = TimedRotatingFileHandler(
        filename=str(log_path),
        when="midnight",
        interval=1,
        backupCount=30,   # keep 30 days of rotated backups
        encoding="utf-8",
        utc=False,
    )
    file_handler.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(level)

    # Avoid duplicate handlers if setup_logging() is called more than once
    root.handlers.clear()
    root.addHandler(console)
    root.addHandler(file_handler)

    return log_path
