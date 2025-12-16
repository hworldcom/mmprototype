import logging
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

def setup_logging(level: str = "INFO", component: str = "app", subdir: str = "default") -> Path:
    """
    Configure logging:
      - Console (stdout)
      - Daily log file in logs/<component>/<symbol>/YYYY-MM-DD.log (Berlin date)
      - Also keeps rotated backups if the process spans midnight

    Returns:
      Path to the "current" daily log file.
    """

    log_dir = Path("logs") / component / subdir
    log_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%Y-%m-%d")
    log_path = log_dir / f"{date_str}.log"

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(sh)
    return log_path
