"""Init functions for whole app."""

import logging
import pathlib
import sys
import tomllib
from logging.handlers import RotatingFileHandler

PROJECT_NAME = "appgoblin"

HOME = pathlib.Path.home()

# load config in /home/my-user/appgoblin/config.toml
# Save logs in /home/my-user/appgoblin/config.toml
TOP_CONFIGDIR = pathlib.Path(HOME, pathlib.Path(".config"))
CONFIG_DIR = pathlib.Path(TOP_CONFIGDIR, pathlib.Path(PROJECT_NAME))
CONFIG_FILENAME = "config.toml"
CONFIG_FILE_PATH = pathlib.Path(CONFIG_DIR, CONFIG_FILENAME)
LOG_DIR = pathlib.Path(CONFIG_DIR, pathlib.Path("logs"))
MODULE_DIR = pathlib.Path(__file__).resolve().parent.parent


def handle_exception(exc_type, exc_value, exc_traceback) -> None:  # noqa: ANN001
    """Handle uncaught exceptions for whole app."""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


def check_config_dirs() -> None:
    """Check if config dirs exist and if not create."""
    dirs = [TOP_CONFIGDIR, CONFIG_DIR, LOG_DIR]
    for _dir in dirs:
        if not pathlib.Path.exists(_dir):
            pathlib.Path.mkdir(_dir, exist_ok=True)


def get_logger(mod_name: str, log_name: str = "dash") -> logging.Logger:
    """Get a python logger."""
    logformat = "%(asctime)s: %(name)s: %(levelname)s: %(message)s"
    check_config_dirs()
    log_dir = pathlib.Path(HOME, pathlib.Path(f".config/{PROJECT_NAME}/logs"))
    if not pathlib.Path.exists(log_dir):
        pathlib.Path.mkdir(log_dir, exist_ok=True)
        print(f"Couldn't find {log_dir=} so it was created.")  # noqa: T201
    filename = f"{log_dir}/{log_name}.log"
    # Writes to file
    rotate_handler = RotatingFileHandler(
        filename=filename,
        maxBytes=50000000,
        backupCount=5,
    )
    logging.basicConfig(
        format=logformat,
        level=logging.INFO,
        handlers=[
            rotate_handler,
        ],
    )
    """Retun logger object."""
    # create logger
    logger = logging.getLogger(mod_name)
    logger.setLevel(logging.INFO)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    return logger


# Set global handling of uncaught exceptions
sys.excepthook = handle_exception

logger = get_logger(__name__)

DATE_FORMAT = "%Y-%m-%d"

if not pathlib.Path.exists(CONFIG_FILE_PATH):
    error = f"Couldn't find {CONFIG_FILENAME} please add to {CONFIG_DIR}"
    logger.error(error)
    raise FileNotFoundError(error)


with CONFIG_FILE_PATH.open("rb") as f:
    CONFIG = tomllib.load(f)

DATE_FORMAT = "%Y-%m-%d"
