import logging
import os

from dotenv import load_dotenv

CLI_LOGS_ENV_VAR = "VALKYRIE_CLI_LOGS"


def configure_cli_logging() -> None:
    if os.environ.get(CLI_LOGS_ENV_VAR) == "true":
        logging.disable(logging.NOTSET)
        return

    logging.disable(logging.CRITICAL)


def load_cli_environment() -> None:
    configure_cli_logging()
    load_dotenv()
    configure_cli_logging()
