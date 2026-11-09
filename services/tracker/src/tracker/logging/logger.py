import logging


def get_logger(name: str, stream: bool = False) -> logging.Logger:
    return logging.getLogger(name if not stream else f"{name}.stream")
