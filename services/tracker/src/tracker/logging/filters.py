import logging
import re

_MONITOR_POLL_PATH = re.compile(r"^/benchmarks/[^/]+/monitor/tasks$")
_UVICORN_ACCESS_ARG_COUNT = 5


class AccessLogNoiseFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if not isinstance(args, tuple) or len(args) != _UVICORN_ACCESS_ARG_COUNT:
            return "/health" not in record.getMessage()
        path, status = args[2], args[4]
        if not isinstance(path, str):
            return True
        route = path.split("?", 1)[0]
        if "/health" in route:
            return False
        return not (_MONITOR_POLL_PATH.match(route) and isinstance(status, int) and status < 400)


class EngineioNoiseFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.msg != "packet queue is empty, aborting"
