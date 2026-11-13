import argparse
import os
from pathlib import Path

import uvicorn

from tracker.local import config


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, help="Local execution resource configuration.")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on.")
    parser.add_argument(
        "--timeout-keep-alive", type=int, default=5, help="Seconds to keep idle client connections open."
    )
    args = parser.parse_args()
    if args.config is not None:
        config.configure(args.config)
        os.environ["EXECUTOR_LAUNCHER"] = "local"
        os.environ.pop("EXECUTOR_PAYLOAD_KMS_KEY_ID", None)
        from tracker.local.releases import register_checkout_release

        register_checkout_release()
    is_local = config.resources is not None
    uvicorn.run(
        "main:app",
        host="127.0.0.1" if is_local else "0.0.0.0",
        port=args.port,
        workers=1 if is_local else 2,
        timeout_keep_alive=args.timeout_keep_alive,
        log_config=None,
    )


if __name__ == "__main__":
    main()
