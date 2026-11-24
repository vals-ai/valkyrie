import argparse
import asyncio
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
        from tracker.database.session import engine
        from tracker.executor.dispatch_recovery import recover_interrupted_local_dispatches
        from tracker.local.releases import register_checkout_release
        from tracker.repositories.advisory_lock import local_tracker_lock

        if not asyncio.run(local_tracker_lock(engine).acquire()):
            raise SystemExit("Another local Tracker is already using this database. Stop it first.")

        server_config = uvicorn.Config(
            "main:app",
            host="127.0.0.1",
            port=args.port,
            timeout_keep_alive=args.timeout_keep_alive,
            log_config=None,
            access_log=False,
        )
        with server_config.bind_socket() as listener:
            register_checkout_release()
            recover_interrupted_local_dispatches()
            uvicorn.Server(server_config).run(sockets=[listener])
        return
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=args.port,
        workers=2,
        timeout_keep_alive=args.timeout_keep_alive,
        log_config=None,
    )


if __name__ == "__main__":
    main()
