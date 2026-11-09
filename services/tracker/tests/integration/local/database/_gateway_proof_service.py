import asyncio
import json
import os
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4

import uvicorn
from redis.asyncio import Redis

import model_gateway.app as gateway_app
import model_gateway.model_helpers as model_helpers
import model_gateway.managed_auth.tables  # noqa: F401
from model_gateway.managed_auth.authority_publisher import ManagedAuthorityPublisher
from model_gateway.managed_auth.credentials import create_managed_key_secret
from model_gateway.managed_auth.database import build_database_url, create_engine, metadata
from model_gateway.managed_auth.reconciliation import HumanReconciler
from model_gateway.managed_auth.store import ManagedAuthStore
from model_gateway.managed_auth.types import HumanIdentity, PersonalOwner, ProjectReference
from model_library.base.output import QueryResult


class SimulatedProvider:
    delegate = None

    async def query(self, inputs, **kwargs):
        return QueryResult(output_text="simulated grader")


class Settings:
    def get(self, name, default=""):
        return {
            "MODEL_GATEWAY_API_KEYS": json.dumps({"operations": "sk-local"}),
            "MODEL_GATEWAY_HMAC_SECRET": "local-only-secret",
        }.get(name, default)


async def main():
    engine = create_engine(
        build_database_url(
            json.loads(os.environ["MODEL_GATEWAY_AUTH_DATABASE_SECRET"]),
            os.environ["MODEL_GATEWAY_AUTH_DATABASE_NAME"],
        ),
        4,
    )
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(metadata.create_all)
        store = ManagedAuthStore(engine)
        owner = HumanIdentity("https://issuer.example", f"owner-{uuid4()}")
        project_id = UUID(os.environ["TRACKER_PROOF_PROJECT_ID"])
        project = ProjectReference(project_id, "organization", "development")
        root, verifier = create_managed_key_secret()
        key_id = uuid4()
        namespace = f"tracker-proof-{uuid4()}"
        owner_token = uuid4().hex
        os.environ.update(
            MODEL_GATEWAY_AUTH_NAMESPACE=namespace,
            GATEWAY_RUNTIME_ROLE="combined",
            MODEL_GATEWAY_AUTH_POOL_SIZE="4",
            DESCOPE_PROJECT_ID="local",
            DESCOPE_EXPECTED_ISSUER=owner.issuer,
            DESCOPE_ORGANIZATION_ID="organization",
            DESCOPE_MANAGEMENT_CREDENTIAL="local-only",
            DESCOPE_MACHINE_ACCESS_KEY_ID=f"machine-{uuid4()}",
            DESCOPE_MACHINE_SERVICE_OWNER_ID=str(uuid4()),
        )
        await redis.set(f"gateway:managed-auth:{namespace}:owner", owner_token, nx=True, ex=3600)
        await store.record_human(owner, True, owner)
        request = await store.create_request(
            uuid4(),
            "grader project",
            "proof",
            owner,
            PersonalOwner(owner),
            None,
            project,
        )
        pending = await store.decide_request(request.id, owner, key_id, (None, None))
        await store.collect_key(key_id, pending.revision, verifier, owner)
        with (
            patch.object(gateway_app, "model_library_settings", Settings()),
            patch.object(model_helpers, "get_registry_model", return_value=SimulatedProvider()),
        ):
            app = gateway_app.create_app()
            async with app.router.lifespan_context(app):
                publisher = ManagedAuthorityPublisher(
                    engine,
                    redis,
                    HumanReconciler(store, app.state.managed_auth_verifier, owner.issuer),
                    namespace=namespace,
                )
                await publisher.publish_once(owner_token)
                Path(os.environ["TRACKER_PROOF_READY_FILE"]).write_text(
                    json.dumps({"root": root, "key_id": str(key_id), "project_id": str(project_id)})
                )
                server = uvicorn.Server(
                    uvicorn.Config(
                        app,
                        host="127.0.0.1",
                        port=int(os.environ["TRACKER_PROOF_GATEWAY_PORT"]),
                        lifespan="off",
                        log_level="error",
                    )
                )
                await server.serve()
    finally:
        await redis.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
