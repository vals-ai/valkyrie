import os
from uuid import UUID

import boto3
from botocore.config import Config

_KMS_CONFIG = Config(connect_timeout=2, read_timeout=5, retries={"mode": "standard", "total_max_attempts": 3})


def encrypt_grader_token(token: str, *, record_id: UUID, kind: str) -> bytes:
    response = boto3.client("kms", config=_KMS_CONFIG).encrypt(
        KeyId=os.environ["REMOTE_GRADER_KMS_KEY_ID"],
        Plaintext=token.encode(),
        EncryptionContext={f"remote_grader_{kind}_id": str(record_id)},
    )
    return response["CiphertextBlob"]


def decrypt_grader_token(ciphertext: bytes, *, record_id: UUID, kind: str) -> str:
    response = boto3.client("kms", config=_KMS_CONFIG).decrypt(
        KeyId=os.environ["REMOTE_GRADER_KMS_KEY_ID"],
        CiphertextBlob=ciphertext,
        EncryptionContext={f"remote_grader_{kind}_id": str(record_id)},
    )
    return response["Plaintext"].decode()
