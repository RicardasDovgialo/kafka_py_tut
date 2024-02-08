from google.cloud import secretmanager



def get_secret(secret_id: str, version_id="latest"):
    """
    Get information about the given secret. This only returns metadata about
    the secret container, not any secret material.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/youtube-notifications-example/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)

    return response.payload.data.decode('UTF-8')

KAFKA_CONFIG = {
    "bootstrap.servers": "...:9020",
    "security.protocol": "sasl_ssl",
    "sasl.mechanisim": "PLAIN",
    "sasl.username": "...",
    "sasl.password": get_secret("local_kafka_pass"),
}

KAFKA_SCHEMA_REGISTRY = {
    "url": "...",
    "basic.auth.user.info": get_secret("local_sr_api_creds"),
}

x = KAFKA_SCHEMA_REGISTRY.pop("url")

print(x)
print("haha")
print(KAFKA_SCHEMA_REGISTRY)