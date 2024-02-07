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
