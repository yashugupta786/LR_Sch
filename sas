import os
import uuid
from datetime import datetime, timedelta
from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
    ContainerClient,
    generate_blob_sas,
    BlobSasPermissions,
    ContentSettings
)

# Replace with your actual Azure Blob credentials
AZURE_STORAGE_ACCOUNT_NAME = "<your-account-name>"
AZURE_STORAGE_ACCOUNT_KEY = "<your-account-key>"

# Replace with tenant-specific info
TENANT_ID = "CITI"
RUN_ID = "RUN_CITI_CRE_2024_Q1_001"
INDUSTRY = "CRE"
OBLIGOR = "Sherwood Village Cooperative D Inc"
LOCAL_FILE_PATH = "loan.pdf"  # Your test file
RENAMED_FILE = f"loan_{TENANT_ID}_{INDUSTRY}_Q1_2024.pdf"


def generate_container_name(tenant_id):
    return f"loan-docs-{tenant_id.lower()}"


def generate_blob_path(run_id, industry, obligor, file_name):
    safe_obligor = obligor.replace(" ", "_")
    return f"{run_id}/{industry}/{safe_obligor}/{file_name}"


def create_blob_client():
    account_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE_ACCOUNT_KEY)


def ensure_container(blob_service_client, container_name):
    container_client = blob_service_client.get_container_client(container_name)
    try:
        container_client.create_container()
        print(f"✅ Container created: {container_name}")
    except Exception:
        print(f"ℹ️ Container already exists: {container_name}")


def upload_file(blob_service_client, container_name, blob_path, file_path):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True, content_settings=ContentSettings(content_type='application/pdf'))
    print(f"📤 Uploaded to: {blob_path}")


def generate_sas_url(container_name, blob_path):
    sas_token = generate_blob_sas(
        account_name=AZURE_STORAGE_ACCOUNT_NAME,
        container_name=container_name,
        blob_name=blob_path,
        account_key=AZURE_STORAGE_ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(days=365 * 10)  # Max ~10 years
    )
    blob_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{container_name}/{blob_path}"
    full_url = f"{blob_url}?{sas_token}"
    return full_url


# ======= MAIN SCRIPT ========
if __name__ == "__main__":
    blob_service_client = create_blob_client()
    container_name = generate_container_name(TENANT_ID)
    blob_path = generate_blob_path(RUN_ID, INDUSTRY, OBLIGOR, RENAMED_FILE)

    # Step 1: Ensure container
    ensure_container(blob_service_client, container_name)

    # Step 2: Upload file
    upload_file(blob_service_client, container_name, blob_path, LOCAL_FILE_PATH)

    # Step 3: Generate SAS URL
    sas_url = generate_sas_url(container_name, blob_path)
    print(f"\n🔗 SAS URL:\n{sas_url}")
