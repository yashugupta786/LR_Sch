import logging
from azure.storage.blob import ContentSettings
from azure.core.exceptions import ResourceExistsError
from datafusion_common_loanai_services.azure_blob.azure_blob_connection_manager import AzureBlobConnectionManager

# -----------------------------
# Custom Helper Functions
# -----------------------------

def generate_container_name(tenant_id: str) -> str:
    return f"loan-docs-{tenant_id.lower()}"

def generate_blob_path(run_id: str, industry: str, obligor: str, file_name: str) -> str:
    safe_obligor = obligor.replace(" ", "_")
    return f"{run_id}/{industry}/{safe_obligor}/{file_name}"

# -----------------------------
# Main Blob Logic
# -----------------------------

def upload_to_blob(
    tenant_id: str,
    run_id: str,
    industry: str,
    obligor: str,
    file_name: str,
    file_bytes: bytes
):
    logger = logging.getLogger("BlobUploader")
    logging.basicConfig(level=logging.INFO)

    # Step 1: Initialize blob client using vault secrets
    blob_conn_mgr = AzureBlobConnectionManager()
    blob_service_client = blob_conn_mgr.get_blob_service_client()

    container_name = generate_container_name(tenant_id)
    blob_path = generate_blob_path(run_id, industry, obligor, file_name)

    # Step 2: Ensure container exists
    try:
        blob_service_client.create_container(container_name)
        logger.info(f"✅ Created new container: {container_name}")
    except ResourceExistsError:
        logger.info(f"ℹ️ Container already exists: {container_name}")

    # Step 3: Upload file
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

    blob_client.upload_blob(
        file_bytes,
        overwrite=True,
        content_settings=ContentSettings(content_type="application/pdf")
    )
    logger.info(f"✅ File uploaded to: {blob_path}")
    return blob_path

# -----------------------------
# Run This Test
# -----------------------------

if __name__ == "__main__":
    # Dummy file for testing
    tenant_id = "CITI"
    run_id = "RUN_CITI_CRE_2024_Q1_001"
    industry = "CRE"
    obligor = "Sherwood Village Cooperative D Inc"
    file_name = "loan_CITI_CRE_Q1_2024.pdf"

    with open("sample.pdf", "rb") as f:
        file_bytes = f.read()

    blob_path = upload_to_blob(
        tenant_id=tenant_id,
        run_id=run_id,
        industry=industry,
        obligor=obligor,
        file_name=file_name,
        file_bytes=file_bytes
    )

    print(f"✅ Uploaded blob path: {blob_path}")
