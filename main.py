from fastapi import FastAPI, Body
import httpx
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

app = FastAPI()

# SB CONFIG
BASE_URL = os.getenv("API_ENDPOINT")
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
PROJECT_ID = os.getenv("SB_PROJECT_ID")
APP_MANIFEST_GENERATION_AWS = os.getenv("APP_MANIFEST_GENERATION_AWS")
APP_GCS_DATA_TRANSFER = os.getenv("APP_GCS_DATA_TRANSFER")
APP_MANIFEST_GENERATION_GCS = os.getenv("APP_MANIFEST_GENERATION_GCS")

# AWS CONFIG
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
AWS_ACCESS_KEY_ID_NHLBI = os.getenv("SECRET_KEY_ID_NHLBI")
AWS_SECRET_ACCESS_KEY_NHLBI = os.getenv("SECRET_ACCESS_KEY_NHLBI")
AWS_ACCESS_KEY_ID_TOPMED = os.getenv("SECRET_KEY_ID_TOPMED")
AWS_SECRET_ACCESS_KEY_TOPMED = os.getenv("SECRET_ACCESS_KEY_TOPMED")
BUCKET = os.getenv("BUCKET")
NHLBI_RTI_ACCESS_JSON = os.getenv("NHLBI_RTI_ACCESS_JSON")

def get_proper_credentials(status):
    if status:
        print(AWS_ACCESS_KEY_ID_TOPMED)
        return AWS_ACCESS_KEY_ID_TOPMED, AWS_SECRET_ACCESS_KEY_TOPMED
    else:
        print(AWS_ACCESS_KEY_ID_NHLBI)
        return AWS_ACCESS_KEY_ID_NHLBI, AWS_SECRET_ACCESS_KEY_NHLBI


HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "X-SBG-Auth-Token": AUTH_TOKEN,
}

@app.get("/apps")
async def get_apps():
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{BASE_URL}/apps",
            headers=HEADERS
        )
        return response.json()

@app.post("/manifest_generation_aws")
async def manifest_generation_aws(bucket: str = Body("nih-nhlbi-rti-test-gcp-bucket", embed=True), is_topmed: bool = Body(False, embed=True)):
    print(bucket, is_topmed)
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY = get_proper_credentials(is_topmed)
    
    # nih-nhlbi-rti-test-gcp-bucket
    task_data = {
        # "description": "request sent from fastapi",    
        "name": f"generate_manifest_aws - fastapi - {bucket}",
        "app": APP_MANIFEST_GENERATION_AWS,
        "project": PROJECT_ID,
        # "output_location": f"volumes://QC/manifest_generation/{bucket}/",
        "inputs": {
            "AWS_DEFAULT_REGION": AWS_DEFAULT_REGION,
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "BUCKET": bucket
        }
    }
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{BASE_URL}/tasks",
            headers=HEADERS,
            json=task_data
        )
        
        try:
            return response.json()
        except Exception as e:
            return {
                "error": str(e),
                "content": response.text,
                "status_code": response.status_code
            }

@app.post("/initiate_aws_transfer_gcs")
async def initiate_aws_transfer_gcs(bucket: str = Body("nih-nhlbi-rti-test-gcp-bucket", embed=True), is_topmed: bool = Body(False, embed=True), transfer_job_exists: bool = Body(False, embed=True)):
    print(bucket, is_topmed, transfer_job_exists)
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY = get_proper_credentials(is_topmed)
    
    # nih-nhlbi-rti-test-gcp-bucket
    task_data = {
        # "description": "request sent from fastapi",    
        "name": f"initiate_aws_transfer_gcs - fastapi - {bucket}",
        "app": APP_GCS_DATA_TRANSFER,
        "project": PROJECT_ID,
        # "output_location": f"volumes://QC/manifest_generation/{bucket}/",
        "inputs": {
            "AWS_DEFAULT_REGION": AWS_DEFAULT_REGION,
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "BUCKET": bucket,
            "NHLBI_RTI_ACCESS_JSON": NHLBI_RTI_ACCESS_JSON
        }
    }

    if transfer_job_exists:
        task_data["inputs"]["CREATE_OR_RUN"] = "--run"

    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{BASE_URL}/tasks",
            headers=HEADERS,
            json=task_data
        )
        
        try:
            return response.json()
        except Exception as e:
            return {
                "error": str(e),
                "content": response.text,
                "status_code": response.status_code
            }