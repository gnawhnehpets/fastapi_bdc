from fastapi import FastAPI, Body
import httpx
from dotenv import load_dotenv
import os
from time import sleep
import requests
from fastapi.concurrency import run_in_threadpool
import traceback


# Load environment variables from .env file
load_dotenv()

app = FastAPI()

# SB CONFIG
BASE_URL = os.getenv("API_ENDPOINT")
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
PROJECT_ID = os.getenv("SB_PROJECT_ID")
APP_MANIFEST_GENERATION_AWS = os.getenv("APP_MANIFEST_GENERATION_AWS")
APP_GCS_DATA_TRANSFER = os.getenv("APP_GCS_DATA_TRANSFER_V2")
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

jobs = {}  # Dictionary to track jobs


@app.get("/apps")
async def get_apps():
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{BASE_URL}/apps",
            headers=HEADERS
        )
        return response.json()

@app.post("/manifest_generation_aws")
async def manifest_generation_aws_v2(
    bucket: str = Body("nih-nhlbi-rti-test-gcp-bucket", embed=True), 
    is_topmed: bool = Body(False, embed=True)):
    print(bucket, is_topmed)
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY = get_proper_credentials(is_topmed)
    
    # nih-nhlbi-rti-test-gcp-bucket
    task_data = {
        # "description": "request sent from fastapi",    
        "name": f"generate_manifest_aws - <fastapi>:{bucket}",
        "app": APP_MANIFEST_GENERATION_AWS,
        "project": PROJECT_ID,
        "execution_settings": {
            "use_memoization": False
        },
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
            f"{BASE_URL}/tasks?action=run",
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
    return response.json()

@app.post("/initiate_aws_transfer_gcs")
async def initiate_aws_transfer_gcs(
    bucket: str = Body("nih-nhlbi-rti-test-gcp-bucket", embed=True), 
    is_topmed: bool = Body(False, embed=True) ):
    print(bucket, is_topmed)
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY = get_proper_credentials(is_topmed)
    
    # nih-nhlbi-rti-test-gcp-bucket
    task_data = {
        # "description": "request sent from fastapi",    
        "name": f"initiate_aws_transfer_gcs - <fastapi>:{bucket}",
        "app": APP_GCS_DATA_TRANSFER,
        "project": PROJECT_ID,
        "execution_settings": {
            "use_memoization": False
        },
        # "output_location": f"volumes://QC/manifest_generation/{bucket}/",
        "inputs": {
            "AWS_DEFAULT_REGION": AWS_DEFAULT_REGION,
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "BUCKET": bucket,
            "NHLBI_RTI_ACCESS_JSON": NHLBI_RTI_ACCESS_JSON
        }
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{BASE_URL}/tasks?action=run",
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
        
@app.post("/manifest_generation_gcs")
async def manifest_generation_gcs(bucket: str = Body("nih-nhlbi-rti-test-gcp-bucket", embed=True), is_topmed: bool = Body(False, embed=True)):
    print(f"bucket: {bucket}, is_topmed: {is_topmed}")
    
    # Get AWS credentials based on the status (is_topmed or not)
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY = get_proper_credentials(is_topmed)
    
    task_data = {
        "name": f"generate_manifest_gcs - <fastapi>:{bucket}",
        "app": APP_MANIFEST_GENERATION_GCS,
        "project": PROJECT_ID,
        "execution_settings": {
            "use_memoization": False
                            },
        "inputs": {
            "AWS_DEFAULT_REGION": AWS_DEFAULT_REGION,
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "BUCKET": bucket,
            "NHLBI_RTI_ACCESS_JSON": NHLBI_RTI_ACCESS_JSON
        }
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{BASE_URL}/tasks?action=run",
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

@app.get("/check_job_status")
async def check_job_status(job_id: str):

    status = "QUEUED"
    poll_url = f"https://api.sb.biodatacatalyst.nhlbi.nih.gov/v2/tasks/{job_id}"
    retries = 0
    retry_limit = 15  # Allow 15 retries with a 20-second interval (total of 5 minutes)

    while status not in ["COMPLETED", "FAILED"] and retries < retry_limit:
        # status_response = requests.request("GET", poll_url, headers=headers)
        status_response = await run_in_threadpool(requests.request, "GET", poll_url, headers=HEADERS)
        status_data = status_response.json()
        print(status_data)

        status = status_data.get("status", "UNKNOWN")
        print(f"Current task status: {status}")

        if status == "COMPLETED":
            print(f"Task {job_id} completed successfully.")
            return status, status_data
        elif status == "FAILED":
            print(f"Task {job_id} failed.")
            print(f"Error details: {status_data.get('errors', 'No error details available')}")
            return status, status_data

        # Wait before polling again
        sleep(20)  # Poll every 20 seconds
        retries += 1

    return status, status_data
    # if job_id in jobs:
    #     return jobs[job_id]
    # return {"error": "Job not found"}

@app.post("/orchestrate_manifest_generation_v2")
async def orchestrate_manifest_generation_v2(
    bucket: str = Body("nih-nhlbi-rti-test-gcp-bucket", embed=True), 
    is_topmed: bool = Body(False, embed=True)
):
    job_id = f"{bucket}-pipeline"
    jobs[job_id] = []
    
    try:
        # Step 1: Trigger manifest_generation_aws
        jobs[job_id].append( {"status": "in progress", "step": "manifest_generation_aws"} )
        manifest_aws_job = await manifest_generation_aws_v2(bucket=bucket, is_topmed=is_topmed)
        manifest_aws_job_id = manifest_aws_job.get("id")
        jobs[job_id].append({"id":manifest_aws_job_id, "status": "in progress", "step": "manifest_generation_aws"})

        manifest_aws_job_status, manifest_aws_job_status_data = await check_job_status(manifest_aws_job_id)

        if manifest_aws_job_status == "COMPLETED":
            jobs[job_id].append({"id":manifest_aws_job_id, "status": "completed", "step": "manifest_generation_aws"})    
            transfer_job = await initiate_aws_transfer_gcs(bucket=bucket, is_topmed=is_topmed)
            transfer_job_id = transfer_job.get("id")
            jobs[job_id].append({"id":transfer_job_id, "status": "in progress", "step": "transfer_aws_gcs"})

            transfer_job_status, transfer_job_status_data = await check_job_status(transfer_job_id)

            if transfer_job_status == "COMPLETED":
                jobs[job_id].append({"id":transfer_job_id, "status": "complete", "step": "transfer_aws_gcs"})
                manifest_gcs_job = await manifest_generation_gcs(bucket=bucket, is_topmed=is_topmed)
                manifest_gcs_job_id = manifest_gcs_job.get("id")
                jobs[job_id].append({"id":transfer_job_id, "status": "in progress", "step": "manifest_generation_gcs"})
                manifest_gcs_job_status, manifest_gcs_job_status_data = await check_job_status(manifest_gcs_job_id)

                if manifest_gcs_job_status == "COMPLETED":
                    jobs[job_id].append({"id":transfer_job_id, "status": "completed", "step": "manifest_generation_gcs"})
                    return {"message": "Pipeline completed successfully", "job_id": job_id}


    except Exception as e:
        traceback.print_exc()  # Log the full error traceback
        jobs[job_id].append({"status": "failed", "error": str(e)})
        return {"error": str(e)}