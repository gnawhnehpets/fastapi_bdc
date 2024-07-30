from fastapi import FastAPI
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
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET = os.getenv("BUCKET")


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

@app.post("/tasks")
async def create_task():
    task_data = {
        "description": "request sent from postman",
        "name": "generate_manifest_aws - POSTMAN - nih-nhlbi-rti-test-gcp-bucket",
        "app": APP_MANIFEST_GENERATION_AWS,
        "project": PROJECT_ID,
        "inputs": {
            "AWS_DEFAULT_REGION": AWS_DEFAULT_REGION,
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "BUCKET": BUCKET
        }
    }
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{BASE_URL}/tasks",
            headers=HEADERS,
            json=task_data
        )
        return response.json()