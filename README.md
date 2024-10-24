## API service for Seven Bridges Platform

### Background
Need to:
- trigger manifest generation app (AWS)
- trigger data transfer
- trigger manifest generation app (GCS)
- update Jira ticket

### Usage:
Update the `.env` file with your authorization token from Seven Bridges along with tokens from Jira and Fresh Desk.

Seven Bridge app IDs can be obtained from previously run tasks.

#### Without
To start the API service, run:
`fastapi dev main.py`


#### Docker
##### 1. with docker
Build the image
`docker build -t progress_tracker_api .`

Run the container
`docker run -d -p 8000:80 --env-file .env fastapi-app`

###### 2. with docker compose
or run docker-compose:
`docker-compose up --build`