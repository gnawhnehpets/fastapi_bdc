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

To start the API service, run:
`fastapi dev main.py`

