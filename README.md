# Globant Challenge - Query API

REST API for querying metrics and data from BigQuery for the Globant Data Engineering Challenge.

## Features

- Read-only access to BigQuery
- Metrics endpoints for business intelligence
- API key authentication via Secret Manager
- Deployed on Cloud Run
- CI/CD with GitHub Actions

## API Endpoints

### Health Check
cat > requirements.txt << 'EOF'
fastapi==0.104.1
uvicorn[standard]==0.24.0
google-cloud-bigquery==3.13.0
google-cloud-secret-manager==2.16.4
pydantic==2.5.0
pytest==7.4.3
httpx==0.25.1
pytest-asyncio==0.21.1
