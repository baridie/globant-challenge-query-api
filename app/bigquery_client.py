import os
from google.cloud import bigquery
import logging

logger = logging.getLogger(__name__)

class BigQueryClient:
    def __init__(self):
        self.project_id = os.getenv("PROJECT_ID")
        self.dataset_id = os.getenv("DATASET_ID")
        
        if not self.project_id or not self.dataset_id:
            raise ValueError("PROJECT_ID and DATASET_ID environment variables must be set")
        
        self.client = bigquery.Client(project=self.project_id)
        logger.info(f"BigQuery client initialized for project: {self.project_id}")
    
    def query(self, sql: str):
        """Execute a query and return results as list of dicts"""
        try:
            query_job = self.client.query(sql)
            results = query_job.result()
            
            rows = []
            for row in results:
                rows.append(dict(row))
            
            logger.info(f"Query executed successfully, returned {len(rows)} rows")
            return rows
        except Exception as e:
            logger.error(f"Query error: {str(e)}")
            raise Exception(f"Query error: {str(e)}")

bq_client = BigQueryClient()
