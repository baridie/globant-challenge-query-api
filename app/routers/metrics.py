from fastapi import APIRouter, HTTPException
from ..bigquery_client import bq_client
import os
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("/metrics/hires-by-quarter")
def hires_by_quarter():
    """
    Number of employees hired for each job and department in 2021 divided by quarter.
    Ordered alphabetically by department and job.
    
    Returns:
        List of dicts with: department, job, Q1, Q2, Q3, Q4
    """
    dataset_id = os.getenv("DATASET_ID")
    
    query = f"SELECT * FROM `{dataset_id}.rpt_hired_by_q_2021`"

    try:
        logger.info("Executing hires-by-quarter query")
        results = bq_client.query(query)
        return results
    except Exception as e:
        logger.error(f"Error in hires-by-quarter: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/metrics/departments-above-mean")
def departments_above_mean():
    """
    List of departments that hired more employees than the mean in 2021.
    Ordered by number of employees hired (descending).
    
    Returns:
        List of dicts with: id, department, hired
    """
    dataset_id = os.getenv("DATASET_ID")
    
    query = f"SELECT * FROM `{dataset_id}.rpt_hired_by_dept_avg`"
    
    try:
        logger.info("Executing departments-above-mean query")
        results = bq_client.query(query)
        return results
    except Exception as e:
        logger.error(f"Error in departments-above-mean: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

