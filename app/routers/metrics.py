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
    
    query = f"""
    SELECT 
        d.department,
        j.job,
        COUNTIF(EXTRACT(QUARTER FROM e.datetime) = 1) AS Q1,
        COUNTIF(EXTRACT(QUARTER FROM e.datetime) = 2) AS Q2,
        COUNTIF(EXTRACT(QUARTER FROM e.datetime) = 3) AS Q3,
        COUNTIF(EXTRACT(QUARTER FROM e.datetime) = 4) AS Q4
    FROM 
        `{dataset_id}.hired_employees` e
    INNER JOIN 
        `{dataset_id}.departments` d ON e.department_id = d.id
    INNER JOIN 
        `{dataset_id}.jobs` j ON e.job_id = j.id
    WHERE 
        EXTRACT(YEAR FROM e.datetime) = 2021
    GROUP BY 
        d.department, j.job
    ORDER BY 
        d.department ASC, j.job ASC
    """
    
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
    
    query = f"""
    WITH department_hires AS (
        SELECT 
            d.id,
            d.department,
            COUNT(e.id) AS hired
        FROM 
            `{dataset_id}.departments` d
        LEFT JOIN 
            `{dataset_id}.hired_employees` e 
            ON d.id = e.department_id 
            AND EXTRACT(YEAR FROM e.datetime) = 2021
        GROUP BY 
            d.id, d.department
    ),
    mean_hires AS (
        SELECT AVG(hired) AS mean_hired
        FROM department_hires
        WHERE hired > 0
    )
    SELECT 
        dh.id,
        dh.department,
        dh.hired
    FROM 
        department_hires dh
    CROSS JOIN 
        mean_hires mh
    WHERE 
        dh.hired > mh.mean_hired
    ORDER BY 
        dh.hired DESC
    """
    
    try:
        logger.info("Executing departments-above-mean query")
        results = bq_client.query(query)
        return results
    except Exception as e:
        logger.error(f"Error in departments-above-mean: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/metrics/table-stats/{table_name}")
def table_stats(table_name: str):
    """
    Get basic statistics for a table (for debugging/verification)
    
    Args:
        table_name: Name of the table (departments, jobs, or hired_employees)
    
    Returns:
        Dict with: total_rows, unique_ids
    """
    if table_name not in ['departments', 'jobs', 'hired_employees']:
        raise HTTPException(status_code=400, detail="Invalid table name")
    
    dataset_id = os.getenv("DATASET_ID")
    
    query = f"""
    SELECT 
        COUNT(*) AS total_rows,
        COUNT(DISTINCT id) AS unique_ids
    FROM 
        `{dataset_id}.{table_name}`
    """
    
    try:
        logger.info(f"Getting stats for table: {table_name}")
        results = bq_client.query(query)
        return results[0] if results else {"total_rows": 0, "unique_ids": 0}
    except Exception as e:
        logger.error(f"Error getting table stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
