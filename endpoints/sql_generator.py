from fastapi import APIRouter, File, UploadFile
from pydantic import BaseModel
from typing import Optional
import json
from loguru import logger
from sql_generator import pipeline_generate_sql
from core.database import parse_sql_schema

router = APIRouter()


class PromptPayload(BaseModel):
    """Request body for SQL generation with optional schema"""
    prompt: str
    schema: Optional[str] = None  # SQL CREATE TABLE statements


class PromptPayloadWithFile(BaseModel):
    """Request body for SQL generation with optional schema file path"""
    prompt: str
    schema_file_path: Optional[str] = None


@router.post("/generate-sql", tags=["Projeto TAES"])
def generate_sql(payload: PromptPayload):
    """
    Generate SQL from a user prompt and optional schema.
    
    Args:
        payload: PromptPayload containing:
            - prompt: User's natural language question
            - schema: Optional SQL CREATE TABLE statement(s)
    
    Returns:
        Dictionary with generated SQL query
    """
    logger.info(f"Generating SQL with prompt: {payload.prompt}")
    
    try:
        schema_dict = None
        if payload.schema:
            schema_dict = parse_sql_schema(payload.schema)
        
        sql = pipeline_generate_sql(
            original_prompt=payload.prompt,
            schema=schema_dict
        )
        return {"SQL": sql}
    except Exception as e:
        logger.error(f"Error generating SQL: {e}")
        return {"error": str(e)}


@router.post("/generate-sql-with-file", tags=["Projeto TAES"])
def generate_sql_with_file(payload: PromptPayloadWithFile):
    """
    Generate SQL from a user prompt and optional schema file.
    
    Args:
        payload: PromptPayloadWithFile containing:
            - prompt: User's natural language question
            - schema_file_path: Optional path to schema file (JSON, YAML, or SQL)
    
    Returns:
        Dictionary with generated SQL query
    """
    logger.info(f"Generating SQL with prompt: {payload.prompt}")
    
    try:
        sql = pipeline_generate_sql(
            original_prompt=payload.prompt,
            schema_file_path=payload.schema_file_path
        )
        return {"SQL": sql}
    except Exception as e:
        logger.error(f"Error generating SQL: {e}")
        return {"error": str(e)}
