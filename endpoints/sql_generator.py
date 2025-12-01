from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from loguru import logger
from experiments.question_rewriting import (
    generate_sql_from_question,
    generate_sql_with_rewriting,
    load_schema_and_content_from_file,
)

router = APIRouter()


class PromptPayload(BaseModel):
    """Request body for SQL generation with required schema and optional db_content"""
    prompt: str
    schema: str  # SQL CREATE TABLE statements - REQUIRED
    db_content: Optional[str] = None  # Sample database records for question rewriting


class PromptPayloadWithFile(BaseModel):
    """Request body for SQL generation with schema file containing both schema and records"""
    prompt: str
    schema_file_path: str  # File containing both schema and records - REQUIRED


@router.post("/generate-sql", tags=["Projeto TAES"])
def generate_sql(payload: PromptPayload):
    """
    Generate SQL from a user prompt and required schema.
    Uses question rewriting methodology from DART-SQL.
    
    Args:
        payload: PromptPayload containing:
            - prompt: User's natural language question (required)
            - schema: SQL CREATE TABLE statement(s) (required)
            - db_content: Optional sample database records for question rewriting
    
    Returns:
        Dictionary with generated SQL query
    """
    logger.info(f"Generating SQL with prompt: {payload.prompt}")
    
    try:
        db_schema = payload.schema.strip()
        db_content = payload.db_content or ""
        result = generate_sql_with_rewriting(
            question=payload.prompt,
            db_schema=db_schema,
            db_content=db_content
        )
        return {"SQL": result["generated_sql"]}
    except Exception as e:
        logger.error(f"Error generating SQL: {e}")
        return {"error": str(e)}


@router.post("/generate-sql-with-file", tags=["Projeto TAES"])
def generate_sql_with_file(payload: PromptPayloadWithFile):
    """
    Generate SQL from a user prompt and required schema file.
    
    The schema file should contain both the database schema and sample records.
    Format the file as JSON with "schema" and "records" keys.
    
    Example:
    {
      "schema": "CREATE TABLE equipment_maintenance (equipment_type VARCHAR(255), maintenance_frequency INT);",
      "records": "INSERT INTO equipment_maintenance VALUES ('pump', 30); INSERT INTO equipment_maintenance VALUES ('motor', 15);"
    }
    
    Args:
        payload: PromptPayloadWithFile containing:
            - prompt: User's natural language question (required)
            - schema_file_path: Path to JSON schema file containing both schema and records (required)
    
    Returns:
        Dictionary with generated SQL query
    """
    logger.info(f"Generating SQL with file: {payload.schema_file_path}")
    
    try:
        # Load both schema and content from the file (required)
        db_schema, db_content = load_schema_and_content_from_file(payload.schema_file_path)
        
        result = generate_sql_with_rewriting(
            question=payload.prompt,
            db_schema=db_schema,
            db_content=db_content
        )
        return {"SQL": result["generated_sql"]}
    except Exception as e:
        logger.error(f"Error generating SQL: {e}")
        return {"error": str(e)}
