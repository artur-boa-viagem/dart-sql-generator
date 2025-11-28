"""
Database utilities for schema extraction and formatting.
"""
import json
import re
from pathlib import Path
from loguru import logger


def parse_sql_schema(sql_statements: str) -> dict:
    """
    Parse SQL CREATE TABLE statements into a schema dictionary.
    
    Args:
        sql_statements: SQL CREATE TABLE statement(s)
    
    Returns:
        Dictionary containing the parsed schema
    """
    logger.info("Parsing SQL schema")
    
    schema = {}
    
    # Find all CREATE TABLE statements
    create_table_pattern = r'CREATE\s+TABLE\s+(\w+)\s*\((.*?)\);'
    matches = re.finditer(create_table_pattern, sql_statements, re.IGNORECASE | re.DOTALL)
    
    for match in matches:
        table_name = match.group(1)
        columns_str = match.group(2)
        
        schema[table_name] = {
            "columns": {},
            "primary_key": None,
            "foreign_keys": []
        }
        
        # Parse columns
        column_definitions = [col.strip() for col in columns_str.split(',')]
        
        for col_def in column_definitions:
            col_def = col_def.strip()
            
            # Check for primary key
            if col_def.upper().startswith('PRIMARY KEY'):
                pk_match = re.search(r'PRIMARY\s+KEY\s*\((\w+)\)', col_def, re.IGNORECASE)
                if pk_match:
                    schema[table_name]["primary_key"] = pk_match.group(1)
            
            # Check for foreign key
            elif col_def.upper().startswith('FOREIGN KEY'):
                fk_match = re.search(
                    r'FOREIGN\s+KEY\s*\((\w+)\)\s+REFERENCES\s+(\w+)\s*\((\w+)\)',
                    col_def, re.IGNORECASE
                )
                if fk_match:
                    fk_str = f"{fk_match.group(1)} -> {fk_match.group(2)}.{fk_match.group(3)}"
                    schema[table_name]["foreign_keys"].append(fk_str)
            
            # Parse column with name and type
            else:
                parts = col_def.split(None, 1)
                if len(parts) >= 2:
                    col_name = parts[0]
                    col_type = parts[1]
                    schema[table_name]["columns"][col_name] = col_type
                elif len(parts) == 1 and parts[0]:
                    schema[table_name]["columns"][parts[0]] = "VARCHAR(255)"
    
    logger.info(f"Parsed schema for {len(schema)} table(s)")
    return schema


def load_schema_from_file(file_path: str) -> dict:
    """
    Load database schema from a JSON, YAML, or SQL file.
    
    Args:
        file_path: Path to the schema file (JSON, YAML, or SQL)
    
    Returns:
        Dictionary containing the schema
    """
    logger.info(f"Loading schema from file: {file_path}")
    
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"Schema file not found: {file_path}")
    
    try:
        if file_path.suffix.lower() == '.json':
            with open(file_path, 'r') as f:
                schema = json.load(f)
        elif file_path.suffix.lower() in ['.yaml', '.yml']:
            import yaml
            with open(file_path, 'r') as f:
                schema = yaml.safe_load(f)
        elif file_path.suffix.lower() == '.sql':
            with open(file_path, 'r') as f:
                sql_content = f.read()
            schema = parse_sql_schema(sql_content)
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")
        
        logger.info("Schema loaded successfully")
        return schema
    
    except Exception as e:
        logger.error(f"Error loading schema from file: {e}")
        raise


def format_schema_for_prompt(schema: dict) -> str:
    """
    Format database schema into a readable string for LLM prompts.
    
    Args:
        schema: Dictionary containing database schema
    
    Returns:
        Formatted schema string
    """
    logger.info("Formatting schema for prompt")
    
    formatted = "DATABASE SCHEMA:\n\n"
    
    if isinstance(schema, dict):
        for table_name, table_info in schema.items():
            formatted += f"Table: {table_name}\n"
            
            if isinstance(table_info, dict):
                if 'columns' in table_info:
                    formatted += "Columns:\n"
                    for col_name, col_type in table_info['columns'].items():
                        formatted += f"  - {col_name}: {col_type}\n"
                
                if 'primary_key' in table_info and table_info['primary_key']:
                    formatted += f"Primary Key: {table_info['primary_key']}\n"
                
                if 'foreign_keys' in table_info and table_info['foreign_keys']:
                    formatted += "Foreign Keys:\n"
                    for fk in table_info['foreign_keys']:
                        formatted += f"  - {fk}\n"
            
            formatted += "\n"
    
    return formatted


def validate_schema(schema: dict) -> bool:
    """
    Validate that the schema has the expected structure.
    
    Args:
        schema: Dictionary containing database schema
    
    Returns:
        True if schema is valid, False otherwise
    """
    if not isinstance(schema, dict):
        logger.warning("Schema must be a dictionary")
        return False
    
    if len(schema) == 0:
        logger.warning("Schema is empty")
        return False
    
    logger.info("Schema validation passed")
    return True
