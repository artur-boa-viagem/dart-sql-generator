import uuid
from loguru import logger
from openai import OpenAI

from core.config import settings
from core.database import format_schema_for_prompt, validate_schema

client = OpenAI(api_key=settings.PROJETO_TAES_OPENAI_API_KEY)


def improve_prompt(original_prompt: str, schema: dict | None = None) -> str:
    """
    Chama a OpenAI para melhorar/expandir o prompt que foi enviado pelo usuário.
    
    Args:
        original_prompt: The user's original question/prompt
        schema: Optional database schema dictionary
    
    Returns:
        Improved prompt string
    """
    logger.info("Chamando OpenAI para melhorar prompt SQL...")

    try:
        schema_context = ""
        if schema and validate_schema(schema):
            schema_context = "\n\n" + format_schema_for_prompt(schema)
        
        system_message = (
            "You are a database-aware question rewriting assistant. Your job: "
            "given a user natural language question + a brief description of the database " 
            "schema and several example rows from relevant tables, rewrite the user's question " 
            "to be unambiguous and directly grounded in the database content. Preserve intent, "
            "but replace or expand vague terms with the exact table/column values or identifiers "
            "found in the provided sample rows. If multiple plausible mappings exist, list them and pick the best one as the primary rewrite."
        )
        
        if schema_context:
            system_message += schema_context
        
        response = client.chat.completions.create(
            model="gpt-4.1",
            messages=[
                {
                    "role": "system",
                    "content": system_message,
                },
                {"role": "user", "content": original_prompt},
            ],
        )

        improved = response.choices[0].message.content.strip()
        logger.info(f"Prompt melhorado: {improved}")
        return improved

    except Exception as e:
        logger.error(f"Erro ao melhorar prompt: {e}")
        raise RuntimeError("Erro ao melhorar prompt.") from e


def generate_sql_from_prompt(prompt: str, schema: dict | None = None) -> str:
    """
    Chama a OpenAI para gerar SQL a partir do prompt melhorado.
    
    Args:
        prompt: The improved prompt/question
        schema: Optional database schema dictionary
    
    Returns:
        Generated SQL query string
    """
    logger.info("Chamando OpenAI para gerar SQL...")

    try:
        schema_context = ""
        if schema and validate_schema(schema):
            schema_context = "\n\n" + format_schema_for_prompt(schema)
        
        system_message = (
            "You are a Text-to-SQL generator. Use the schema and the rewritten "
            "question to produce a single valid SQL query, no explanations."
        )
        
        if schema_context:
            system_message += schema_context
        
        response = client.chat.completions.create(
            model="gpt-4.1",
            messages=[
                {
                    "role": "system",
                    "content": system_message,
                },
                {"role": "user", "content": prompt},
            ],
        )

        sql_code = response.choices[0].message.content.strip()
        logger.info("SQL gerado com sucesso.")
        return sql_code

    except Exception as e:
        logger.error(f"Erro ao gerar SQL: {e}")
        raise RuntimeError("Erro ao gerar SQL.") from e



def pipeline_generate_sql(
    original_prompt: str, 
    schema: dict | None = None,
    schema_file_path: str | None = None,
    request_id: uuid.UUID | None = None
) -> str:
    """
    Fluxo completo:
    1. Recebe prompt via FastAPI
    2. Carrega schema (se fornecido como arquivo ou input)
    3. Melhora prompt usando OpenAI com contexto do schema
    4. Usa prompt melhorado para gerar SQL final
    
    Args:
        original_prompt: The user's original question/prompt
        schema: Optional database schema as dictionary
        schema_file_path: Optional path to schema file (JSON or YAML)
        request_id: Optional request identifier for logging
    
    Returns:
        Generated SQL query string
    """
    request_id = request_id or uuid.uuid4()
    logger.info(f"{request_id}, pipeline_generate_sql, started")

    # Load schema from file if provided
    if schema_file_path:
        from core.database import load_schema_from_file
        schema = load_schema_from_file(schema_file_path)
    
    improved_prompt = improve_prompt(original_prompt, schema)
    final_sql = generate_sql_from_prompt(improved_prompt, schema)

    logger.info(f"{request_id}, pipeline_generate_sql, done")
    return final_sql
