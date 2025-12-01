"""Question Rewriting seguindo metodologia DART-SQL"""
from loguru import logger
from openai import OpenAI
from pathlib import Path
import json
from core.config import settings

client = OpenAI(api_key=settings.PROJETO_TAES_OPENAI_API_KEY)

# Modelo usado no experimento
MODEL = "gpt-5-nano"

def build_rewriting_prompt(question: str, db_content: str) -> str:
    """
    Prompt SIMPLIFICADO para teste - GPT-5 nano parece ter problemas com prompts longos
    """
    
    return f"""Rewrite the question to make it clearer and more specific using the database content provided.

Database content (first records):
{db_content}

Original question: {question}

Rewritten question (in English, only the question without explanations):"""

def rewrite_question(question: str, db_content: str = "") -> str:
    """
    Reescreve pergunta usando LLM com conteúdo do banco.
    
    Args:
        question: Questão original
        db_content: K=5 registros de cada tabela
    
    Returns:
        Questão reescrita
    """
    logger.info(f"Reescrevendo: {question}")
    
    prompt = build_rewriting_prompt(question, db_content)
    
    # DEBUG: Verificar tamanho do prompt
    logger.debug(f"📏 Tamanho do prompt: {len(prompt)} caracteres")
    logger.debug(f"📏 Primeiros 2000 chars: {prompt[:2000]}")
    
    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "user", "content": prompt}
            ],
            max_completion_tokens=2000  # Aumentado de 500 para 2000
        )
        
        rewritten = response.choices[0].message.content.strip()
        
        # Limpar prefixos comuns que o modelo pode adicionar
        prefixes = ["Pergunta reescrita:", "Reescrita:", "Resposta:"]
        for prefix in prefixes:
            if rewritten.startswith(prefix):
                rewritten = rewritten[len(prefix):].strip()
        
        # DEBUG: Log completo da resposta
        logger.debug(f"Resposta do modelo (completa): '{rewritten}'")
        logger.debug(f"Tamanho da resposta: {len(rewritten)} caracteres")
        logger.debug(f"Finish reason: {response.choices[0].finish_reason}")
        
        if not rewritten:
            logger.warning("⚠️ Modelo retornou string vazia! Usando questão original.")
            return question
        
        logger.info(f"Reescrita: {rewritten}")
        return rewritten
        
    except Exception as e:
        logger.error(f"Erro ao reescrever: {e}")
        # Fallback: retorna original
        return question

def generate_sql_from_question(question: str, db_schema: str) -> str:
    """
    Gera SQL a partir da pergunta (original ou reescrita) + schema.
    Usa o mesmo prompt zero-shot para ambos os baselines.
    
    Args:
        question: Questão (original ou reescrita)
        db_schema: Schema do banco (CREATE TABLE statements)
    
    Returns:
        Query SQL gerada
    """
    logger.info(f"Gerando SQL para: {question}")
    
    # Prompt Zero-Shot padrão (similar ao DAIL-SQL sem exemplos)
    system_prompt = """You are a SQL expert. Generate a SQL query based on the question and database schema provided.

Rules:
- Return ONLY the SQL query, no explanations or translations
- The question may be in Portuguese or English - generate SQL regardless
- Use proper SQL syntax
- Follow the schema exactly as provided
- Do NOT translate or explain the question, just generate the SQL"""

    user_prompt = f"""### Database Schema:
{db_schema}

### Question:
{question}

### SQL Query:"""

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_completion_tokens=2000  # Aumentado de 500 para 2000
        )
        
        sql = response.choices[0].message.content.strip()
        
        # DEBUG: Log resposta antes de processar
        logger.debug(f"SQL bruto recebido: '{sql[:200]}'")
        logger.debug(f"Finish reason: {response.choices[0].finish_reason}")
        
        # Remove markdown se presente
        if sql.startswith("```"):
            lines = sql.split("\n")
            sql = "\n".join(lines[1:-1]) if len(lines) > 2 else sql
            sql = sql.replace("```sql", "").replace("```", "").strip()
        
        if not sql:
            logger.warning(f"⚠️ SQL vazio após processar! Questão era: {question[:100]}")
        
        logger.info(f"SQL gerado: {sql[:100]}...")
        return sql
        
    except Exception as e:
        logger.error(f"Erro ao gerar SQL: {e}")
        raise

def generate_sql_with_rewriting(question: str, db_schema: str, db_content: str) -> dict:
    """
    Pipeline RW-Enhanced Zero-Shot:
    1. Reescreve a questão usando conteúdo do banco
    2. Gera SQL da questão reescrita + schema
    
    Returns:
        Dict com questão original, reescrita e SQL gerado
    """
    # Etapa 1: Question Rewriting
    rewritten_question = rewrite_question(question, db_content)
    
    # Etapa 2: SQL Generation da questão reescrita
    sql = generate_sql_from_question(rewritten_question, db_schema)
    
    return {
        "original_question": question,
        "rewritten_question": rewritten_question,
        "generated_sql": sql
    }


def load_schema_from_file(file_path: str) -> str:
    """
    Carrega schema de um arquivo (SQL, JSON, ou YAML).
    
    Args:
        file_path: Caminho para o arquivo de schema
    
    Returns:
        String contendo o schema
    """
    try:
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Schema file not found: {file_path}")
        
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        logger.info(f"Schema carregado de: {file_path}")
        return content
        
    except Exception as e:
        logger.error(f"Erro ao carregar schema: {e}")
        raise


def load_schema_and_content_from_file(file_path: str) -> tuple[str, str]:
    """
    Carrega schema E conteúdo do banco de um arquivo JSON.
    O arquivo JSON deve ter a seguinte estrutura:
    {
      "schema": "CREATE TABLE ...",
      "records": "INSERT INTO ... VALUES (...); INSERT INTO ... VALUES (...);"
    }
    
    Args:
        file_path: Caminho para o arquivo JSON com schema e conteúdo
    
    Returns:
        Tuple (schema, db_content) - ambas como strings
    """
    try:
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Schema file not found: {file_path}")
        
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extrair schema e records do JSON
        schema = data.get("schema", "").strip()
        db_content = data.get("records", "").strip()
        
        logger.info(f"Schema e conteúdo carregados de: {file_path}")
        return schema, db_content
        
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar JSON: {e}")
        raise ValueError(f"Invalid JSON format in {file_path}: {e}")
    except Exception as e:
        logger.error(f"Erro ao carregar schema e conteúdo: {e}")
        raise