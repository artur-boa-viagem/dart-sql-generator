"""Zero-Shot Baseline (Linha de Base 1)

Gera SQL diretamente da questão original + schema, sem rewriting.
Usa GPT-5 nano.
"""
from loguru import logger
from openai import OpenAI
from core.config import settings

client = OpenAI(api_key=settings.PROJETO_TAES_OPENAI_API_KEY)

# Modelo usado no experimento
MODEL = "gpt-5-nano"

def generate_sql_zero_shot(question: str, db_schema: str) -> dict:
    """
    Baseline Zero-Shot: Gera SQL da questão original + schema.
    
    Estrutura do prompt:
    - Questão Original
    - Schema do banco
    - Instruções Zero-Shot
    
    Args:
        question: Questão original do usuário
        db_schema: Schema do banco (CREATE TABLE statements)
    
    Returns:
        Dict com questão original e SQL gerado
    """
    logger.info(f"Zero-Shot para: {question}")
    
    # Prompt Zero-Shot padrão (similar ao usado em DART-SQL)
    system_prompt = """You are a SQL expert. Generate a SQL query based on the question and database schema provided.

Rules:
- Return ONLY the SQL query, no explanations
- Use proper SQL syntax
- Follow the schema exactly as provided"""

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
            max_completion_tokens=500
        )
        
        sql = response.choices[0].message.content.strip()
        
        # Remove markdown se presente
        if sql.startswith("```"):
            lines = sql.split("\n")
            sql = "\n".join(lines[1:-1]) if len(lines) > 2 else sql
            sql = sql.replace("```sql", "").replace("```", "").strip()
        
        logger.info(f"SQL gerado: {sql[:100]}...")
        
        return {
            "original_question": question,
            "generated_sql": sql
        }
        
    except Exception as e:
        logger.error(f"Erro ao gerar SQL zero-shot: {e}")
        raise