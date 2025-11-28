"""Carrega dataset Spider e extrai schema + conteúdo do banco"""
import pandas as pd
import json
import sqlite3
import os
from loguru import logger
from typing import Dict, List

# Caminho para os arquivos do Spider
SPIDER_DIR = "spider_data/spider_data"
TABLES_JSON = os.path.join(SPIDER_DIR, "tables.json")
DATABASE_DIR = os.path.join(SPIDER_DIR, "database")

# Cache para schemas
_TABLES_CACHE = None

def load_tables_json():
    """Carrega tables.json com schemas de todas as databases"""
    global _TABLES_CACHE
    if _TABLES_CACHE is None:
        logger.info(f"Carregando schemas de {TABLES_JSON}...")
        with open(TABLES_JSON, 'r', encoding='utf-8') as f:
            _TABLES_CACHE = json.load(f)
        logger.info(f"{len(_TABLES_CACHE)} databases carregadas")
    return _TABLES_CACHE

def load_spider_dataset(split="dev"):
    """Carrega dataset Spider"""
    logger.info(f"Carregando Spider ({split})...")
    df = pd.read_json(f"hf://datasets/aherntech/spider-realistic/{split}.json")
    logger.info(f"{len(df)} exemplos carregados")
    return df

def extract_database_schema(db_id: str) -> str:
    """
    Extrai schema do banco de dados do tables.json.
    Formato: CREATE TABLE statements
    """
    tables_data = load_tables_json()
    
    # Encontra o schema para o db_id
    db_schema = None
    for db in tables_data:
        if db["db_id"] == db_id:
            db_schema = db
            break
    
    if not db_schema:
        return f"-- Schema not found for database: {db_id}"
    
    schema_parts = []
    table_names = db_schema.get("table_names_original", [])
    column_names = db_schema.get("column_names_original", [])
    column_types = db_schema.get("column_types", [])
    primary_keys = db_schema.get("primary_keys", [])
    
    # Organiza colunas por tabela
    tables = {}
    for col_idx, (table_idx, col_name) in enumerate(column_names):
        if table_idx == -1:  # Coluna especial "*"
            continue
        if table_idx not in tables:
            tables[table_idx] = []
        col_type = column_types[col_idx] if col_idx < len(column_types) else "text"
        is_pk = col_idx in primary_keys
        tables[table_idx].append((col_name, col_type, is_pk))
    
    # Gera CREATE TABLE statements
    for table_idx, table_name in enumerate(table_names):
        if table_idx not in tables:
            continue
        
        cols_def = []
        for col_name, col_type, is_pk in tables[table_idx]:
            pk_str = " PRIMARY KEY" if is_pk else ""
            cols_def.append(f"  {col_name} {col_type}{pk_str}")
        
        create_stmt = f"CREATE TABLE {table_name} (\n" + ",\n".join(cols_def) + "\n)"
        schema_parts.append(create_stmt)
    
    return "\n\n".join(schema_parts)

def extract_database_content(db_id: str, k: int = 5) -> str:
    """
    Extrai K primeiros registros de cada tabela do banco SQLite.
    
    Args:
        db_id: ID do banco de dados
        k: Número de registros a extrair por tabela
    
    Returns:
        String formatada com K registros de cada tabela
    """
    # Caminho para o arquivo SQLite
    db_path = os.path.join(DATABASE_DIR, db_id, f"{db_id}.sqlite")
    
    if not os.path.exists(db_path):
        return f"-- Database file not found: {db_path}"
    
    content_parts = []
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Lista todas as tabelas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        for (table_name,) in tables:
            try:
                # Extrai K primeiros registros
                cursor.execute(f"SELECT * FROM {table_name} LIMIT {k}")
                rows = cursor.fetchall()
                
                if rows:
                    # Pega nomes das colunas
                    col_names = [desc[0] for desc in cursor.description]
                    
                    content_parts.append(f"Table: {table_name}")
                    content_parts.append(f"Columns: {', '.join(col_names)}")
                    
                    for idx, row in enumerate(rows, 1):
                        row_str = ' | '.join(str(val) if val is not None else 'NULL' for val in row)
                        content_parts.append(f"  Row {idx}: {row_str}")
                    
                    content_parts.append("")  # Linha em branco
                    
            except sqlite3.Error as e:
                content_parts.append(f"-- Error reading table {table_name}: {e}")
        
        conn.close()
        
    except sqlite3.Error as e:
        return f"-- Error connecting to database: {e}"
    
    return "\n".join(content_parts)

def prepare_examples(df, limit=None):
    """
    Prepara exemplos com schema e conteúdo do banco.
    
    Carrega schemas de tables.json e conteúdo de arquivos SQLite.
    """
    if limit:
        df = df.head(limit)
    
    examples = []
    for idx, row in df.iterrows():
        db_id = row.get("db_id", "")
        
        logger.info(f"Carregando schema e conteúdo para {db_id}...")
        
        # Extrai schema do tables.json
        schema_str = extract_database_schema(db_id)
        
        # Extrai K=5 registros do SQLite
        content_str = extract_database_content(db_id, k=5)
        
        examples.append({
            "id": idx,
            "question": row.get("question", ""),
            "query": row.get("query", ""),
            "db_id": db_id,
            "db_schema": schema_str,
            "db_content": content_str
        })
    
    return examples

if __name__ == "__main__":
    df = load_spider_dataset("dev")
    print("Colunas disponíveis:", df.columns.tolist())
    print("\nPrimeiro exemplo:")
    example = prepare_examples(df, limit=1)[0]
    print(f"\nDB ID: {example['db_id']}")
    print(f"Question: {example['question']}")
    print(f"\nSchema:\n{example['db_schema'][:300]}...")
    print(f"\nContent:\n{example['db_content'][:300]}...")