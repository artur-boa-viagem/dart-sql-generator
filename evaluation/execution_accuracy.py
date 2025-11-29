import sqlite3
from typing import Any, List, Tuple


# Cache opcional para evitar reabrir tantos bancos
_CONNECTION_CACHE = {}


def get_connection(db_path: str) -> sqlite3.Connection:
    """Reaproveita conexões SQLite para performance."""
    if db_path in _CONNECTION_CACHE:
        return _CONNECTION_CACHE[db_path]

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Melhor para acesso por nome
    _CONNECTION_CACHE[db_path] = conn
    return conn


def execute_query(db_path: str, sql: str) -> List[Tuple[Any]]:
    """
    Executa uma query SQL e retorna o resultado como lista de tuplas.
    Resultados são ordenados para comparação independente da ordem.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(sql)
        rows = cursor.fetchall()

        # CORREÇÃO AQUI: extrair valores, não as chaves
        rows = [tuple(row[k] for k in row.keys()) for row in rows]

        rows.sort()
        return rows

    except Exception:
        # Se a query der erro → EX=0
        return None


def compare_results(pred_rows, gold_rows) -> bool:
    """Compara dois conjuntos de resultados SQL."""
    if pred_rows is None or gold_rows is None:
        return False

    if len(pred_rows) != len(gold_rows):
        return False

    return pred_rows == gold_rows


def compute_execution_accuracy(db_path: str, predicted_sql: str, gold_sql: str) -> int:
    """
    Retorna 1 se predicted_sql == gold_sql no sentido de execução.
    """
    pred_out = execute_query(db_path, predicted_sql)
    gold_out = execute_query(db_path, gold_sql)

    return 1 if compare_results(pred_out, gold_out) else 0
