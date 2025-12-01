"""Métricas de Avaliação: EX e EM conforme DART-SQL"""
from loguru import logger
import re
from typing import Set, Tuple
from .execution_accuracy import compute_execution_accuracy

def normalize_sql(sql: str) -> str:
    """Normaliza SQL para comparação"""
    sql = " ".join(sql.split())
    sql = sql.lower()
    sql = sql.rstrip(";")
    return sql.strip()

def extract_sql_clauses(sql: str) -> Set[str]:
    """
    Extrai cláusulas SQL para Exact-Set-Match (EM).
    Remove valores literais, mantendo apenas estrutura.
    
    Exemplo:
    SELECT name FROM users WHERE age > 20
    -> {select_name, from_users, where_age}
    """
    sql_normalized = normalize_sql(sql)
    
    # Remove valores literais (strings e números)
    sql_structure = re.sub(r"'[^']*'", "VALUE", sql_normalized)
    sql_structure = re.sub(r"\b\d+\b", "VALUE", sql_structure)
    
    clauses = set()
    
    # Extrai SELECT columns
    select_match = re.search(r"select\s+(.*?)\s+from", sql_structure)
    if select_match:
        cols = [c.strip() for c in select_match.group(1).split(",")]
        for col in cols:
            clauses.add(f"select_{col}")
    
    # Extrai FROM tables
    from_match = re.search(r"from\s+(\w+)", sql_structure)
    if from_match:
        clauses.add(f"from_{from_match.group(1)}")
    
    # Extrai WHERE conditions
    where_match = re.search(r"where\s+(.*?)(?:group|order|limit|$)", sql_structure)
    if where_match:
        conditions = where_match.group(1).strip()
        clauses.add(f"where_{conditions}")
    
    # Extrai GROUP BY
    group_match = re.search(r"group\s+by\s+(.*?)(?:having|order|limit|$)", sql_structure)
    if group_match:
        clauses.add(f"groupby_{group_match.group(1).strip()}")
    
    # Extrai ORDER BY
    order_match = re.search(r"order\s+by\s+(.*?)(?:limit|$)", sql_structure)
    if order_match:
        clauses.add(f"orderby_{order_match.group(1).strip()}")
    
    return clauses

def exact_set_match(predicted: str, ground_truth: str) -> bool:
    """
    Exact-Set-Match (EM): Verifica se as cláusulas são idênticas
    após remover valores literais.
    """
    pred_clauses = extract_sql_clauses(predicted)
    gt_clauses = extract_sql_clauses(ground_truth)
    
    return pred_clauses == gt_clauses

def exact_match(predicted: str, ground_truth: str) -> bool:
    """Verifica se SQLs são exatamente idênticos (string match)"""
    return normalize_sql(predicted) == normalize_sql(ground_truth)

def calculate_exact_set_match_accuracy(results: list) -> float:
    """
    Calcula Exact-Set-Match Accuracy (EM).
    Métrica mais flexível que exact match string.
    """
    if not results:
        return 0.0
    
    matches = sum(
        1 for r in results 
        if exact_set_match(r.get("predicted_sql", ""), r.get("ground_truth_sql", ""))
    )
    
    accuracy = matches / len(results)
    logger.info(f"Exact-Set-Match (EM): {matches}/{len(results)} = {accuracy:.2%}")
    return accuracy

def execution_match(pred_sql: str, gold_sql: str, db_path: str) -> bool:
    return compute_execution_accuracy(db_path, pred_sql, gold_sql) == 1

def calculate_exact_match_accuracy(results: list) -> float:
    """
    Calcula String Exact Match Accuracy.
    Métrica mais restritiva.
    """
    if not results:
        return 0.0
    
    matches = sum(
        1 for r in results 
        if exact_match(r.get("predicted_sql", ""), r.get("ground_truth_sql", ""))
    )
    
    accuracy = matches / len(results)
    logger.info(f"String Exact Match: {matches}/{len(results)} = {accuracy:.2%}")
    return accuracy

def token_overlap(predicted: str, ground_truth: str) -> float:
    """Sobreposição de tokens"""
    pred_tokens = set(normalize_sql(predicted).split())
    gt_tokens = set(normalize_sql(ground_truth).split())
    
    if not gt_tokens:
        return 0.0
    
    intersection = pred_tokens & gt_tokens
    return len(intersection) / len(gt_tokens)

def evaluate_results(results: list) -> dict:
    """
    Avalia resultados com EM, Exact Match, Token Overlap e Execution Accuracy (EX).
    """
    exact_set_match_acc = calculate_exact_set_match_accuracy(results)
    string_exact_match_acc = calculate_exact_match_accuracy(results)

    # Token overlap
    overlaps = [
        token_overlap(r.get("predicted_sql", ""), r.get("ground_truth_sql", ""))
        for r in results
    ]
    avg_overlap = sum(overlaps) / len(overlaps) if overlaps else 0.0

    # Execution Accuracy (EX)
    ex_scores = []
    for r in results:
        pred = r.get("predicted_sql", "")
        gold = r.get("ground_truth_sql", "")
        db = r.get("db_path")

        if not db:
            raise ValueError("Faltando db_path em um dos resultados para calcular EX.")

        ex_scores.append(1 if execution_match(pred, gold, db) else 0)

    ex_accuracy = sum(ex_scores) / len(ex_scores) if ex_scores else 0.0

    return {
        "exact_set_match_accuracy": exact_set_match_acc,
        "string_exact_match_accuracy": string_exact_match_acc,
        "execution_accuracy": ex_accuracy,          # <--- AQUI
        "average_token_overlap": avg_overlap,
        "total_examples": len(results)
    }

def compare_methods(rewriting_results: list, zero_shot_results: list) -> dict:
    """
    Compara Baseline 1 (Zero-Shot) vs Baseline 2 (RW-Enhanced).
    
    Retorna melhoria do RW sobre Zero-Shot.
    """
    rewriting_metrics = evaluate_results(rewriting_results)
    zero_shot_metrics = evaluate_results(zero_shot_results)
    
    return {
        "baseline_1_zero_shot": zero_shot_metrics,
        "baseline_2_rw_enhanced": rewriting_metrics,
        "improvement": {
            "exact_set_match": rewriting_metrics["exact_set_match_accuracy"] - zero_shot_metrics["exact_set_match_accuracy"],
            "string_exact_match": rewriting_metrics["string_exact_match_accuracy"] - zero_shot_metrics["string_exact_match_accuracy"],
            "execution_accuracy": rewriting_metrics["execution_accuracy"] - zero_shot_metrics["execution_accuracy"],   # <--- AQUI
            "token_overlap": rewriting_metrics["average_token_overlap"] - zero_shot_metrics["average_token_overlap"]
        }
    }