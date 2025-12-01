"""Script principal do experimento DART-SQL

Compara:
- Baseline 1: Zero-Shot (Questão Original + Schema)
- Baseline 2: RW-Enhanced (Questão Reescrita + Schema)

Métricas:
- EM (Exact-Set-Match Accuracy)
- String Exact Match
- Token Overlap
"""
import json
import os
from datetime import datetime
from loguru import logger

from data.spider_loader import load_spider_dataset, prepare_examples
from experiments.question_rewriting import generate_sql_with_rewriting
from experiments.zero_shot_baseline import generate_sql_zero_shot
from evaluation.metrics import compare_methods

def run_experiment(num_examples=10):
    """Executa experimento completo conforme metodologia DART-SQL"""
    
    logger.info("="*80)
    logger.info("EXPERIMENTO DART-SQL: Question Rewriting vs Zero-Shot")
    logger.info("="*80)
    logger.info(f"Modelo: GPT-5 nano")
    logger.info(f"Dataset: Spider-Realistic")
    logger.info(f"Exemplos: {num_examples}")
    logger.info("="*80)
    
    # 1. Carregar dados
    logger.info("\n[1/4] Carregando dataset Spider-Realistic...")
    df = load_spider_dataset("dev")
    examples = prepare_examples(df, limit=num_examples)
    logger.info(f"Carregados {len(examples)} exemplos com schema e conteúdo")
    
    # 2. BASELINE 1: Zero-Shot (Questão Original + Schema)
    logger.info(f"\n[2/4] Executando BASELINE 1 - Zero-Shot...")
    logger.info("Estrutura: Questão Original + Schema + Instruções Zero-Shot")
    zero_shot_results = []
    
    for i, ex in enumerate(examples, 1):
        logger.info(f"\n  [{i}/{num_examples}] {ex['question']}")
        try:
            result = generate_sql_zero_shot(
                question=ex["question"],
                db_schema=ex["db_schema"]
            )
            zero_shot_results.append({
                "example_id": ex["id"],
                "db_id": ex["db_id"],
                "db_path": ex["db_path"],
                "original_question": ex["question"],
                "predicted_sql": result["generated_sql"],
                "ground_truth_sql": ex["query"]
            })
        except Exception as e:
            logger.error(f"Erro: {e}")
            zero_shot_results.append({
                "example_id": ex["id"],
                "db_id": ex["db_id"],
                "db_path": ex["db_path"],
                "original_question": ex["question"],
                "predicted_sql": "",
                "ground_truth_sql": ex["query"],
                "error": str(e)
            })
    
    # 3. BASELINE 2: RW-Enhanced (Questão Reescrita + Schema)
    logger.info(f"\n[3/4] Executando BASELINE 2 - RW-Enhanced Zero-Shot...")
    logger.info("Estrutura: Questão Reescrita + Schema + Instruções Zero-Shot")
    rewriting_results = []
    
    for i, ex in enumerate(examples, 1):
        logger.info(f"\n  [{i}/{num_examples}] {ex['question']}")
        try:
            result = generate_sql_with_rewriting(
                question=ex["question"],
                db_schema=ex["db_schema"],
                db_content=ex["db_content"]
            )
            rewriting_results.append({
                "example_id": ex["id"],
                "db_id": ex["db_id"],
                "db_path": ex["db_path"],
                "original_question": ex["question"],
                "rewritten_question": result["rewritten_question"],
                "predicted_sql": result["generated_sql"],
                "ground_truth_sql": ex["query"]
            })
        except Exception as e:
            logger.error(f"Erro: {e}")
            rewriting_results.append({
                "example_id": ex["id"],
                "db_id": ex["db_id"],
                "db_path": ex["db_path"],
                "original_question": ex["question"],
                "rewritten_question": "",
                "predicted_sql": "",
                "ground_truth_sql": ex["query"],
                "error": str(e)
            })
    
    # 4. Comparar resultados
    logger.info("\n[4/4] Comparando resultados...")
    comparison = compare_methods(rewriting_results, zero_shot_results)
    
    # 5. Exibir resumo
    logger.info("\n" + "="*80)
    logger.info("RESUMO DOS RESULTADOS")
    logger.info("="*80)
    
    logger.info(f"\nBASELINE 1 - Zero-Shot (Questão Original + Schema):")
    logger.info(f"  EM (Exact-Set-Match): {comparison['baseline_1_zero_shot']['exact_set_match_accuracy']:.2%}")
    logger.info(f"  Execution Accuracy: {comparison['baseline_1_zero_shot']['execution_accuracy']:.2%}")
    logger.info(f"  String Exact Match: {comparison['baseline_1_zero_shot']['string_exact_match_accuracy']:.2%}")
    logger.info(f"  Token Overlap: {comparison['baseline_1_zero_shot']['average_token_overlap']:.2%}")
    
    logger.info(f"\nBASELINE 2 - RW-Enhanced (Questão Reescrita + Schema):")
    logger.info(f"  EM (Exact-Set-Match): {comparison['baseline_2_rw_enhanced']['exact_set_match_accuracy']:.2%}")
    logger.info(f"  Execution Accuracy: {comparison['baseline_2_rw_enhanced']['execution_accuracy']:.2%}")
    logger.info(f"  String Exact Match: {comparison['baseline_2_rw_enhanced']['string_exact_match_accuracy']:.2%}")
    logger.info(f"  Token Overlap: {comparison['baseline_2_rw_enhanced']['average_token_overlap']:.2%}")
    
    logger.info(f"\nMELHORIA (RW-Enhanced - Zero-Shot):")
    logger.info(f"  EM: {comparison['improvement']['exact_set_match']:+.2%}")
    logger.info(f"  Execution Accuracy: {comparison['improvement']['execution_accuracy']:+.2%}")
    logger.info(f"  String Match: {comparison['improvement']['string_exact_match']:+.2%}")
    logger.info(f"  Token Overlap: {comparison['improvement']['token_overlap']:+.2%}")
    logger.info("="*80)
    
    # 6. Salvar resultados
    os.makedirs("results", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = f"results/experiment_dart_sql_{timestamp}.json"
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump({
            "experiment_config": {
                "timestamp": timestamp,
                "model": "gpt-5-nano",
                "dataset": "spider-realistic",
                "num_examples": num_examples,
                "methodology": "DART-SQL Question Rewriting"
            },
            "baseline_1_zero_shot_results": zero_shot_results,
            "baseline_2_rw_enhanced_results": rewriting_results,
            "comparison": comparison
        }, f, indent=2, ensure_ascii=False)
    
    logger.info(f"\nResultados salvos em: {filepath}")
    return comparison

if __name__ == "__main__":
    # Configurar número de exemplos para teste
    NUM_EXAMPLES = 10  # Comece com 10 para validar, depois aumente para 50-100
    
    try:
        results = run_experiment(num_examples=NUM_EXAMPLES)
        logger.info("\n✓ Experimento concluído com sucesso!")
    except Exception as e:
        logger.error(f"\n✗ Erro ao executar experimento: {e}")
        raise