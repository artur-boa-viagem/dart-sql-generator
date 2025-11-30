# Projeto TAES - DART-SQL Question Rewriting

Implementação do módulo de **Question Rewriting** do paper DART-SQL para avaliar seu impacto isolado na geração de SQL.

## 📋 Metodologia

Este projeto implementa a metodologia de ablação do DART-SQL, comparando:

### **Baseline 1: Zero-Shot Puro**
- **Input**: Questão Original + Schema do Banco
- **Modelo**: GPT-5 nano
- **Prompt**: Instruções Zero-Shot padrão (similar ao DAIL-SQL sem few-shot)

### **Baseline 2: RW-Enhanced Zero-Shot**
- **Input**: Questão Reescrita + Schema do Banco
- **Processo**:
  1. **Question Rewriting**: Reescreve a questão usando K=5 registros de cada tabela
  2. **SQL Generation**: Gera SQL da questão reescrita usando o mesmo prompt zero-shot
- **Modelo**: GPT-5 nano

## 🎯 Objetivos

Quantificar o impacto isolado do **Question Rewriting** na acurácia Text-to-SQL, sem os módulos de Context-Aware Prompt (CAP) e Refinement do DART-SQL completo.

## 📊 Métricas de Avaliação

- **EM (Exact-Set-Match Accuracy)**: Métrica principal - compara cláusulas SQL após remover valores literais
- **String Exact Match**: Comparação exata de strings normalizadas
- **Token Overlap**: Métrica auxiliar de sobreposição de tokens

## 🏗️ Estrutura do Projeto

```
projeto_taes/
├── data/
│   ├── spider_loader.py        # Carrega Spider + extrai schema e K=5 registros
│   └── __init__.py
├── experiments/
│   ├── question_rewriting.py   # Módulo RW com prompt DART-SQL
│   ├── zero_shot_baseline.py   # Baseline puro
│   ├── run_experiment.py       # Script principal
│   └── __init__.py
├── evaluation/
│   ├── metrics.py              # Métricas EM, EX, Token Overlap
│   └── __init__.py
├── results/                    # Resultados JSON dos experimentos
├── core/
│   └── config.py              # Configurações (API key)
└── README.md
```

## 🚀 Como Executar

### 1. Configurar ambiente

```bash
# Criar e ativar venv
python -m venv venv
source venv/Scripts/activate  # Windows Git Bash
# venv\Scripts\activate  # Windows CMD

# Instalar dependências
pip install pandas openai loguru datasets pydantic-settings python-dotenv
```

### 2. Configurar API Key

Criar arquivo `.env` na raiz:
```env
PROJETO_TAES_OPENAI_API_KEY=sk-proj-sua_chave_aqui
```

### 3. Executar experimento

```bash
python -m experiments.run_experiment
```

### 4. Ajustar número de exemplos

Editar `experiments/run_experiment.py`:
```python
NUM_EXAMPLES = 10  # Para teste
# NUM_EXAMPLES = 50  # Para experimento maior
```

## 📈 Resultados Esperados

Baseado no paper DART-SQL (ablação no Spider-dev):

- **Zero-Shot Baseline**: ~76.2% EX
- **Com Question Rewriting**: ~79.9% EX
- **Melhoria esperada**: +3-5% em datasets como Spider-Realistic

## 🔧 Detalhes de Implementação

### Question Rewriting Prompt

Segue a estrutura DART-SQL:

1. **Instrução**: "Formule consultas de linguagem natural para dados do banco de dados..."
2. **Especificações**:
   - Reescrita de ambiguidade (alinhar com formato do banco)
   - Preservação de informações-chave
   - Evitar modificações desnecessárias
3. **Input**: Questão Original + K=5 registros de cada tabela

### Zero-Shot SQL Generation Prompt

- System: "You are a SQL expert..."
- User: Database Schema + Question
- Temperature: 0.0 (determinístico)

## 📝 Arquivos de Resultado

Salvos em `results/experiment_dart_sql_TIMESTAMP.json`:

```json
{
  "experiment_config": {
    "model": "gpt-5-nano",
    "dataset": "spider-realistic",
    "methodology": "DART-SQL Question Rewriting"
  },
  "baseline_1_zero_shot_results": [...],
  "baseline_2_rw_enhanced_results": [...],
  "comparison": {
    "improvement": {
      "exact_set_match": 0.05,
      "string_exact_match": 0.03
    }
  }
}
```

## 🎓 Referências

- **DART-SQL Paper**: "Text-to-SQL Parsing with Rewriting and Refinement"
- **Dataset**: Spider-Realistic (aherntech/spider-realistic)
- **Modelo**: GPT-5 nano

## ⚠️ Limitações Atuais

- **EX (Execution Accuracy)** requer execução real nas databases SQLite do Spider (não implementado)
- Usamos **EM (Exact-Set-Match)** como métrica principal proxy
- Schema extraction depende da estrutura do dataset aherntech/spider-realistic

## 🔜 Próximos Passos

1. Implementar EX real executando queries nas databases SQLite
2. Testar com dataset Spider original (não realistic)
3. Experimentar variações do prompt de rewriting
4. Análise qualitativa de casos onde RW ajuda/prejudica