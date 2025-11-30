# Projeto TAES - DART-SQL Question Rewriting & SQL Generator

Implementação do módulo de **Question Rewriting** do paper DART-SQL para avaliar seu impacto isolado na geração de SQL, com uma aplicação web interativa para geração de SQL a partir de linguagem natural.

---

## 📋 Índice

1. [Visão Geral do Projeto](#visão-geral-do-projeto)
2. [Arquitetura](#arquitetura)
3. [Instalação e Setup](#instalação-e-setup)
4. [Como Executar](#como-executar)
5. [API Endpoints](#api-endpoints)
6. [Uso da Interface Web](#uso-da-interface-web)
7. [Metodologia Experimental](#metodologia-experimental)
8. [Estrutura do Projeto](#estrutura-do-projeto)
9. [Troubleshooting](#troubleshooting)

---

## 📖 Visão Geral do Projeto

Este projeto implementa uma solução completa para geração de SQL a partir de questões em linguagem natural, combinando:

- **Backend FastAPI**: API REST para processamento de questões e geração de SQL
- **Frontend Web**: Interface interativa para testar a geração de SQL
- **Módulo DART-SQL**: Question Rewriting para melhorar a qualidade das consultas SQL geradas

### Componentes Principais

1. **SQL Generator**: Transforma questões em linguagem natural em queries SQL usando OpenAI
2. **Schema Handler**: Suporta múltiplos formatos de schema (SQL, JSON, YAML)
3. **Question Rewriting**: Reescreve questões ambíguas para melhor compreensão do modelo
4. **Web Interface**: Frontend responsivo com tema escuro moderno

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────┐
│        Frontend (http://localhost:8080)  │
│  - Dark Theme UI                         │
│  - Two Input Methods                     │
│  - Real-time Status                      │
└────────────────┬──────────────────────────┘
                 │ HTTP
                 ▼
┌─────────────────────────────────────────┐
│   Backend API (http://localhost:8000)    │
│  - CORS Enabled                          │
│  - FastAPI with OpenAI Integration       │
└────────────────┬──────────────────────────┘
                 │
         ┌───────┴────────┐
         ▼                ▼
    ┌─────────┐    ┌─────────────┐
    │ OpenAI  │    │ SQL Parser   │
    │ API     │    │ (Schema)     │
    └─────────┘    └─────────────┘
```

---

## 🚀 Instalação e Setup

### Pré-requisitos

- Python 3.8 ou superior
- pip (gerenciador de pacotes Python)
- Uma chave de API válida da OpenAI

### Passo 1: Clonar o Repositório

### Passo 2: Criar e Ativar Ambiente Virtual

#### Windows (PowerShell):
```powershell
python -m venv venv
venv\Scripts\Activate.ps1
```

#### Windows (CMD):
```cmd
python -m venv venv
venv\Scripts\activate.bat
```

#### Linux/macOS:
```bash
python -m venv venv
source venv/bin/activate
```

### Passo 3: Instalar Dependências

```bash
pip install -r requirements.txt
```

### Passo 4: Configurar Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto:

```env
PROJETO_TAES_OPENAI_API_KEY=sk-proj-sua_chave_aqui
```

---

## 🎯 Como Executar

### Opção 1: Modo Completo (Frontend + Backend)

**Terminal 1 - Backend:**
```powershell
python -m uvicorn endpoints.server:app --host 0.0.0.0 --port 8000 --reload
```

**Terminal 2 - Frontend:**
```powershell
cd frontend
python -m http.server 8080
```

**Acessar a aplicação:**
- Frontend: `http://localhost:8080`
- Backend Swagger: `http://localhost:8000/docs`

### Opção 2: Apenas Backend (API)

```powershell
python -m uvicorn endpoints.server:app --host 0.0.0.0 --port 8000 --reload
```

Acesse a documentação interativa em: `http://localhost:8000/docs`

### Opção 3: Experimento DART-SQL

```powershell
python -m experiments.run_experiment
```

---

## 🔌 API Endpoints

### 1. Health Check
**GET** `/health`

Verifica se o servidor está operacional.

**Response:**
```json
{"status": "healthy"}
```

---

### 2. Generate SQL with Inline Schema
**POST** `/api/v1/generate-sql`

Gera SQL a partir de uma questão com schema inline (CREATE TABLE statements).

**Request:**
```json
{
  "prompt": "What is the maintenance frequency for equipment type 'pump'?",
  "schema": "CREATE TABLE equipment_maintenance (equipment_type VARCHAR(255), maintenance_frequency INT);"
}
```

**Response:**
```json
{
  "SQL": "SELECT maintenance_frequency FROM equipment_maintenance WHERE equipment_type = 'pump';"
}
```

---

### 3. Generate SQL with File Schema
**POST** `/api/v1/generate-sql-with-file`

Gera SQL a partir de uma questão com schema de um arquivo (JSON, YAML ou SQL).

**Request:**
```json
{
  "prompt": "What is the maintenance frequency for equipment type 'pump'?",
  "schema_file_path": "/path/to/schema.json"
}
```

**Response:**
```json
{
  "SQL": "SELECT maintenance_frequency FROM equipment_maintenance WHERE equipment_type = 'pump';"
}
```

---

## 💻 Uso da Interface Web

### Recursos

✅ **Connection Status** - Indicador de status de conexão com o backend (verde/vermelho)  
✅ **Dois Métodos de Input** - Alterne entre schema inline e arquivo  
✅ **Feedback em Tempo Real** - Spinner de carregamento durante geração  
✅ **Botão Copy** - Copie o SQL gerado com um clique  
✅ **Tema Escuro** - Interface moderna com tipografia limpa  
✅ **Tratamento de Erros** - Mensagens de erro claras para debug  
✅ **Atalhos de Teclado** - Ctrl+Enter para gerar SQL  

### Tutorial Rápido

1. **Abra o navegador** e acesse `http://localhost:8080`
2. **Aguarde** o indicador de conexão ficar verde
3. **Digite a questão** no campo "Natural Language Question"
   - Exemplo: `"What is the maintenance frequency for equipment type 'pump'?"`
4. **(Opcional) Adicione o schema**:
   - Inline: Cole um CREATE TABLE statement
   - File: Digite o caminho do arquivo (JSON, YAML, ou SQL)
5. **Clique em "Generate SQL"** ou pressione **Ctrl+Enter**
6. **Visualize o resultado** na caixa de output
7. **Copie** clicando no botão "Copy"

### Formatos de Schema Suportados

#### SQL (Inline)
```sql
CREATE TABLE equipment_maintenance (
  equipment_type VARCHAR(255),
  maintenance_frequency INT,
  PRIMARY KEY (equipment_type)
);
```

#### JSON (Arquivo)
```json
{
  "equipment_maintenance": {
    "columns": {
      "equipment_type": "VARCHAR(255)",
      "maintenance_frequency": "INT"
    },
    "primary_key": "equipment_type"
  }
}
```

#### YAML (Arquivo)
```yaml
equipment_maintenance:
  columns:
    equipment_type: VARCHAR(255)
    maintenance_frequency: INT
  primary_key: equipment_type
```

---

## 🧪 Metodologia Experimental (DART-SQL)

### Baseline 1: Zero-Shot Puro

- **Input**: Questão Original + Schema do Banco
- **Modelo**: GPT-4
- **Prompt**: Instruções Zero-Shot padrão
- **Saída**: SQL Query

### Baseline 2: RW-Enhanced Zero-Shot

- **Input**: Questão Reescrita + Schema do Banco
- **Processo**:
  1. **Question Rewriting**: Reescreve a questão usando K=5 registros de cada tabela
  2. **SQL Generation**: Gera SQL da questão reescrita usando o mesmo prompt zero-shot
- **Modelo**: GPT-4

### Métricas de Avaliação

- **EM (Exact-Set-Match Accuracy)**: Métrica principal - compara cláusulas SQL após remover valores literais
- **String Exact Match**: Comparação exata de strings normalizadas
- **Token Overlap**: Métrica auxiliar de sobreposição de tokens

### Executar Experimento

```powershell
python -m experiments.run_experiment
```

Os resultados serão salvos em `results/experiment_dart_sql_TIMESTAMP.json`

---

## 📁 Estrutura do Projeto

```
projeto_taes/
├── core/
│   ├── __init__.py
│   ├── config.py              # Configurações e variáveis de ambiente
│   └── database.py            # Utilitários para parsing de schema
├── endpoints/
│   ├── __init__.py
│   ├── server.py              # Aplicação FastAPI principal
│   └── sql_generator.py       # Rotas da API
├── frontend/
│   └── index.html             # Interface web
├── experiments/
│   ├── __init__.py
│   ├── question_rewriting.py  # Módulo RW com prompt DART-SQL
│   ├── zero_shot_baseline.py  # Baseline puro
│   └── run_experiment.py      # Script principal
├── data/
│   ├── __init__.py
│   └── spider_loader.py       # Carrega Spider dataset
├── evaluation/
│   ├── __init__.py
│   └── metrics.py             # Métricas EM, EX, Token Overlap
├── results/                   # Resultados JSON dos experimentos
├── sql_generator.py           # Módulo principal de geração de SQL
├── requirements.txt           # Dependências Python
├── .env                       # Variáveis de ambiente (não versionar)
├── README.md                  # Este arquivo
└── SCHEMA_INTEGRATION.md      # Documentação de schema

```

---

## 🔧 Detalhes de Implementação

### Question Rewriting Prompt

Segue a estrutura DART-SQL:

```
Você é um assistente especializado em reescrever perguntas de usuários
para serem mais específicas e alinhadas com a estrutura do banco de dados.

Dada uma pergunta em linguagem natural e alguns exemplos de dados,
reescreva a pergunta para ser inequívoca e diretamente baseada no
conteúdo do banco de dados.
```

### Zero-Shot SQL Generation Prompt

```
Você é um gerador de SQL. Use o schema e a pergunta reescrita
para produzir uma única consulta SQL válida, sem explicações.
```

### Pipeline de Processamento

1. **Input**: Questão do usuário + Schema (opcional)
2. **Question Rewriting**: Melhora a questão com contexto do schema
3. **SQL Generation**: Gera SQL a partir da questão melhorada
4. **Output**: Query SQL pronta para execução

---

## 🐛 Troubleshooting

### Backend Connection Failed

**Problema**: Frontend mostra "Backend disconnected"

**Solução**:
- Verifique se o backend está rodando na porta 8000
- Confirm CORS está habilitado em `endpoints/server.py`
- Verifique o console do navegador para mensagens de erro detalhadas

```powershell
python -m uvicorn endpoints.server:app --host 0.0.0.0 --port 8000 --reload
```

---

### Module Not Found Errors

**Problema**: `ModuleNotFoundError: No module named 'core.database'`

**Solução**:
- Verifique se o ambiente virtual está ativado
- Reinstale as dependências:
  ```bash
  pip install -r requirements.txt
  ```
- Verifique os imports nos arquivos Python

---

### Schema Parsing Issues

**Problema**: Schema não está sendo reconhecido

**Solução**:
- Use sintaxe SQL válida para CREATE TABLE
- Para schemas em arquivo, verifique se o arquivo existe no caminho especificado
- Formatos suportados: JSON, YAML, SQL
- Exemplo de SQL válido:
  ```sql
  CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));
  ```

---

### API Key Issues

**Problema**: `Error: Invalid API key`

**Solução**:
- Verifique se a variável `PROJETO_TAES_OPENAI_API_KEY` está definida em `.env`
- A chave deve começar com `sk-proj-`
- Reinicie o backend após alterar `.env`:
  ```powershell
  python -m uvicorn endpoints.server:app --host 0.0.0.0 --port 8000 --reload
  ```

---

### Port Already in Use

**Problema**: `OSError: [Errno 48] Address already in use`

**Solução**:

#### Windows PowerShell:
```powershell
# Encontrar processo na porta 8000
Get-Process | Where-Object {$_.Port -eq 8000}

# Matar processo
Stop-Process -Id <PID> -Force

# Ou usar porta diferente
python -m uvicorn endpoints.server:app --port 8001
```

#### Linux/macOS:
```bash
# Encontrar processo
lsof -i :8000

# Matar processo
kill -9 <PID>
```

---

## 📈 Resultados Esperados

Baseado no paper DART-SQL (ablação no Spider-dev):

- **Zero-Shot Baseline**: ~76.2% EM
- **Com Question Rewriting**: ~79.9% EM
- **Melhoria esperada**: +3-5% em datasets como Spider-Realistic

---

## 🎓 Referências

- **DART-SQL Paper**: "Text-to-SQL Parsing with Rewriting and Refinement"
- **Dataset**: Spider-Realistic (aherntech/spider-realistic)
- **Modelo**: GPT-4 / GPT-4 Turbo
- **Framework**: FastAPI, OpenAI API

---

## ⚠️ Limitações Atuais

- **EX (Execution Accuracy)** requer execução real nas databases SQLite do Spider (não implementado)
- Usamos **EM (Exact-Set-Match)** como métrica principal proxy
- Schema extraction depende da estrutura do dataset fornecido

---

## 🔜 Próximos Passos

1. Implementar EX real executando queries nas databases SQLite
2. Testar com dataset Spider original (não realistic)
3. Experimentar variações do prompt de rewriting
4. Análise qualitativa de casos onde RW ajuda/prejudica
5. Integração com banco de dados real
6. Melhorias de UX na interface web

---

## 📝 Licença

Este projeto é parte da pesquisa em avaliação de modelos de linguagem para geração de SQL.

---

## ✉️ Contato

Para dúvidas ou sugestões, abra uma issue no repositório.
