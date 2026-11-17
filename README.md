# Gerenciador de Tarefas Distribuído

Trabalho 2 de MC714 (Sistemas Distribuídos), 2º semestre de 2026, Unicamp.

API de tarefas replicada em três contêineres Docker, que integra os algoritmos Bully (eleição de
líder), Primary-Backup (replicação) e Heartbeat (detecção de falhas). O funcionamento dos
algoritmos e a análise dos experimentos estão no relatório.

## Integrantes

| Nome | RA |
|---|---|
| Henrique Parede de Souza | 260497 |
| Pedro Brasil Barroso | 260637 |

## Estrutura

```
app/            código dos nós: API (FastAPI), algoritmos, comunicação HTTP e persistência (SQLite)
experiments/    execução dos experimentos e geração da figura do relatório
compose.yaml    cluster com três nós
Dockerfile      imagem dos nós
```

## Executar o sistema

Requer Docker com Compose v2 e as portas 8001 a 8003 livres.

```bash
docker compose up --build -d
curl localhost:8003/status
```

O nó 3 vence a eleição inicial e passa a `PRIMARY` depois de sincronizar as réplicas (campo `role`
em `/status`). Só o primário atende o cliente: um backup responde 409 com o endereço do líder, e
durante uma eleição ou recuperação os nós respondem 503.

```bash
curl -X POST localhost:8003/tasks -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: req-1' -d '{"title": "Minha tarefa"}'
curl localhost:8003/tasks
curl -X PUT localhost:8003/tasks/<id> -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: req-2' -d '{"status": "completed"}'
```

Repetir uma requisição com a mesma `Idempotency-Key` devolve o resultado anterior, sem aplicá-la
de novo. Para simular a falha do líder e depois encerrar o cluster:

```bash
docker compose kill node3   # após alguns segundos, o nó 2 assume como primário
curl localhost:8002/status
docker compose down
```

### Endpoints

| Endpoint | Descrição |
|---|---|
| `POST /tasks` | Cria uma tarefa (`title` e `status`, por padrão `pending`) |
| `GET /tasks` | Lista as tarefas |
| `PUT /tasks/{id}` | Atualiza `title` e/ou `status` |
| `GET /status` | Papel, líder, epoch, nós ativos, nós suspeitos e membros da replicação |
| `GET /metrics` | Contadores de mensagens e eventos usados nos experimentos |
| `GET /health` | Indica que a API do nó está no ar |

Os endpoints `/internal/*` são usados apenas na comunicação entre os nós. A documentação
interativa fica em `localhost:8001/docs`.

### Parâmetros

Variáveis de ambiente de cada nó, definidas em `compose.yaml`:

| Variável | Padrão | Descrição |
|---|---|---|
| `NODE_ID` | | Identificador do nó; o maior ativo vence a eleição |
| `PEERS` | | Endereços de todos os nós, no formato `1=http://node1:8000,2=...` |
| `DB_PATH` | | Caminho do banco SQLite do nó |
| `HEARTBEAT_INTERVAL` | `0.5` | Intervalo entre heartbeats (s) |
| `FAILURE_TIMEOUT` | `3.0` | Tempo sem heartbeats para suspeitar de um nó (s) |
| `REQUEST_TIMEOUT` | `1.0` | Prazo de cada mensagem entre nós (s) |
| `COORDINATOR_TIMEOUT` | `3.0` | Espera pelo anúncio do líder depois de receber OK (s) |

## Experimentos

Requer Python 3.10 ou superior e as portas 18001 a 18007 livres.

```bash
python -m venv venv
venv/bin/pip install -r experiments/requirements.txt
venv/bin/python experiments/run_experiments.py    # malha completa, cerca de 58 min
venv/bin/python experiments/analyze_results.py
```

`run_experiments.py` sobe um cluster novo para cada execução, aplica a carga e a falha do cenário
e derruba o cluster. Cada execução vira uma linha em `<out>/<experimento>.csv`, e as execuções
já registradas são puladas, então uma malha interrompida continua de onde parou.

| Opção | Padrão | Descrição |
|---|---|---|
| `--exp` | `all` | Experimento: `bully`, `pb`, `hb` ou `all` |
| `--reps` | `10` | Execuções por nível, com as sementes 1000, 2000, ... |
| `--out` | `experiments/results/raw` | Pasta dos CSVs |

`analyze_results.py` não tem opções: lê `experiments/results/raw`, gera
`relatorio/images/bully_mensagens.pdf` e imprime as médias ± IC95 citadas no relatório.

Os resultados do relatório foram obtidos com Docker 29.9.3 e Docker Compose 5.7.1. A imagem dos
nós usa Python 3.13.18, FastAPI 0.163.1, Uvicorn 0.54.0, HTTPX 0.28.1 e SQLite 3.46.1, versões
fixadas no `Dockerfile` e no `requirements.txt`.
