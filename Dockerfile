FROM python:3.13.18-slim

# Sem buffer, os logs da aplicação aparecem imediatamente em `docker compose logs`.
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app

# Um único worker: o estado do nó (papel, líder, epoch) fica em memória no processo.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
