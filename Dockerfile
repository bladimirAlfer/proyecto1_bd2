FROM python:3.11-slim

WORKDIR /app

# Evita archivos .pyc y mejora logs en consola
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Instalar dependencias del sistema mínimas
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias Python
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo el proyecto
COPY . .

# Crear carpetas necesarias si no existen
RUN mkdir -p data/csv data/db data/results experimental_results

EXPOSE 8000

CMD ["python", "frontend/app.py"]