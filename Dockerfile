FROM mcr.microsoft.com/playwright/python:v1.50.0-noble

WORKDIR /app

# Кэширование слоёв
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Код
COPY bot.py .

# Переменные окружения
ENV PYTHONUNBUFFERED=1
    PYTHONDONTWRITEBYTECODE=1
    PLAYWRIGHT_BROWSERS_PATH=0

# Railway
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Запуск бота
CMD ["python", "bot.py"]
