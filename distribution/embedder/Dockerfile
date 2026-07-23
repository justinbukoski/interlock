FROM python:3.12-slim
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir fastapi==0.116.1 sentence-transformers==5.1.0 uvicorn==0.35.0
COPY app.py /app/app.py
WORKDIR /app
EXPOSE 8840
CMD ["uvicorn", "app:api", "--host", "0.0.0.0", "--port", "8840", "--workers", "1"]
