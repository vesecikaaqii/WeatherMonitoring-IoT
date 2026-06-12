# Mobile dashboard (FastAPI + PWA)
FROM python:3.11-slim

WORKDIR /app

COPY mobile-dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mobile-dashboard/ /app/

EXPOSE 8600

CMD ["uvicorn", "mobile_api:app", "--host", "0.0.0.0", "--port", "8600"]
