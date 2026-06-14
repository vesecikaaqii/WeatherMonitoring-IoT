# Mobile dashboard (FastAPI + PWA)
FROM python:3.11-slim

WORKDIR /app

COPY mobile-dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mobile-dashboard/ /app/
# The LSTM forecaster + trained model live under stream-processing/; the mobile
# API imports lstm_forecast and loads models/ from here at runtime.
COPY stream-processing/ /app/stream-processing/

EXPOSE 8600

CMD ["uvicorn", "mobile_api:app", "--host", "0.0.0.0", "--port", "8600"]
