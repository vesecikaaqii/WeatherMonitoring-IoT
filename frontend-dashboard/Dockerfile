# Streamlit dashboard
FROM python:3.11-slim

WORKDIR /app

COPY frontend-dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy dashboard + the lstm_forecast helper it imports for the AI page
COPY frontend-dashboard/ /app/frontend-dashboard/
COPY stream-processing/ /app/stream-processing/

EXPOSE 8501

CMD ["streamlit", "run", "frontend-dashboard/app.py", \
     "--server.address=0.0.0.0", "--server.port=8501"]
