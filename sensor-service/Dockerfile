# Simulator + FastAPI control plane
FROM python:3.11-slim

WORKDIR /app

COPY sensor-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the sensor service code (config.py, simulator.py, simulator_api.py)
COPY sensor-service/ /app/

EXPOSE 8000

# Start the FastAPI control plane (which owns the simulator engine)
CMD ["uvicorn", "simulator_api:app", "--host", "0.0.0.0", "--port", "8000"]
