import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

# Konfigurimi i Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensor_ids = ['S1', 'S2', 'S3']
topic_name = 'weather_data'

print("Duke nisur gjenerimin e të dhënave nga sensorët...")

try:
    while True:
        for sensor in sensor_ids:
            # Gjenerimi i të dhënave të simuluara
            data = {
                "sensor_id": sensor,
                "timestamp": datetime.utcnow().isoformat(),
                "temperature": round(random.uniform(-5.0, 40.0), 2),
                "humidity": round(random.uniform(20.0, 100.0), 2)
            }
            producer.send(topic_name, data)
            print(f"Dërguar në Kafka: {data}")
            
        time.sleep(2)
except KeyboardInterrupt:
    print("Simulatori u ndalua.")
finally:
    producer.close()