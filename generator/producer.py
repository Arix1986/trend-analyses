from kafka import KafkaProducer
import json, random,time
from datetime import datetime

TOPIC = 'social-events'

TOPICS_TENDENCIA=[
    "python","AI","kafka","beam","docker","streamlit","data-engineering"
]

ACCIONES=["post","comment","like","share"]

PESOS=[0.15,0.30,0.10,0.10,0.10,0.10,0.15]


producer= KafkaProducer(
    bootstrap_servers='172.30.86.244:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8') ,
    request_timeout_ms=5000,
    retries=3
)

def generar_evento():
    return {
        "timestamp":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "user_id": f"user_{random.randint(1,100)}",
        "action":random.choice(ACCIONES),
        "topic":random.choices(TOPICS_TENDENCIA, weights=PESOS)[0],
        "value":1
    }
print("Generador Iniciado - enviando eventos kasfka...")    
while True:
    payload = generar_evento()
    producer.send(TOPIC,payload)
    producer.flush()
    print(f"Enviado: {payload}")
    time.sleep(15)    
