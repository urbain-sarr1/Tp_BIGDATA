import json
import pandas as pd
import s3fs
from confluent_kafka import Consumer
from datetime import datetime

# 🎯 Configuration de Kafka Consumer
kafka_config = {
    "bootstrap.servers": "127.0.0.1:9092",
    "group.id": "transactions-group",
    "auto.offset.reset": "latest",
}

# 📂 Configuration MinIO pour stockage S3
s3_client = s3fs.S3FileSystem(
    anon=False,
    key="minio",
    secret="minio123",
    client_kwargs={"endpoint_url": "http://127.0.0.1:9000"}
)

output_path = "s3://warehouse/transactions.parquet"

# 📦 Création du consommateur Kafka
consumer = Consumer(kafka_config)
consumer.subscribe(["transactions"])

# 🏗️ Fonction pour transformer et enregistrer les données en Parquet
def process_transactions():
    transactions = []

    while True:
        msg = consumer.poll(1.0)  # Attend un message max 1s
        
        if msg is None:
            continue
        if msg.error():
            print(f"Erreur Kafka: {msg.error()}")
            continue

        try:
            # 🔍 Parsing JSON
            transaction = json.loads(msg.value().decode("utf-8"))

            # 🔄 Transformation
            transaction["montant_eur"] = transaction["montant"] * 0.85  # USD → EUR

            # 🚫 Suppression des transactions invalides
            if transaction["montant"] is None or transaction["utilisateur"]["adresse"] is None:
                continue

            transactions.append(transaction)

            # 📦 Sauvegarde toutes les 10 transactions
            if len(transactions) >= 10:
                save_to_parquet(transactions)
                transactions = []

        except Exception as e:
            print(f"Erreur de traitement: {e}")

# 📤 Fonction pour sauvegarder en Parquet
def save_to_parquet(data):
    df = pd.DataFrame(data)
    df.to_parquet(output_path, engine='fastparquet', index=False, storage_options={
        "key": "minio",
        "secret": "minio123",
        "client_kwargs": {"endpoint_url": "http://127.0.0.1:9000"}
    })
    print("✅ Transactions enregistrées en Parquet !")

# 🚀 Lancement du traitement
if __name__ == "__main__":
    print("🟢 Démarrage de Kafka Streams...")
    process_transactions()
