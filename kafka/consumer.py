from kafka import KafkaConsumer
import json
import csv

# Inisialisasi Kafka Consumer
consumer = KafkaConsumer(
    'bus_transaction_data',  # Nama topik Kafka yang sama dengan producer
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=5000  # Timeout jika tidak ada pesan baru dalam 5 detik
)

# Fungsi untuk memproses sejumlah data
def consume_data_with_timeout(limit=10):
    count = 0
    batch = []

    for message in consumer:
        batch.append(message.value)
        print(f"Received: {message.value}")
        count += 1
        
        # Berhenti jika sudah mencapai batas limit
        if count >= limit:
            break
    
    # Simpan batch ke file JSON untuk verifikasi
    if batch:
        with open('limited_batch_data.json', 'w') as json_file:
            json.dump(batch, json_file, indent=2)
        print("Batch data saved to limited_batch_data.json")

        # Simpan batch ke file CSV
        with open('limited_batch_data.csv', 'w', newline='') as csv_file:
            # Mengambil header dari kunci data pertama
            writer = csv.DictWriter(csv_file, fieldnames=batch[0].keys())
            writer.writeheader()
            writer.writerows(batch)
        print("Batch data saved to limited_batch_data.csv")

if __name__ == "__main__":
    consume_data_with_timeout()
