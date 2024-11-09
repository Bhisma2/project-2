from kafka import KafkaProducer
import csv
import json
import time
import random
import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'bus_transaction_data'
file_path = '../data/transjakarta.csv'
max_batches = 3 

def send_csv_data(max_duration_minutes=5):
    for batch_number in range(1, max_batches + 1):
        print(f"Memulai batch {batch_number}")

        start_time = datetime.datetime.now()
        
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                current_time = datetime.datetime.now()
                elapsed_time = (current_time - start_time).total_seconds() / 60 

                if elapsed_time >= max_duration_minutes:
                    print(f"Pengiriman batch {batch_number} selesai.")
                    break

                producer.send(topic_name, value=row)
                print(f"Sent: {row}")

                time.sleep(random.uniform(0.1, 1))

        time.sleep(2)

    print("Pengiriman data selesai setelah 3 batch.")

if __name__ == "__main__":
    send_csv_data()