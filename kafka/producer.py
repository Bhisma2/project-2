from kafka import KafkaProducer
import csv
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'bus_transaction_data'

def send_csv_data(limit=10):
    with open('../data/transjakarta.csv', 'r') as file:
        reader = csv.DictReader(file)
        count = 0
        for row in reader:
            if count >= limit:
                break
            producer.send(topic_name, value=row)
            print(f"Sent: {row}")
            count += 1
            
            # Tambahkan jeda antar pengiriman untuk simulasi streaming
            time.sleep(random.uniform(0.1, 1))

if __name__ == "__main__":
    send_csv_data()



# from kafka import KafkaProducer
# import csv
# import json
# import time
# import random

# producer = KafkaProducer(
#     bootstrap_servers='localhost:9092',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# topic_name = 'bus_transaction_data'

# def send_csv_data():
#     with open('../data/transjakarta.csv', 'r') as file:
#         reader = csv.DictReader(file)
#         for row in reader:
#             producer.send(topic_name, value=row)
#             print(f"Sent: {row}")
            
#             time.sleep(random.uniform(0.1, 1))

# if __name__ == "__main__":
#     send_csv_data()
