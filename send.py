
import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1'))
channel = connection.channel()

channel.queue_declare(queue='q1')

# type = CSV,JSON,XML,TBL
data = {
        "database": "C:\\Users\\10\\PycharmProjects\\project1\\chinook.db",
        "type": "CSV"
    }

message = json.dumps(data)

channel.basic_publish(exchange='',
                      routing_key='q1',
                      body=message)

print(" [x] Sent 'Hello Data'")

connection.close()

