#!/usr/bin/env python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='query')

db_path = "chinook.db"
country = "Argentina"
year = "2011"

packet = db_path + "$" + country + "$" + year

channel.basic_publish(exchange='', routing_key='query', body=packet,
                      properties=pika.BasicProperties(delivery_mode=2))
print("Query was sent")

connection.close()
