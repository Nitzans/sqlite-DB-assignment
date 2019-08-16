#!/usr/bin/env python
# encoding=utf8
import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='query')

db_path = sys.argv[1]
country = sys.argv[2]
year = sys.argv[3]

#db_path = "chinook.db"
#country = "India"
#year = "2011"

packet = db_path + "$" + country + "$" + year

channel.basic_publish(exchange='', routing_key='query', body=packet,
                      properties=pika.BasicProperties(delivery_mode=2))
print("Query was sent")

connection.close()
