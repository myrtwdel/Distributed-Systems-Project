import pika, time, sys
from datetime import datetime

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)

args = sys.argv
snt_id = args[1]
task_name = args[2]
task_details = ''.join(args[3:])
snt_timestamp = datetime.now().strftime("%d-%m-%Y %H:%M:%S")


task_message = f"{snt_timestamp} - {snt_id} {task_name}: {task_details}"

channel.basic_publish(exchange='', routing_key='task_queue', body=task_message, properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))

print("--------------------------------------------------------------------------------------")
print(f" [SENDER {snt_id}] Just sent the following message to the HELLO QUEUE:")
print(f"\t [SENDER {snt_id}]\t SND TIME: {snt_timestamp}")
print(f"\t \t \t MSG BODY: {task_name} {task_details}")
print("--------------------------------------------------------------------------------------")
print("--------------------------------------------------------------------------------------")

connection.close()