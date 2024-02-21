import pika, time, sys
from datetime import datetime


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)

args = sys.argv
rec_id = args[1]
curr_timestamp = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

print("--------------------------------------------------------------------------------------")
print(f" [RECEIVER {rec_id}] \t STARTED: {curr_timestamp}")
print(" \t \t \t Waiting for incoming messages from HELLO QUEUE. Press CTRL+C to exit...")
print("--------------------------------------------------------------------------------------")

def callback(ch, method, properties, body):
    task_message = {body.decode()}
    task_message = str(task_message)
    task_message = task_message.replace("{", "").replace("}", "").replace("'", "")
    snt_timestamp, rest = task_message.split(' - ')
    task_name, task_details = rest.split(': ')
    snt_id, task_name = task_name.split(' ')
 
    rec_timestamp = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    print("--------------------------------------------------------------------------------------")
    print(f" [RECEIVER {rec_id}] New message received.")
    print(f" \t \t \t RCV TIME: {rec_timestamp}")
    print(f" \t [SENDER {snt_id}] \t SND TIME: {snt_timestamp}")
    print(f" \t \t \t MSG BODY: {task_name} {task_details}")
    time.sleep(body.count(b'.'))
    print("--------------------------------------------------------------------------------------")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)

channel.basic_consume(queue='task_queue', on_message_callback=callback)

channel.start_consuming()