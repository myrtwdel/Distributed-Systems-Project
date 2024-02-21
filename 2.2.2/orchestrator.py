import pika, time, sys
from datetime import datetime

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

#Δήλωση της ουράς διεργασιών
channel.queue_declare(queue='task_queue', durable=True)

# Δήλωση της dead-letter ουράς
channel.queue_declare(queue='dead_letter_queue', durable=True)
channel.exchange_declare(exchange='queue_exchange', exchange_type='direct', durable=True)
channel.queue_bind(exchange='queue_exchange', queue='dead_letter_queue', routing_key='task_queue')

orch_id = sys.argv[1]
task_times = [int(x) for x in sys.argv[2:]]

snt_timestamp = datetime.now().strftime("%d-%m-%Y %H:%M:%S")


print("--------------------------------------------------------------------------------------")
print("--------------------------------------------------------------------------------------")
print(f" [ORCHESTRATOR {orch_id}] Process Name = orchestrator.py")
print(f" \t TASK SUBMISSION TIME: {snt_timestamp}")
print("--------------------------------------------------------------------------------------")

for idx, time_taken in enumerate(task_times):
    print(f" \n ORCHESTRATOR: {orch_id}; Task-ID: {idx + 1}; Duration: {time_taken} (seconds).")
    print("--------------------------------------------------------------------------------------")
    time.sleep(time_taken)
    task_message = f"{orch_id} - {idx + 1} : {time_taken}"
    channel.basic_publish(exchange='', routing_key='task_queue', body=task_message, properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))
print(f" \t [ORCHESTRATOR {orch_id}] Just enqueued {len(task_times)} tasks to WORK QUEUE, to be serviced by active workers.")
print("--------------------------------------------------------------------------------------")


connection.close()