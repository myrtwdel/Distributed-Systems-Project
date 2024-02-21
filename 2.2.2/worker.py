import pika, time, sys, signal
from datetime import datetime
from progress.bar import ChargingBar


def signal_handler(sig, frame):
    print(" \n Interrupted")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

#Αν η διεργασία βρίσκεται στην ουρά διεργασιών για παραπάνω από 60 δευτερόλεπτα, μεταφέρεται στην ουρά dead-letter
channel.queue_declare(queue='task_queue', durable=True)

#Εδώ πάνε οι διεργασίες που διακόπτονται λόγω αδυναμίας εξυπηρέτησης
channel.queue_declare(queue='dead_letter_queue', durable=True)
channel.exchange_declare(exchange='queue_exchange', exchange_type='direct', durable=True)
channel.queue_bind(exchange='queue_exchange', queue='dead_letter_queue', routing_key='task_queue')


work_id = sys.argv[1]
curr_timestamp = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

print("--------------------------------------------------------------------------------------")
print("--------------------------------------------------------------------------------------")
print(f" [WORKER {work_id}] Claiming tasks to serve from WORK QUEUE. Press CTRL+C to exit...")
print("--------------------------------------------------------------------------------------")

def callback(ch, method, properties, body):
    task_message = {body.decode()}
    task_message = str(task_message)
    task_message = task_message.replace("{", "").replace("}", "").replace("'", "")
    orch_id, rest = task_message.split(' - ')
    task_id, time_taken = rest.split(' : ')
 
    
    print(f" \n [WORKER {work_id}] Received new task, at time {datetime.now().strftime("%d-%m-%Y %H:%M:%S")}")
    print(f" ORCHESTRATOR-ID: {orch_id}; Task-ID: {task_id}; Duration: {time_taken} (seconds)")
    with ChargingBar('') as bar:
        for i in range(100):
            time.sleep(int(time_taken)/100)
            bar.next()
    
    print("...task service is completed...")
    print("--------------------------------------------------------------------------------------")
    ch.basic_ack(delivery_tag=method.delivery_tag)



channel.basic_qos(prefetch_count=1)

channel.basic_consume(queue='task_queue', on_message_callback=callback)

channel.start_consuming()