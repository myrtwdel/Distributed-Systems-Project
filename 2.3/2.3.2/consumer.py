import pika, sys, signal, random, time
from progress.bar import ChargingBar
from classes import Process

def signal_handler(sig, frame):
    print(" \n Interrupted")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

#Αρχικοποίηση διεργασιών
processes = {
    "01": Process("01", ["04", "09", "18"]),
    "04": Process("04", ["09", "14", "20"]),
    "09": Process("09", ["11", "21", "28"]),
    "11": Process("11", ["14", "18", "20", "28"]),
    "14": Process("14", ["18", "28"]),
    "18": Process("18", ["04", "20", "28"]),
    "20": Process("20", ["04", "28"]),
    "21": Process("21", ["01", "09", "28"]),
    "28": Process("28", ["01", "04"])
}

# Δήλωση της ουράς διεργασιών
channel.queue_declare(queue='process_queue', durable=True)

# Δήλωση της dead-letter ουράς
channel.queue_declare(queue='dead_letter_queue', durable=True)
channel.exchange_declare(exchange='process_exchange', exchange_type='direct', durable=True)
channel.queue_bind(exchange='process_exchange', queue='dead_letter_queue', routing_key='process_queue')

recipient_id = sys.argv[1]

# Εναλλακτήριο διεργασιών
channel.exchange_declare(exchange='filtering_stream', exchange_type='topic', durable=True)
channel.queue_bind(exchange='filtering_stream', queue='process_queue', routing_key=recipient_id)


print("====================================================================================================================================")
print(f" [Process {recipient_id}] Awaiting Messages from its neighbors. Press CTRL+C to exit...")

counter = 0

def callback(ch, method, properties, body):
    message = body.decode()
    message = str(message)
    message = message.replace("{", "").replace("}", "").replace("'", "")
    sender_id, rest = message.split(' + ')
    recipient_ids, message_body = rest.split(' - ')
    
    if recipient_id in recipient_ids:
        print("====================================================================================================================================")
        print(f" [Process {recipient_id} Just received a new message:]")
        print("------------------------------------------------------------------------------------------------------------------------------------")
        print(f" \t SENDER PROCESS ID: {sender_id}")
        print(f" \t {message_body}")
        global counter
        counter += 1
        print("------------------------------------------------------------------------------------------------------------------------------------")
        print(f" Total number of received Messages for Process {recipient_id} is: {counter}")
        print("------------------------------------------------------------------------------------------------------------------------------------")
        time_other = int(random.randint(1, 9))
        print(f" ...simulating the execution of some other local work, for {time_other} seconds...")
        with ChargingBar('') as bar:
            for i in range(100):
                time.sleep(int(time_other)/100)
                bar.next()
        ch.basic_ack(delivery_tag=method.delivery_tag)

    else:
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=True)

channel.basic_qos(prefetch_count=1)

channel.basic_consume(queue='process_queue', on_message_callback=callback)

channel.start_consuming()