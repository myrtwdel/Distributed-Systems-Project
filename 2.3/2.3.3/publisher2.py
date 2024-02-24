import pika, sys
from classes import Process
from classes import HeartbitAndTemperatureGenerator

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

hbtg = HeartbitAndTemperatureGenerator(1)

# Δήλωση της ουράς διεργασιών
channel.queue_declare(queue='process_queue', durable=True)

# Δήλωση της dead-letter ουράς
channel.queue_declare(queue='dead_letter_queue', durable=True)
channel.exchange_declare(exchange='process_exchange', exchange_type='direct', durable=True)
channel.queue_bind(exchange='process_exchange', queue='dead_letter_queue', routing_key='process_queue')


message = hbtg.send_samples_to_processes()
messages = []
# Αποστολή του ίδιου μηνύματος 9 φορές ώστε να εξυπηρετηθεί από κάθε διεργασία το μέρος του δείγματος που έχει αναλάβει
for pid, process in processes.items():
    receiver_id = process.pid
    message = f" {message} + {receiver_id}"
    #messages.append(message)
    channel.basic_publish(exchange='process_exchange', routing_key='process_queue', body=message, properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))

#for message in messages:
   # channel.basic_publish(exchange='process_exchange', routing_key='process_queue', body=message, properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))

# Εναλλακτήριο διεργασιών
channel.exchange_declare(exchange='filtering_stream', exchange_type='topic', durable=True)
channel.queue_bind(exchange='filtering_stream', queue='process_queue')
channel.basic_publish(exchange='filtering_stream', body=message, routing_key='process')



print("====================================================================================================================================")
print(f" [Process SENDER] Publisher's Process Name = publisher.py")
print("------------------------------------------------------------------------------------------------------------------------------------")
print(f" \t PROCESS ID: SENDER")
print(f" \t RECIPIENT PROCESS ID(s): ALL")
message_timestamp, message_body = message.split(' = ')
print(f" \t SAMPLING TIMESTAMP: {message_timestamp}")
print(f" \t SAMPLES: {message_body}")
print("------------------------------------------------------------------------------------------------------------------------------------")
print(f" New Message just published.")
print("====================================================================================================================================")





connection.close()