import pika
from classes import HeartbitAndTemperatureGenerator, Process

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


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

hbtg = HeartbitAndTemperatureGenerator(30)
message = hbtg.send_samples_to_processes()

# Δήλωση της ουράς διεργασιών
channel.queue_declare(queue='process_queue', durable=True)

# Δήλωση της dead-letter ουράς
channel.queue_declare(queue='dead_letter_queue', durable=True)
channel.exchange_declare(exchange='process_exchange', exchange_type='direct', durable=True)
channel.queue_bind(exchange='process_exchange', queue='dead_letter_queue', routing_key='process_queue')


print("====================================================================================================================================")
print(f" [Publisher] publisher.py")
print("------------------------------------------------------------------------------------------------------------------------------------")
print(f" [Publisher] Sent the samples to all 9 processes.")
message_timestamp, message_body = message.split(' = ')
print(f" \t SAMPLING TIMESTAMP: {message_timestamp}")
print(f" \t SAMPLES: {message_body}")
print("------------------------------------------------------------------------------------------------------------------------------------")


# Αποστολή του ίδιου μηνύματος 9 φορές ώστε να εξυπηρετηθεί από κάθε διεργασία το μέρος του δείγματος που έχει αναλάβει
for i in range(9):
    receiver_pr = list(processes.values())[i]
    receiver_id = receiver_pr.pid
    message = f" {message} + {receiver_id}"
    channel.basic_publish(exchange='process_exchange', routing_key='process_queue', body=message, properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))


print(" New Messages just published.")
print("====================================================================================================================================")

connection.close()