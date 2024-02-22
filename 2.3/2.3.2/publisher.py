import pika, sys
from classes import Process
from classes import HeartbitAndTemperatureGenerator


def check_elements_exist(array1, array2):
    for element in array1:
        if element not in array2:
            return False
    return True


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

hbtg = HeartbitAndTemperatureGenerator(sampling_interval=30, repeat_sampling=True)

# Δήλωση της ουράς διεργασιών
channel.queue_declare(queue='process_queue', durable=True)

# Δήλωση της dead-letter ουράς
channel.queue_declare(queue='dead_letter_queue', durable=True)
channel.exchange_declare(exchange='process_exchange', exchange_type='direct', durable=True)
channel.queue_bind(exchange='process_exchange', queue='dead_letter_queue', routing_key='process_queue')

sender_id = sys.argv[1]
recipient_ids = sys.argv[2].split(',')
message_body = hbtg.send_samples_to_processes()
#message_body = message_body.replace("[", "").replace("]", "").replace("'", "").replace(",", "")
message = f"{sender_id} + {recipient_ids} - {message_body}"
sender_pr = Process.get_process_by_pid(sender_id, processes)

# Εναλλακτήριο διεργασιών
channel.exchange_declare(exchange='filtering_stream', exchange_type='topic', durable=True)
channel.queue_bind(exchange='filtering_stream', queue='process_queue', routing_key=sender_id)
channel.basic_publish(exchange='filtering_stream', body=message, routing_key=sender_id)


if check_elements_exist(recipient_ids, sender_pr.neighbors):
    print("====================================================================================================================================")
    print(f" [Process {sender_id}] Publisher's Process Name = publisher.py")
    print("------------------------------------------------------------------------------------------------------------------------------------")
    print(f" \t PROCESS ID: {sender_id}")
    print(f" \t RECIPIENT PROCESS ID(s): {recipient_ids}")
    print(f" \t {message_body}")
    print("------------------------------------------------------------------------------------------------------------------------------------")
    channel.basic_publish(exchange='process_exchange', routing_key='process_queue', body=message, properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))
    print(f" New Message just published.")
    print("====================================================================================================================================")

else:
    print("Couldn't send message, because the receivers are not neighbors.")




connection.close()