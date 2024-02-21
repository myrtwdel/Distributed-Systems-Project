import pika, sys, random

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()



#Δήλωση της ουράς διεργασιών
channel.queue_declare(queue='task_queue', durable=True)

# Δήλωση της dead-letter ουράς
channel.queue_declare(queue='dead_letter_queue', durable=True)
channel.exchange_declare(exchange='task_stream', exchange_type='direct', durable=True)
channel.queue_bind(exchange='task_stream', queue='dead_letter_queue', routing_key='task_queue')



pub_id = sys.argv[1]
message_tag = sys.argv[2]
message_body =  sys.argv[3:]
message_body= str(message_body)
message_body = message_body.replace("[", "").replace("]", "").replace("'", "").replace(",", "")
message_id = random.randint(0, 9)*1000 + random.randint(0, 9)*100 + random.randint(0, 9)*10 + random.randint(0, 9)
message = f"{pub_id} - {message_tag} = {message_id} + {message_body}"

#Εναλλακτήριο εργατών/ενορχηστρωτών
channel.exchange_declare(exchange='filtering_stream', exchange_type='topic', durable=True)
channel.queue_bind(exchange='filtering_stream', queue='task_queue', routing_key=message_tag)
channel.basic_publish(exchange='filtering_stream', body=message, routing_key=message_tag)


print("====================================================================================================================================")
print(f" [PUBLISHER {pub_id}] Publisher's Process Name = publisher.py")
print("------------------------------------------------------------------------------------------------------------------------------------")
print(f" \t PUBLISHER ID: {pub_id}")
print(f" \t MESSAGE ID: {pub_id}.{message_id}")
print(f" \t MESSAGE TAG: {message_tag}")
print(f" \t BODY: {message_body}")
print("------------------------------------------------------------------------------------------------------------------------------------")
channel.basic_publish(exchange='task_stream', routing_key='task_queue', body=message, properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))
print(f" New Message just published.")
print("====================================================================================================================================")



connection.close()