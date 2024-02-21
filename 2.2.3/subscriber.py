import pika, time, sys, signal, random

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
channel.exchange_declare(exchange='task_stream', exchange_type='direct', durable=True)
channel.queue_bind(exchange='task_stream', queue='dead_letter_queue', routing_key='task_queue')

#Εναλλακτήριο εργατών/ενορχηστρωτών
channel.exchange_declare(exchange='filtering_stream', exchange_type='topic', durable=True)

sub_id = sys.argv[1]
message_tags = sys.argv[2:]

message_tags = str(message_tags)
message_tags = message_tags.replace("[", "").replace("]", "").replace("'", "").replace(",", "")

print("====================================================================================================================================")
print(f" [SUBSCRIBER {sub_id}] Awaiting Messages with TAGS in [{message_tags}] from MESSAGE FILTERING STREAM. Press CTRL+C to exit...")

counter = 0

def callback(ch, method, properties, body):
    message = body.decode()
    message = str(message)
    message = message.replace("{", "").replace("}", "").replace("'", "")
    pub_id, rest = message.split(' - ')
    message_tag, rest = rest.split(' = ')
    message_id, message_body = rest.split(' + ')


    if "COMPLETED" in message_body:
        rest, task_id = rest.split(' ID: ')

        if message_tag in message_tags:

            print("====================================================================================================================================")
            print(f" [SUBSCRIBER {sub_id} Just received a new message:]")
            print("------------------------------------------------------------------------------------------------------------------------------------")
            print(f" \t PUBLISHER ID: {pub_id}")
            print(f" \t MESSAGE ID: {pub_id}.{message_id}")
            print(f" \t MESSAGE TAG: {message_tag}")
            print(f" \t BODY: {message_body}")
            global counter
            counter += 1
            print("------------------------------------------------------------------------------------------------------------------------------------")
            print(f" Total number of received Messages for SUBSCRIBER {sub_id} is: {counter}")
            print("------------------------------------------------------------------------------------------------------------------------------------")
            time_other = int(random.randint(1, 9))
            print(f" ...simulating the execution of some other local work, for {time_other} seconds...")
            time.sleep(time_other)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        else:
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=True)

    
    else:
        rest, time_taken = message_body.split(' LOAD: ')
        time_taken, rest = time_taken.split(' SECONDS')

        if message_tag in message_tags:

            print("====================================================================================================================================")
            print(f" [SUBSCRIBER {sub_id} Just received a new message:]")
            print("------------------------------------------------------------------------------------------------------------------------------------")
            print(f" \t PUBLISHER ID: {pub_id}")
            print(f" \t MESSAGE ID: {pub_id}.{message_id}")
            print(f" \t MESSAGE TAG: {message_tag}")
            print(f" \t BODY: {message_body}")
            time.sleep(int(time_taken))
            counter += 1
            print("------------------------------------------------------------------------------------------------------------------------------------")
            print(f" Total number of received Messages for SUBSCRIBER {sub_id} is: {counter}")
            print("------------------------------------------------------------------------------------------------------------------------------------")
            time_other = int(random.randint(1, 9))
            print(f" ...simulating the execution of some other local work, for {time_other} seconds...")
            time.sleep(time_other)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        else:
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=True)


channel.basic_qos(prefetch_count=1)

channel.basic_consume(queue='task_queue', on_message_callback=callback)

channel.start_consuming()