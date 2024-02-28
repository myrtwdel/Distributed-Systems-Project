import time, random, pika
from datetime import datetime
from progress.bar import ChargingBar



class HeartbitAndTemperatureGenerator:

    def __init__(self, sampling_interval):
        self.sampling_interval = sampling_interval

    
    def generate_temperature_samples(self):
        temperature_samples = [round(random.uniform(0, 40), 1) for _ in range(32)]
        return temperature_samples
    
   
    def send_samples_to_processes(self):

        # Generate heartbit
        self.heartbit = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        
        # Generate temperature samples
        temperature_samples = self.generate_temperature_samples()

        # Sleep for sampling interval
        time.sleep(self.sampling_interval)

        # Return the message
        return f"{self.heartbit} = {temperature_samples}"


class Process:
    def __init__(self, pid, neighbors):
        self.pid = pid
        self.neighbors = neighbors
        self.counter = 0
        self.local_min = 0
        self.local_max = 0
        self.local_avg = 0
        self.global_min = 100
        self.global_max = 0
        self.global_avg = 0

    def get_process_by_pid(pid, processes):
        return processes.get(pid)
    
    # Λίστα των κατάλληλων αισθητηρών για την κάθε διεργασία ανάλογα με τον αμέσως επόμενο έξω-γείτονα
    def get_sensor_list(self):
        sensors = list(range(int(self.pid), int(self.neighbors[0])))
        return sensors
    
    
    # Ανάθεση τοπικών και καθολικών τιμών για κάθε διεργασία
    def update_values(self):

        # Αρχικοποίηση
        self.local_max = float(self.local_max)
        self.local_min = float(self.local_min)
        self.local_avg = float(self.local_avg)
        self.global_max = float(self.global_max)
        self.global_min = float(self.global_min)
        self.global_avg = float(self.global_avg)


        # Καθολικό Μέγιστο
        if self.local_max > self.global_max:
            self.global_max = self.local_max

        #Καθολικό Ελάχιστο
        if self.local_min < self.global_min:
            self.global_min = self.local_min

        # Καθολικός Μέσος Όρος
        if self.global_avg != 0:
            self.global_avg = (self.global_avg + self.local_avg) / 2
        else:
            self.global_avg = self.local_avg



    def publish(self, message_body):
        message = f"{self.pid} + {self.neighbors} * {message_body}"
        
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()

        # Δήλωση της ουράς διεργασιών
        channel.queue_declare(queue='process_queue', durable=True)

        # Δήλωση της dead-letter ουράς
        channel.queue_declare(queue='dead_letter_queue', durable=True)
        channel.exchange_declare(exchange='process_exchange', exchange_type='direct', durable=True)
        channel.queue_bind(exchange='process_exchange', queue='dead_letter_queue', routing_key='process_queue')

        # Ανταλλακτήριο διεργασιών
        channel.exchange_declare(exchange='filtering_stream', exchange_type='topic', durable=True)
        channel.queue_bind(exchange='filtering_stream', queue='process_queue', routing_key=self.pid)
        channel.basic_publish(exchange='filtering_stream', body=message, routing_key=self.pid)        


        print("====================================================================================================================================")
        print(f" [Process {self.pid}] Publisher's Process Name = publisher.py")
        print("------------------------------------------------------------------------------------------------------------------------------------")
        print(f" \t PROCESS ID: {self.pid}")
        print(f" \t RECIPIENT PROCESS ID(s): {str(self.neighbors).replace("[", "").replace("]","")}")
        message_timestamp, message_body = message_body.split(' = ')
        print(f" \t SAMPLING TIMESTAMP: {message_timestamp}")
        print(f" \t SAMPLES: {message_body}")
        print("------------------------------------------------------------------------------------------------------------------------------------")
        channel.basic_publish(exchange='process_exchange', routing_key='process_queue', body=message, properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))
        print(f" New Message just published.")
        print("====================================================================================================================================")

    def callback(self, ch, method, properties, body):
        message = body.decode()
        message = str(message)
        message = message.replace("{", "").replace("}", "").replace("'", "")
        sender_id, rest = message.split(' + ')
        recipient_ids, rest = rest.split(' * ')
        message_timestamp, message_body = rest.split(' = ')
        message_body = message_body.strip("[]").split(', ')

        sensors = self.get_sensor_list()
        indexes = [x - 1 for x in sensors]

        samples = [message_body[i] for i in indexes]
        samples = sorted(samples, key = lambda x:float(x))

        self.local_max = max(samples, key=lambda x:float(x))
        self.local_min = min(samples, key=lambda x:float(x))
        self.local_avg = sum(float(x) for x in samples)/len(samples)
    
        if self.pid in recipient_ids:
            print("====================================================================================================================================")
            print(f" [Process {self.pid} Just received a new message:]")
            print("------------------------------------------------------------------------------------------------------------------------------------")
            print(f" \t SENDER PROCESS ID: {sender_id}")
            print(f" \t SENT FROM SENSORS: {sensors}")
            print(f" \t SAMPLING TIMESTAMP: {message_timestamp}")
            print(f" \t SAMPLES: {str(samples).replace("'","")}")
            print(f" \t HIGHEST TEMPERATURE: {self.local_max}")
            print(f" \t LOWEST TEMPERATURE {self.local_min}")
            print(f" \t AVERAGE TEMPERATURE: {self.local_avg}")
            print("------------------------------------------------------------------------------------------------------------------------------------")
        
            if (float(self.local_max) > float(self.global_max)):
                print(" \t > Global Maximum Temperature updated...")
        

            if (float(self.local_min) < float(self.global_min)):
                print(" \t > Global Minimum Temperature updated...")

        
            if (float(self.local_max) != float(self.global_max)):
                print(" \t > Global Average Temperature updated... \n")

            self.update_values()

            print(f" \t GLOBAL MAX: {self.global_max}")
            print(f" \t GLOBAL MIN: {self.global_min}")
            print(f" \t GLOBAL AVERAGE: {self.global_avg}")


            self.counter += 1
            print("------------------------------------------------------------------------------------------------------------------------------------")
            print(f" Total number of received Messages for Process {self.pid} is: {self.counter}")
            print("------------------------------------------------------------------------------------------------------------------------------------")
            time_other = int(random.randint(1, 9))
            print(f" ...simulating the execution of some other local work, for {time_other} seconds...")
            with ChargingBar('') as bar:
                for i in range(100):
                    time.sleep(int(time_other)/100)
                    bar.next()
            ch.basic_ack(delivery_tag=method.delivery_tag)

            self.publish(rest)

            print("====================================================================================================================================")
            print(f" [Process {self.pid}] Awaiting Messages from its neighbors. Press CTRL+C to exit...")

        else:
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=True)


    def consume(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()

        # Δήλωση της ουράς διεργασιών
        channel.queue_declare(queue='process_queue', durable=True)

        # Δήλωση της dead-letter ουράς
        channel.queue_declare(queue='dead_letter_queue', durable=True)
        channel.exchange_declare(exchange='process_exchange', exchange_type='direct', durable=True)
        channel.queue_bind(exchange='process_exchange', queue='dead_letter_queue', routing_key='process_queue')

        # Ανταλλακτήριο διεργασιών
        channel.exchange_declare(exchange='filtering_stream', exchange_type='topic', durable=True)
        channel.queue_bind(exchange='filtering_stream', queue='process_queue', routing_key=self.pid)

        print("====================================================================================================================================")
        print(f" [Process {self.pid}] Awaiting Messages from its neighbors. Press CTRL+C to exit...")


        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='process_queue', on_message_callback=self.callback)
        channel.start_consuming()

        



        