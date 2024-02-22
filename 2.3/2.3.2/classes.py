import time, random
from datetime import datetime

class HeartbitAndTemperatureGenerator:

    def __init__(self, sampling_interval, repeat_sampling):
        self.sampling_interval = sampling_interval
        self.repeat_sampling = repeat_sampling
        self.heartbit = 0

    def generate_heartbit(self):
        self.heartbit +=1
        return self.heartbit
    
    def generate_temperature_samples(self):
        temperature_samples = [round(random.uniform(0, 40), 1) for _ in range(32)]
        return temperature_samples
    
   
    def send_samples_to_processes(self):
        while True:
            # Generate heartbit
            self.heartbit = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

            # Generate temperature samples
            temperature_samples = self.generate_temperature_samples()

            # Sleep for sampling interval
            time.sleep(self.sampling_interval)

            # If repeat_sampling is False, break the loop
            if not self.repeat_sampling:
                break
            
            return f"Samples for Heartbit {self.heartbit}: {temperature_samples}"
        

class Process:
    def __init__(self, pid, neighbors):
        self.pid = pid
        self.neighbors = neighbors

    def get_process_by_pid(pid, processes):
        return processes.get(pid)


