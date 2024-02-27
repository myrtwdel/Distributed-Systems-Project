import time, random
from datetime import datetime


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
        self.local_min = 0
        self.local_max = 0
        self.local_avg = 0
        self.global_min = 100
        self.global_max = 0
        self.global_avg = 0

    def get_process_by_pid(pid, processes):
        return processes.get(pid)
    
    def get_sensor_list(self):
        sensors = list(range(int(self.pid), int(self.neighbors[0])))
        return sensors
    
    def update_values(self):

        self.local_min = float(self.local_min)
        self.local_max = float(self.local_max)
        self.local_avg = float(self.local_avg)
        self.global_min = float(self.global_min)
        self.global_max = float(self.global_max)
        self.global_avg = float(self.global_avg)

        if self.global_avg != 0:
            self.global_avg = (self.global_avg + self.local_avg) / 2
        else:
            self.global_avg = self.local_avg

        if self.local_max > self.global_max:
            self.global_max = self.local_max

        if self.local_min < self.global_min:
            self.global_min = self.local_min


    