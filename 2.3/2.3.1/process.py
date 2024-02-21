class Process:
    def __init__(self, pid, neighbors):
        self.pid = pid
        self.neighbors = neighbors

    def get_process_by_pid(pid, processes):
        return processes.get(pid)

