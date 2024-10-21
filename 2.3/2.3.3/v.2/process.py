import sys, signal
from classes import Process
from classes import HeartbitAndTemperatureGenerator


def signal_handler(sig, frame):
    print(" \n Interrupted")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


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


process_id = sys.argv[1]
process_pr = Process.get_process_by_pid(process_id, processes)

# Για την αποστολή ενός μηνύματος
hbtg = HeartbitAndTemperatureGenerator(1)
message_body = hbtg.send_samples_to_processes()
    

if str(sys.argv[2]) == "p":
    process_pr.publish(message_body)
    process_pr.consume()

elif str(sys.argv[2]) == "c":
    process_pr.consume()