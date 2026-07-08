import random
import json
import time

# to generate many records but their ts gap is 5
sim_time = time.time()

def gen_data():
    global sim_time
    sim_time += 5

    data = {
        "timestamp": sim_time,
        "voltage_v": round(random.uniform(80, 100), 2)
    }

    print(json.dumps(data))

if __name__ == "__main__":
    while True:
        gen_data()