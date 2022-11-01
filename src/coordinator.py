import json
import os
import subprocess
import sys
import time
from hashlib import sha1, sha256

import numpy as np
from flask import Flask, request
from requests import get as http_get
from requests import post as http_post

from utils.common import get_current_time


class Coordinator:

    def __init__(self, name, port):
        self.app = Flask(name)
        self.port = port
        self.id = 0
        self.workers = {}
        self.cur_task = "IDLE"
        self.num_partitions = 3
        self.logs = "logs"


    def add_worker(self, port):
        """
        Check if port is registered with master, if not - register and provide it a unique id
        """
        if port not in self.workers:
            self.id += 1
            self.workers[port] = self.id
            return self.id
        else:
            return -1
    
    def probe_workers(self):
        """
        Send probe requests to each worker and update list of registered workers based on the response
        """
        for port in self.workers.copy():
            try:
                res = http_get(f"http://127.0.0.1:{port}/")
                if not (res and res.ok and res.text == "Alive"):
                    self.workers.pop(port)
                    print(f"{get_current_time()} Worker {port} has died. Removing from list of workers")
                    with open(f"{self.logs}/logs.txt",'a') as f:
                        print(f"{get_current_time()} Worker {port} has died. Removing from list of workers",file=f)


            except:
                # if not responding then remove worker 
                self.workers.pop(port)
                print(f"{get_current_time()} Worker {port} has died. Removing from list of workers")
                with open(f"{self.logs}/logs.txt",'a') as f:
                    print(f"{get_current_time()} Worker {port} has died. Removing from list of workers",file=f)


    def partition_input_file(self, file):
        """
        Split the input text file into num_partitions splits. Each split corresponds to a partition
        Store splits in tmp/task_name/<partition#>
        Returns the partition directory
        """
        self.task_file = file
        with open(file, 'r') as f:
            input_data = f.read().split()
            partitions = [ partition.tolist() for partition in np.array_split(input_data, self.num_partitions) ]
            for i in range(self.num_partitions):
                partition_directory = f"./filesystem/{self.cur_task}/partition{i}"
                os.makedirs(partition_directory)

                output_path = f"{partition_directory}/input-{file}"
                partition_file = open(output_path, 'w')

                partition_data = ' '.join(partitions[i])
                partition_file.write(partition_data)
    
    def assign_partitions_to_workers(self):
        """
        Use any round-robin/ hash style partitioning to assign each partition to any 1 worker
        """
        workers = list(self.workers.keys())
        for partition in range(self.num_partitions):
            worker_addr = workers[partition%len(self.workers)]
            http_get(f"http://localhost:{worker_addr}/assign-task/{self.cur_task}")
            http_get(f"http://localhost:{worker_addr}/assign-partition/partition{partition}")
        
    
    def start_map(self):
        """
        Sends commands to workers to start the map tasks on their respective partitions. 
        """
        for worker in self.workers:
            body = {
                "mapper_path": self.mapper,
            }
            http_post(f"http://localhost:{worker}/map", json=body)
        with open(f"{self.logs}/logs.txt",'a') as f:
            print(f"{get_current_time()} {self.cur_task} : Map task started ",file=f)
    
    def start_reduce(self):
        """
        Sends commands to workers to start the reduce tasks on their respective partitions. 
        """
        for worker in self.workers:
            body = {
                "reducer_path": self.reducer,
            }
            http_post(f"http://localhost:{worker}/reduce", json=body)
        with open(f"{self.logs}/logs.txt",'a') as f:
            print(f"{get_current_time()} {self.cur_task} : Reduce task started ",file=f)

    def await_map_results(self, callback):
        task_directory = f"./filesystem/{self.cur_task}"
        for i in range(self.num_partitions):
            intermediate_file_name = f"{task_directory}/partition{i}/int-{self.task_file}" 
            while not os.path.exists(intermediate_file_name):
                time.sleep(0.05)
                continue
            callback(intermediate_file_name)
 
    def await_reduce_results(self, callback):
        task_directory = f"./filesystem/{self.cur_task}"
        for i in range(self.num_partitions):
            output_file_name = f"{task_directory}/partition{i}/out-{self.task_file}" 
            while not os.path.exists(output_file_name):
                time.sleep(0.05)
                continue
            callback(output_file_name)

    def shuffle(self, filepath):
        """
        read the intermediate map output, hash it and split then write to the new partitions
        """
        filename = os.path.basename(filepath).lstrip('int-')
        with open(filepath, 'r') as fd:
            for line in fd.readlines():
                key, val = line.split(',')
                key = key.strip()
                val = val.strip()
                hex_hash = sha1(key.encode('utf-8')).hexdigest()
                int_hash = int(hex_hash, 16)  # convert hexadecimal to integer
                target_partition = int_hash%self.num_partitions
                target_file = os.path.join(f"filesystem/{self.cur_task}/partition{target_partition}/redinput-{filename}")
                if not os.path.exists(target_file):
                    open(target_file, "w+").close() #create file
                os.system(f"echo '{key}, {val}' >> {target_file}") # append to the file
    
    def collect(self, output_file):
        with open(output_file, "r") as fd:
            output = fd.read()
        
        with open(f"filesystem/{self.cur_task}/out.txt", "a") as fd:
            print(output, file=fd)
        
    
    def end_task(self):
        print(f"{get_current_time()} Finished Task {self.cur_task}")
        with open(f"{self.logs}/logs.txt",'a') as f:
            print(f"{get_current_time()} {self.cur_task} : Finished Task ",file=f)
        self.cur_task = "IDLE"


    
    def start_task(self):
        """
        Wrapper around await_map_results, await_reduce_results and await_task to make it easier to run them.
        """
        self.start_map()
        self.await_map_results(self.shuffle)
        with open(f"{self.logs}/logs.txt",'a') as f:
            print(f"{get_current_time()} {self.cur_task} : Finished Map and Shuffle ",file=f)
        self.start_reduce()
        self.await_reduce_results(self.collect)
        with open(f"{self.logs}/logs.txt",'a') as f:
            print(f"{get_current_time()} {self.cur_task} : Finished Reduce and Collect ",file=f)
        self.end_task()
    
    def start_server(self):

        if not os.path.exists(self.logs):
                os.makedirs(f"./{self.logs}")

        # ====================ROUTES==================
        @self.app.route("/", methods=["GET"])
        def probe():
            return f"[{get_current_time()}] Alive\n"

        @self.app.route("/register/<port>")
        def register_worker(port):
            """
            Registers worker and returns a unique id to it.
            Worker will be identified using this unique id in all future communications
            """
            id = self.add_worker(port)
            if id != -1:
                return f"{id}"
            return "-1"

        @self.app.route("/schedule", methods=["POST"])
        def shched_mapred_task():
            if self.cur_task != "IDLE":
                return "Please wait for previous task to finish before scheduling new task"
            
            data = json.loads(request.data)

            assert "input_file" in data
            assert "task_name" in data
            assert "mapper_file" in data
            assert "reducer_file" in data

            self.mapper = data["mapper_file"]
            self.reducer = data["reducer_file"]
            self.cur_task = data["task_name"]
            task=data["task_name"]


            if os.path.exists(f"filesystem/{self.cur_task}"):
                print(f"{get_current_time()} Task name already exists choose a different name")
                with open(f"{self.logs}/logs.txt",'a') as f:
                    print(f"{get_current_time()} Task failed as task name already exists",file=f)
                return "Task name already exists, try again with different name"

            with open(f"{self.logs}/logs.txt",'a') as f:
                    print(f"{get_current_time()} New Task {self.cur_task} has been assigned",file=f)

            self.probe_workers()
            partitions_dir = self.partition_input_file(data["input_file"])
            self.assign_partitions_to_workers()
            self.start_task()

            return "Task successfully completed. Find the final output in filesystem/"+task+"/out.txt"

        
        """
        Start the server on localhost:PORT
        """
        self.app.run("127.0.0.1", self.port)



if __name__ == "__main__":
    PORT=5000

    coordinator = Coordinator(__name__, PORT)
    coordinator.start_server()
