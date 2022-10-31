from hashlib import sha1, sha256
import json
from flask import Flask, request
from requests import get as http_get, post as http_post
from utils.common import get_current_time
import os
import subprocess

class Coordinator:

    def __init__(self, name, port):
        self.app = Flask(name)
        self.port = port
        self.id = 0
        self.workers = {}
        self.cur_task = "IDLE"
        self.num_partitions = 3

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
        for port, id in self.workers:
            try:
                res = http_get(f"127.0.0.1:{port}/")
                if not (res and res.ok and res.text == "Alive"):
                    print(f"{get_current_time()} Worker")
            except:
                # if not responding then remove worker 
                self.workers.pop(port)

    def partition_input_file(self, file):
        """
        Split the input text file into num_partitions splits. Each split corresponds to a partition
        Store splits in tmp/task_name/<partition#>
        Returns a the partition directory
        """
        pass
    
    def assign_partitions_to_workers(self, partitions_dir):
        """
        Use any round-robin/ hash style partitioning to assign each partition to any 1 worker
        """
        for partition in range(self.num_partitions):
            worker_addr = self.workers[partition%len(self.workers)]
            http_get(f"localhost:{worker_addr}/assign-partition/{partition}")
        
    
    def start_map(self):
        """
        Sends commands to workers to start the map tasks on their respective partitions. 
        """
        for worker in self.workers:
            http_get(f"localhost:{worker}/map")
        

    def await_map_results(self, callback):
        """
        start a process that checks every x seconds if a map process has ended. 
        when all maps end, collect intermediate outputs and shuffle them to correct partitions
        based on hash/range. Then execute callback (indicate to workers to start reduce phase)
        """
        pass
    
    def shuffle(self, filepath):
        """
        read the intermediate map output, hash it and split then write to the new partitions
        """
        filename = os.path.basename(filepath)
        with open(filepath, 'r') as fd:
            for line in fd.readlines():
                key, *val = line.split()
                target_partition = sha1(key)%self.num_partitions
                target_file = os.path.join(f"filesystem/{self.cur_task}/{target_partition}/redinput-{filename}")
                if not os.path.exists(target_file):
                    open(target_file, "w").close() #create file
                os.system(f"echo {key, val} >> {target_file}") # append to the file

    def end_task(self):
        print(f"{get_current_time()} Finished Task {self.cur_task}")
        self.cur_task = "IDLE"

    async def start_task(self):
        """
        Wrapper around await_map_results, await_reduce_results and await_task to make it easier to run them asynchronously.
        """
        self.start_map()
        self.await_map_results(self.shuffle)
        self.await_reduce_results()
        self.end_task()
    
    def start_server(self):

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
            id = self.add_worker(id, port)
            if id != -1:
                return f"{id}"
            return -1

        @self.app.route("/schedule", methods=["POST"]):
        def shched_mapred_task():
            if self.cur_task != "IDLE":
                return "Please wait for previous task to finish before scheduling new task"
            
            data = json.loads(request.data)

            assert "input_file" in data
            assert "task_name" in data

            self.cur_task = data["task_name"]
            self.probe_workers()
            partitions_dir = self.partition_input_file(data["input_file"])
            self.assign_partitions_to_workers(partitions_dir)
            self.start_task()

            return "task scheduled successfully"

        
        """
        Start the server on localhost:PORT
        """
        self.app.run("127.0.0.1", self.port)



if __name__ == "__main__":
    PORT=5000
    coordinator = Coordinator(__name__, PORT)
    coordinator.start_server()
