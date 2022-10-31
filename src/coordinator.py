from asyncio import subprocess
import json
from flask import Flask, request
import datetime
from requests import get as http_get


class Coordinator:

    def __init__(self, name, port):
        self.app = Flask(name)
        self.port = port
        self.id = 0
        self.workers = {}
        self.cur_task = "IDLE"

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
                    print(f"{self.get_current_time()} Worker")
            except:
                # if not responding then remove worker 
                self.workers.pop(port)

    def get_current_time():
        return datetime.datetime.now()

    def partition_input_file(self, file):
        """
        Split the input text file into num_worker splits. Each split corresponds to a partition
        Store splits in tmp/task_name/<partition#>
        Returns a the partition directory
        """
        pass
    
    def assign_partitions_to_workers(self, partitions_dir):
        """
        Use any round-robin/ hash style partitioning to assign each partition to any 1 worker
        """
        pass
    
    def start_map(self):
        """
        Sends commands to workers to start the map tasks on their respective partitions. 
        1 worker may get more than 1 command (1 per partition assigned to it)
        """
        pass

    def await_map_results(self, callback):
        """
        start a process that checks every x seconds if a map process has ended. 
        when all maps end, collect intermediate outputs and shuffle them to correct partitions
        based on hash/range. Then execute callback (indicate to workers to start reduce phase)
        """
        pass

    def start_task(self):
        """
        Wrapper around await_map_results, await_reduce_results and await_task to make it easier to run them asynchronously.
        """
        self.start_map()
        self.await_map_results()
        self.await_reduce_results()
        self.end_task()
    
    def start_server(self):

        # ====================ROUTES==================
        @self.app.route("/", methods=["GET"])
        def probe():
            return f"[{state.get_current_time()}] Alive\n"

        @self.app.route("/register/<port>")
        def register_worker(port):
            """
            Registers worker and returns a unique id to it.
            Worker will be identified using this unique id in all future communications
            """
            id = state.add_worker(id, port)
            if id != -1:
                return f"{id}"

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

        @self.app.route("/report/<phase>/<worker>/<status>")
        def report_map_status(phase, worker, status):
            """
            end point for worker to report status of task to coordinator
            @params:
                phase
                    value must be one of map or reduce
                worker
                    the unique assigned to the worker by the coordinator
                status
                    value must be one of 0 or 1
                    0: task succeeded
                    1: task failed
            """
            pass
        
        """
        Start the server on localhost:PORT
        """
        self.app.run("127.0.0.1", self.port)



if __name__ == "__main__":
    PORT=5000
    coordinator = Coordinator()
    coordinator.start_server(__name__, PORT)
