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
        Split the input text file into num_worker partitions.
        Store partitions in tmp/task_name/
        Returns a the partition directory
        """
        pass

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
            data = json.loads(request.data)

            assert "input_file" in data
            assert "task_name" in data

            partitions_dir = self.partition_input_file(data["input_file"])
            
            pass

        @self.app.route("/report/<phase>/<worker>/<status>")
        def report_map_status(phase, worker, status):
            """
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
