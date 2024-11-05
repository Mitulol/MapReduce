"""MapReduce framework Manager node."""
import tempfile
import logging
import json
import time
import click
import shutil
import threading
from queue import Queue
from mapreduce.utils import ThreadSafeOrderedDict
from mapreduce.utils.network import tcp_server, udp_server, tcp_client, udp_client  # Only import necessary utilities
import socket
from pathlib import Path
from queue import Empty
# Call the function later
# tcp_server(...)

class Job:
    def __init__(self, job_id, job_data):
        self.job_id = job_id
        self.job_data = job_data
        self.tasks = Queue()  # Assuming jobs have multiple tasks
        self.completed_tasks = []

    def add_task(self, task):
        self.tasks.put(task)

    def next_task(self):
        """Return the next pending task to be assigned to a Worker."""
        if not self.tasks.empty():
            return self.tasks.get()
        return None

    def task_reset(self, task):
        """Re-enqueue a pending task, e.g., when a Worker is marked dead."""
        self.tasks.put(task) 
        #when a worker is marked dead

    def task_finished(self, task):
        """Mark a pending task as completed."""
        self.completed_tasks.append(task)
    
    def remove_task(self, task):
        """Remove a task from the completed task list - if needed for some reason."""
        if task in self.completed_tasks:
            self.completed_tasks.remove(task)


class RemoteWorker:
    def __init__(self, worker_id, host, port):
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.current_task = None
        self.is_alive = True
        self.state = "ready"
    # def assign_task(self, task):
    #     """Assign task to this Worker and send task data over network."""
    #     self.current_task = task
    #     self.is_alive = True  # Mark as active
    #     message = json.dumps(task).encode("utf-8")
    #     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    #         try:
    #             sock.connect(self.address)
    #             sock.sendall(message)
    #             LOGGER.info(f"Task {task['task_id']} assigned to worker at {self.address}")
    #         except socket.error:
    #             LOGGER.error(f"Failed to send task {task['task_id']} to worker at {self.address}")
    #             self.mark_as_dead()
    # def unassign_task(self):
    #     """Unassign task and return it, e.g., when Worker is marked dead."""
    def mark_as_dead(self):
        self.is_alive = False
        self.current_task = None

LOGGER = logging.getLogger(__name__)

class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        self.host = host
        self.port = port
        self.workers = ThreadSafeOrderedDict()
        self.stage = "map"
        self.job_executing = False
        self.signals = {
            "shutdown": False
        }
        self.current_job_id = 0
        self.job_queue = Queue()
        # self.total_tasks_todo = []
        self.num_map_tasks= 0
        
        self.threads = []
        self.threads.append(threading.Thread(target=udp_server, args=(host, port, self.signals, self.handle_func)))  # listens to heartbeats
        self.threads.append(threading.Thread(target=tcp_server, args=(host, port, self.signals, self.handle_func)))  # listens to messages
        

        for thread in self.threads:
            thread.start()
            LOGGER.info("a thread has started ")
        
        self.process_jobs()
        
        for thread in self.threads:
            thread.join()
    
    def forward_shutdown(self):
        shutdown_message = {
            "message_type": "shutdown"
        }

        for worker in self.workers.values():
            if worker.status != 'dead':
                try:
                    tcp_client(worker.host, worker.port, shutdown_message)
                except ConnectionRefusedError:
                    worker.status = 'dead'

            # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            #     try:
            #         sock.connect((worker_host, worker_port))
            #         sock.sendall(shutdown_message)
            #     except socket.error:
                    
            #         LOGGER.error("Failed to send shutdown to worker %s:%s", worker_host, worker_port)

        LOGGER.info("Manager shutting down")
        
    def handle_registration(self, message_dict):
        LOGGER.info(f"Manager:{self.port} [DEBUG] received\n{json.dumps(message_dict, indent=2)}")

        register_ack_message = json.dumps({
            "message_type": "register_ack"
        }).encode("utf-8")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                worker_host = message_dict["worker_host"]
                worker_port = message_dict["worker_port"]
                sock.connect((worker_host, worker_port))
                sock.sendall(register_ack_message)
                worker_id = (worker_host, worker_port)
                self.workers[worker_id] = RemoteWorker(worker_id = worker_id,host= worker_host, port= worker_port)
                LOGGER.info(f"Manager:{self.port} [INFO] registered worker {message_dict['worker_host']}:{message_dict['worker_port']}")
            except socket.error:
               LOGGER.error("Failed to acknowledge registration for worker %s:%s", worker_host, worker_port)

    def enqueue_job(self, job_data):
        job_id = self.current_job_id
        self.current_job_id += 1
        job = Job(job_id, job_data)
        self.job_queue.put(job)
        LOGGER.info("Job %d enqueued", job_id)

    def handle_func(self, host, port, signals, message_dic):
        LOGGER.debug(f"Worker:{port} [DEBUG] received\n{json.dumps(message_dic, indent=2)}")
        if message_dic.get("message_type") == "shutdown":
            self.signals["shutdown"] = True
            self.forward_shutdown()
        if message_dic.get("message_type") == "register":
            self.handle_registration(message_dic)
        if message_dic.get("message_type") == "new_manager_job":
            self.enqueue_job(message_dic)
        if message_dic.get("message_type") == "finished":
            # finish other tasks- if in map, reduce tasks by 1 and if map and tasks is 0, switch to reduce
            if self.stage == "map" and 
        if message_dic.get("message_type") == "heartbeat":
            LOGGER.info("HEART")

    # def finished():
        

        
    def run_job(self, job): # remember to pop job off queue, and tasks, communicate task over tcp socket
        self.job_executing = True
        job_id = job.job_id
        output_dir = Path(job.job_data["output_directory"])

        if output_dir.exists():
            LOGGER.info("Removing existing output directory: %s", output_dir)
            for item in output_dir.rglob('*'):
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    item.rmdir()
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=False)
        LOGGER.info("Created output directory: %s", output_dir)

        prefix = f"mapreduce-shared-job{job_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)
            time.sleep(2)

            input_dir = Path(job.job_data["input_directory"])
            # input_files = list(input_dir.glob('**/*'))
            input_files = sorted(input_dir.iterdir())
            LOGGER.info("Input files %s", input_files)

            num_mappers = job.job_data["num_mappers"]
            partitions = [[] for _ in range(num_mappers)]
            for i, input_file in enumerate(input_files):
                partitions[i % num_mappers].append(str(input_file))
            LOGGER.info("Created partitions %s", partitions)
            

            for task_id, partition in enumerate(partitions):
                task_data = {
                    "message_type": "new_map_task",
                    "task_id": task_id,
                    "input_paths": partition,
                    "executable": job.job_data["mapper_executable"],
                    "output_directory": str(tmpdir),
                    "num_partitions": job.job_data["num_reducers"],
                }
                LOGGER.info("task_id %s", task_id)
                LOGGER.info("task_data %s", task_data)
                job.add_task(task_data)
            LOGGER.info("job_id %s", job_id)
            LOGGER.info("job.data %s", job.job_data)

            while not self.signals["shutdown"] and job.tasks.qsize() > 0: ##later come back and add the task stuff here
                task = job.next_task()
                if not task:
                    LOGGER.info("No more tasks to process for job %d", job.job_id)
                    break
                
                self.mapping_tasks(task)
                job.task_finished(task)
                
                LOGGER.info("Processing task %s for job %d", task, job.job_id)
                
            LOGGER.info("Job %d completed or shutdown signal received", job.job_id)
            LOGGER.info("shutdown signal%s", self.signals["shutdown"])
        LOGGER.info("Cleaned up tmpdir %s", tmpdir)


    def process_jobs(self):
        while not self.signals["shutdown"]:
            if self.job_queue.qsize() > 0 and not self.job_executing:
                job = self.job_queue.get(timeout=1)
                LOGGER.info(f"Starting job {job.job_id}")
                self.run_job(job)
                

    def mapping_tasks(self, task):
        self.stage = "map"
        self.num_map_tasks += 1
        workerfound = False
        while not workerfound:
            for worker_id, remotework in self.workers.items(): #remote work contains a RemoteWorker
                if remotework.is_alive and remotework.current_task is None:
                    remotework.state = "busy"
                    try:
                        tcp_client(remotework.host,remotework.port,task)
                        workerfound = True
                        remotework.state = "ready"
                        break
                    except:
                        remotework.state = "dead"
                        continue

            if not workerfound:
                LOGGER.info("No available worker found. Retrying in 1 second...")
                time.sleep(1)
            
    # def assign_task(self, worker_id, task):
    #     """Assign task to this Worker."""
    #     LOGGER.info("1 fffff")
    #     worker_info = self.workers[worker_id]
    #     LOGGER.info("worker_info %s", worker_info)
    #     worker_info["state"] = "busy"
    #     tcp_client(self.host,self.port,task)
        # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        #     try:
        #         LOGGER.info("2 fffffe")
        #         sock.connect((worker_info["host"], worker_info["port"]))
        #         LOGGER.info("4546 fffffe")
        #         sock.sendall(message)
        #         LOGGER.info("ehhru fffffe")
        #         return True
        #     except socket.error:
        #         LOGGER.error(f"Failed to send task {task['task_id']} to worker at {self.address}")
        #         self.mark_as_dead() 
        #         task["worker_id"] = worker_id
        #         return False
            
        # LOGGER.info(f"Assigned task {task['task_id']} to worker {worker_id}")
    

@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()