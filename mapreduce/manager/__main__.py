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
            if worker.state != 'dead':
                try:
                    tcp_client(worker.host, worker.port, shutdown_message)
                except ConnectionRefusedError:
                    worker.state = 'dead'

            # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            #     try:
            #         sock.connect((worker_host, worker_port))
            #         sock.sendall(shutdown_message)
            #     except socket.error:
                    
            #         LOGGER.error("Failed to send shutdown to worker %s:%s", worker_host, worker_port)

        LOGGER.info("Manager shutting down")
        
    # Called by TCP thread 
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
            # QUESTION: we need to assign a job to this worker right away, if we can

    def enqueue_job(self, job_data): # Just adds a job to the queue
        job_id = self.current_job_id
        self.current_job_id += 1
        job = Job(job_id, job_data)
        self.job_queue.put(job)
        LOGGER.info("Job %d enqueued", job_id)

    def handle_func(self, host, port, signals, message_dic):
        # TODO: shouldnt this say Manager instead of worker?
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
            self.handle_finished(message_dic)
        if message_dic.get("message_type") == "heartbeat":
            LOGGER.info("HEART")

    def handle_finished(self, message_dic):
        """Handle a 'finished' message indicating a map task completion."""
        LOGGER.info("Received 'finished' message for task %s from worker %s:%s",
                    message_dic["task_id"], message_dic["worker_host"], message_dic["worker_port"])

        # Decrement the count of map tasks
        self.num_map_tasks -= 1
        LOGGER.info("Remaining map tasks: %d", self.num_map_tasks)

        # Mark the worker as ready
        worker_id = (message_dic["worker_host"], message_dic["worker_port"])
        if worker_id in self.workers:
            self.workers[worker_id].state = "ready"
            self.workers[worker_id].current_task = None

        # If all map tasks are completed, initiate shutdown
        if self.num_map_tasks == 0:
            LOGGER.info("All map tasks completed for the current job.")
            self.stage = "complete"  # Indicate that no further processing is required
            self.job_executing = False  # Mark job as complete
            self.signals["shutdown"] = True
            self.forward_shutdown()


    # Called my Main thread
    def run_job(self, job):
        """Execute a job and process all its tasks."""
        self.job_executing = True
        job_id = job.job_id
        output_dir = Path(job.job_data["output_directory"])

        # Clean up existing output directory, if any
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

        # Set up temporary directory for intermediate map outputs
        prefix = f"mapreduce-shared-job{job_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)
            time.sleep(2) # QUESTION: why is this here?

            input_dir = Path(job.job_data["input_directory"])
            input_files = sorted(input_dir.iterdir())
            LOGGER.info("Input files %s", input_files)

            num_mappers = job.job_data["num_mappers"]
            partitions = [[] for _ in range(num_mappers)]
            for i, input_file in enumerate(input_files):
                partitions[i % num_mappers].append(str(input_file))
            LOGGER.info("Created partitions %s", partitions)

            # Add tasks to the job queue
            for task_id, partition in enumerate(partitions):
                task_data = {
                    "message_type": "new_map_task",
                    "task_id": task_id,
                    "input_paths": partition,
                    "executable": job.job_data["mapper_executable"],
                    "output_directory": str(tmpdir),
                    "num_partitions": job.job_data["num_reducers"],
                }
                job.add_task(task_data) # Adds to the queue of tasks in the job object

            # Call mapping_tasks with the Job instance to process tasks
            self.mapping_tasks(job)

        LOGGER.info("Cleaned up tmpdir %s", tmpdir)
        self.job_executing = False

    # Called by Main thread
    # QUESTION: does this thing ever stop??
    def process_jobs(self):
        while not self.signals["shutdown"]:
            if self.job_queue.qsize() > 0 and not self.job_executing:
                job = self.job_queue.get(timeout=1) # QUESTION: whats this?, why the argument timeout = 1?
                LOGGER.info(f"Starting job {job.job_id}")
                self.run_job(job)
        LOGGER.info("Shutdown signal received. Stopping job processing.")
                
    # Called by Main thread
    def mapping_tasks(self, job):
        """Assign map tasks to workers as they become available."""
        self.stage = "map" # QUESTION: is this redundant?
        self.num_map_tasks += job.tasks.qsize()
        
        while not self.signals["shutdown"] and not job.tasks.empty():
            # Check if there are any available workers
            available_worker = next((w for w in self.workers.values() if w.is_alive and w.state == "ready"), None)
            # gets the first available worker, following the order of registration convention

            if available_worker is None:
                # If no workers are available, wait and check again
                LOGGER.info("No available worker found. Retrying in 1 second...")
                time.sleep(1)
                continue

            # Assign a task to the available worker
            task = job.next_task() # task is a JSON Object
            if task:
                try:
                    # Update worker state to busy and send task
                    available_worker.state = "busy"
                    available_worker.current_task = task
                    tcp_client(available_worker.host, available_worker.port, task)
                    LOGGER.info("Assigned task %s to worker %s", task['task_id'], available_worker.worker_id)
                except socket.error:
                    # If there's an error sending the task, mark worker as dead
                    LOGGER.error("Failed to send task to worker %s:%s. Marking as dead.", available_worker.host, available_worker.port)
                    available_worker.mark_as_dead()
                    job.task_reset(task)  # Requeue the task for another worker

    # QUESTION: Is this ever called??
    def wait_for_task_completion(self, task):
        """Wait until the task is completed by the worker."""
        while any(w.current_task == task and w.state == "busy" for w in self.workers.values()):
            time.sleep(0.5)  # Check every 0.5 seconds if the task is completed
  

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