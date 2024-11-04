"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import click
import threading
from queue import Queue
from mapreduce.utils import ThreadSafeOrderedDict
from mapreduce.utils.network import tcp_server, udp_server  # Only import necessary utilities
import socket
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


class RemoteWorker:
    def __init__(self, worker_id, address):
        self.worker_id = worker_id
        self.address = address
        self.current_task = None
        self.is_alive = True
    def assign_task(self, task):
        """Assign task to this Worker."""
    def unassign_task(self):
        """Unassign task and return it, e.g., when Worker is marked dead."""
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
        self.signals = {
            "shutdown": False
        }
        self.current_job_id = 0
        self.job_queue = Queue()

        self.threads = []
        self.threads.append(threading.Thread(target=udp_server, args=(host, port, self.signals, self.handle_func)))  # listens to heartbeats
        self.threads.append(threading.Thread(target=tcp_server, args=(host, port, self.signals, self.handle_func)))  # listens to messages
        # self.threads.append(threading.Thread(target=self.fault_tolerance)) # monitor workers & reassign if dead

        for thread in self.threads:
            thread.start()
            LOGGER.info("a thread has started ")
        
        for thread in self.threads:
            thread.join()

    def forward_shutdown(self):
        shutdown_message = json.dumps({
            "message_type": "shutdown"
        }).encode("utf-8")

        for (worker_host, worker_port) in self.workers:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                try:
                    sock.connect((worker_host, worker_port))
                    sock.sendall(shutdown_message)
                except socket.error:
                    LOGGER.error("Failed to send shutdown to worker %s:%s", worker_host, worker_port)

        self.signals["shutdown"] = True
        LOGGER.info("Manager shutting down")
        
    def handle_registration(self, message_dict):
        LOGGER.info(f"Manager:{self.port} [DEBUG] received\n{json.dumps(message_dict, indent=2)}")
        worker_host = message_dict["worker_host"]
        worker_port = message_dict["worker_port"]
        worker_id = (worker_host, worker_port)
        self.workers[worker_id] = {"state": "ready", "host": worker_host, "port": worker_port}

        register_ack_message = json.dumps({
            "message_type": "register_ack"
        }).encode("utf-8")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
               sock.connect((worker_host, worker_port))
               sock.sendall(register_ack_message)
               LOGGER.info(f"Manager:{self.port} [INFO] registered worker {message_dict['worker_host']}:{message_dict['worker_port']}")
            except socket.error:
               LOGGER.error("Failed to acknowledge registration for worker %s:%s", worker_host, worker_port)

    def enqueue_job(self, job_data):
        job_id = self.current_job_id
        self.current_job_id += 1
        job = Job(job_id, job_data)
        if job_id not in self.jobs_queue:
            self.jobs_queue[job_id] = []
        self.jobs_queue[job_id].append(job)
        for task in job_data["tasks"]:
            job.add_task(task)

        LOGGER.info("Job %d enqueued", job_id)

    def handle_func(self, host, port, signals, message_dic):
        LOGGER.debug(f"Worker:{port} [DEBUG] received\n{json.dumps(message_dic, indent=2)}")
        if message_dic.get("message_type") == "shutdown":
            self.forward_shutdown()
        elif message_dic.get("message_type") == "register":
            self.handle_registration(message_dic)
        elif message_dic.get("message_type") == "new_manager_job":
            self.enqueue_job(message_dic)

    def run_job(self, job):
        job_id = job['job_id']
        output_dir = job['output_directory']



            



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