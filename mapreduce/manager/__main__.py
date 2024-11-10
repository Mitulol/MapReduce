"""MapReduce framework Manager node."""
import tempfile
import logging
import json
import time
import shutil
import threading
import socket
from pathlib import Path
from queue import Queue

import click

from mapreduce.utils import ThreadSafeOrderedDict
from mapreduce.utils.network import tcp_server, udp_server, tcp_client

# fixed version was only changing handle func calls


class Job:
    """Job."""

    def __init__(self, job_id, job_data):
        """Init."""
        self.job_id = job_id
        self.job_data = job_data
        self.tasks = Queue()  # Assuming jobs have multiple tasks
        self.completed_tasks = []

    def add_task(self, task):
        """Add a task."""
        self.tasks.put(task)

    def next_task(self):
        """Return the next pending task to be assigned to a Worker."""
        if not self.tasks.empty():
            return self.tasks.get()
        return None

    def task_reset(self, task):
        """Re-enqueue a pending task, e.g., when a Worker is marked dead."""
        self.tasks.put(task)
        # when a worker is marked dead

    def task_finished(self, task):
        """Mark a pending task as completed."""
        self.completed_tasks.append(task)

    def remove_task(self, task):
        """Remove a task from the completed task list."""
        if task in self.completed_tasks:
            self.completed_tasks.remove(task)


class RemoteWorker:
    """Remote worker."""

    def __init__(self, worker_id, host, port):
        """Init."""
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.current_task = None
        # self.is_alive = True
        self.state = "ready"
        self.received_first_heartbeat = False

    def mark_as_dead(self):
        """Mark as dead."""
        self.state = "dead"
        # self.is_alive = False
        self.current_task = None


LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        self.host = host
        self.port = port
        self.workers = ThreadSafeOrderedDict()
        self.signals = {"shutdown": False}

        # self.stage = "map"
        # self.job_executing = False

        # self.current_job_id = 0
        self.job_queue = Queue()
        # self.current_job = None
        # self.num_map_tasks = 0
        # self.num_reduce_tasks = 0
        # self.last_heartbeat = ThreadSafeOrderedDict()
        # self.initial_map_tasks = 0
        self.threads = []

        self.everything = {
            "stage": "map",
            "job_executing": False,
            "current_job_id": 0,
            "current_job": None,
            "num_map_tasks": 0,
            "num_reduce_tasks": 0,
            "initial_map_tasks": 0,
            "last_heartbeat": ThreadSafeOrderedDict(),
        }
        self.threads.append(
            threading.Thread(
                target=udp_server,
                args=(host, port, self.signals, self.handle_func)
            )
        )
        self.threads.append(
            threading.Thread(
                target=tcp_server,
                args=(host, port, self.signals, self.handle_func)
            )
        )
        self.threads.append(
            threading.Thread(
                target=self.check_worker_heartbeats
            )
        )

        for thread in self.threads:
            thread.start()
            LOGGER.info("Started thread: %s", thread)

        self.process_jobs()

        for thread in self.threads:
            thread.join()

    def check_worker_heartbeats(self):
        """Check heartbeats."""
        # LOGGER.info("Running heartbeat check...")
        # While manager is not shutdown
        while not self.signals["shutdown"]:
            # LOGGER.info("Heartbeat check cycle running...")
            current_time = time.time()
            # looping through all registered workers,
            # checking their last heartbeat
            # Potential bug here, only checking 1 worker.
            # Maybe worker 3002 isn't added to this list.
            # Worker 3002 isn't in this list.
            for worker_id, last_time in list(
                    self.everything["last_heartbeat"].items()):
                time_elapsed = current_time - last_time
                if time_elapsed > 10:
                    if self.workers[worker_id].state in {"ready", "busy"}:
                        self.handle_worker_failure(worker_id)
                else:
                    LOGGER.info("Worker %s heartbeat OK. Last seen %ss \
                                ago.", worker_id, time_elapsed)
            time.sleep(1)

    def handle_heartbeat(self, message_dict):
        """Handle heartbeats."""
        worker_id = (message_dict["worker_host"], message_dict["worker_port"])
        if worker_id in self.workers:
            self.everything["last_heartbeat"][worker_id] = time.time()
            self.workers[worker_id].received_first_heartbeat = True
            # time. {self.last_heartbeat[worker_id]}")
        else:
            LOGGER.warning("Heartbeat received unknown worker %s", worker_id)

    def handle_worker_failure(self, worker_id):
        """Handle worker failure."""
        # LOGGER.info("Handle_worker_fail")
        worker = self.workers[worker_id]
        # print("Worker: ", worker.current_task)
        if worker.current_task is not None:
            # Requeue the task so another worker can handle it
            self.everything["current_job"].task_reset(worker.current_task)
            # LOGGER.info(f"Re-enqueued task {worker.current_task['task_id']}
            # from failed worker {worker_id}.")
        worker.mark_as_dead()
        self.send_message(self.everything["current_job"])

    def forward_shutdown(self):
        """Forward shutdown."""
        LOGGER.info("Shutdown signal received, completing current task")
        # Wait until all currently executing tasks are done
        while True:
            busy_workers = False
            for worker in self.workers.values():
                if worker.state == "busy":
                    busy_workers = True
                    break  # Exit the loop early since we found a busy worker
            if not busy_workers:
                break  # All workers are ready; exit the outer loop
            time.sleep(1)
    # Notify all active workers of shutdown
        shutdown_message = {"message_type": "shutdown"}
        for worker in self.workers.values():
            if worker.state != 'dead':
                try:
                    tcp_client(worker.host, worker.port, shutdown_message)
                except ConnectionRefusedError:
                    worker.state = 'dead'
        LOGGER.info("Manager shutting down after current tasks completed.")

    # Called by TCP thread
    def handle_registration(self, message_dict):
        """Handle registration."""
        LOGGER.info("Manager:%d [DEBUG] received\n%s", self.port,
                    json.dumps(message_dict, indent=2))

        register_ack_message = {
            "message_type": "register_ack"
        }
        worker_host = message_dict["worker_host"]
        worker_port = message_dict["worker_port"]
        worker_id = (worker_host, worker_port)

        try:
            tcp_client(worker_host, worker_port, register_ack_message)
            self.everything["last_heartbeat"][worker_id] = time.time()
            # First registration
            # Regular first registration
            if worker_id not in self.workers:
                self.workers[worker_id] = RemoteWorker(
                    worker_id=worker_id, host=worker_host, port=worker_port
                )
                self.everything["last_heartbeat"][worker_id] = time.time()
                LOGGER.info(
                    "Manager:%d [INFO] registered worker %s:%s",
                    self.port,
                    message_dict['worker_host'],
                    message_dict['worker_port']
                )
            # Edge case for revive registration
            else:
                if self.workers[worker_id].state == "dead":
                    self.workers[worker_id].state = "ready"
                # manager does not know worker died and revived
                else:
                    if self.workers[worker_id].current_task is not None:
                        # Requeue the task so another worker can handle it
                        self.everything["current_job"].task_reset(
                            self.workers[worker_id].current_task
                        )
        except ConnectionRefusedError:
            self.workers[worker_id].mark_as_dead()
        #        sock.connect((worker_host, worker_port))
        #        sock.sendall(register_ack_message)
        #        worker_id = (worker_host, worker_port)
        # self.workers[worker_id] = RemoteWorker(worker_id = worker_id,host=
        # worker_host, port= worker_port)
        #        self.last_heartbeat[worker_id] = time.time()
        #        LOGGER.info(f"Manager:{self.port} [INFO] registered worker
        # {message_dict['worker_host']}:{message_dict['worker_port']}")
        #    except socket.error:
        #       LOGGER.error("Failed to acknowledge registration for worker
        # %s:%s", worker_host, worker_port)
        # QUESTION: we need to assign a job to this worker right away

    def enqueue_job(self, job_data):
        """Enqueue job."""
        job_id = self.everything["current_job_id"]
        self.everything["current_job_id"] += 1
        job = Job(job_id, job_data)
        self.job_queue.put(job)
        LOGGER.info("Job %d enqueued", job_id)

    def handle_func(self, port, message_dic):
        """Handle all messages."""
        LOGGER.debug(
            "Manager:%d [DEBUG] received\n%s",
            port,
            json.dumps(message_dic, indent=2)
        )
        if message_dic.get("message_type") == "shutdown":
            self.signals["shutdown"] = True
            self.forward_shutdown()
        elif message_dic.get("message_type") == "register":
            self.handle_registration(message_dic)
        elif message_dic.get("message_type") == "new_manager_job":
            self.enqueue_job(message_dic)
        elif message_dic.get("message_type") == "finished":
            # finish other tasks- if in map, reduce tasks by
            # 1 and if map and tasks is 0, switch to reduce
            self.handle_finished(message_dic)
        elif message_dic.get("message_type") == "heartbeat":
            self.handle_heartbeat(message_dic)

    def handle_finished(self, message_dic):
        """Handle' finished' message- map task completion."""
        LOGGER.info(
            "Received 'finished' message for task %s from worker %s:%s",
            message_dic["task_id"],
            message_dic["worker_host"],
            message_dic["worker_port"]
        )
        # Decrement the count of map tasks
        if self.everything["stage"] == "map":
            self.everything["num_map_tasks"] -= 1
            LOGGER.info(
                "Remaining map tasks: %d", self.everything["num_map_tasks"]
            )

            # Mark the worker as ready
            worker_host = message_dic["worker_host"]
            worker_port = message_dic["worker_port"]
            worker_id = (worker_host, worker_port)

            if worker_id in self.workers:
                self.workers[worker_id].state = "ready"
                LOGGER.info(
                    "Task %s finished by worker (state: %s)",
                    message_dic["task_id"], self.workers[worker_id].state
                )
                self.workers[worker_id].current_task = None

            # If all map tasks are completed, initiate shutdown
            if self.everything["num_map_tasks"] == 0:
                LOGGER.info("All map tasks completed for current job.")
                self.everything["stage"] = "reduce"
                # self.job_executing = False  # Mark job as complete
                # self.signals["shutdown"] = True
                # self.forward_shutdown()
                # self.reduce_tasks()
        elif self.everything["stage"] == "reduce":
            self.everything["num_reduce_tasks"] -= 1
            LOGGER.info(
                "Remaining reduce tasks: %d",
                self.everything["num_reduce_tasks"]
            )
            # Mark the worker as ready
            worker_id = (
                message_dic["worker_host"],
                message_dic["worker_port"]
            )
            if worker_id in self.workers:
                self.workers[worker_id].state = "ready"
                LOGGER.info(
                    "Received 'finished' message for task %s from worker %s",
                    message_dic["task_id"], self.workers[worker_id].state
                )
                self.workers[worker_id].current_task = None

            # If all map tasks are completed, initiate shutdown
            if self.everything["num_reduce_tasks"] == 0:
                LOGGER.info("All reduce tasks completed for the current job.")
                self.everything["stage"] = "map"  # Reset stage for next job
                self.everything["job_executing"] = False

    # Called my Main thread
    def run_job(self, job):
        """Run single job."""
        self.everything["job_executing"] = True
        self.everything["current_job"] = job
        job_id = job.job_id
        output_dir = Path(job.job_data["output_directory"])

        # Setup for the output directory
        if output_dir.exists():
            LOGGER.info("Removing existing output directory: %s", output_dir)
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=False)
        LOGGER.info("Created output directory: %s", output_dir)

        # Set up temporary directory for intermediate map outputs
        prefix = f"mapreduce-shared-job{job_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)

            input_dir = Path(job.job_data["input_directory"])
            input_files = sorted(input_dir.iterdir())
            LOGGER.info("Input files %s", input_files)

            num_mappers = job.job_data["num_mappers"]
            partitions = [[] for _ in range(num_mappers)]
            for i, input_file in enumerate(input_files):
                partitions[i % num_mappers].append(str(input_file))
            LOGGER.info("Created partitions %s", partitions)

            self.mapping_tasks(partitions, tmpdir, job)

            # Wait for map tasks to complete, but check for shutdown signal
            while self.everything["num_map_tasks"] > 0:
                if self.signals["shutdown"]:
                    LOGGER.info("Shutdown received. Skipping reduce phase.")
                    return  # Exit early if shutdown is active
                time.sleep(1)

            # Only proceed with reduce if no shutdown signal
            if not self.signals["shutdown"]:
                self.reduce_tasks(partitions, tmpdir, job)

                # Wait for reduce tasks to complete
                while self.everything["num_reduce_tasks"] > 0:
                    if self.signals["shutdown"]:
                        LOGGER.info("Shutdown received during reduce tasks.")
                        return  # Exit if shutdown occurs during reduce phase
                    time.sleep(1)

        LOGGER.info("Cleaned up tmpdir %s", tmpdir)
        self.everything["job_executing"] = False

    # Called by Main thread
    # QUESTION: does this thing ever stop??
    def process_jobs(self):
        """Process jobs."""
        while not self.signals["shutdown"]:
            if (
                self.job_queue.qsize() > 0
                and not self.everything["job_executing"]
            ):
                job = self.job_queue.get(timeout=1)
                LOGGER.info("Starting job %d", job.job_id)
                self.run_job(job)
            # Check shutdown signal if triggered while processing jobs
            if self.signals["shutdown"]:
                LOGGER.info(
                    "Shutdown signal received. Exiting process_jobs loop."
                )
                break
        LOGGER.info("Stopping job processing due to shutdown.")

    # Called by Main thread
    def mapping_tasks(self, partitions, tmpdir, job):
        """Assign map tasks to workers as they become available."""
        self.everything["stage"] = "map"
        self.everything["num_map_tasks"] = len(partitions)
        self.everything["initial_map_tasks"] = self.everything["num_map_tasks"]

        for task_id, partition in enumerate(partitions):
            task_data = {
                "message_type": "new_map_task",
                "task_id": task_id,
                "input_paths": partition,
                "executable": job.job_data["mapper_executable"],
                "output_directory": str(tmpdir),
                "num_partitions": job.job_data["num_reducers"],
            }
            job.add_task(task_data)
        self.send_message(job)

    def reduce_tasks(self, partitions, tmpdir, job):
        """Assign reduce tasks to workers as they become available."""
        self.everything["stage"] = "reduce"
        self.everything["num_reduce_tasks"] = job.job_data["num_reducers"]

        # Generate correct input paths from map task outputs for reduce tasks
        partitions = [
            [
                f"{tmpdir}/maptask{map_task_id:05d}-part{reduce_part_id:05d}"
                for map_task_id in range(self.everything["initial_map_tasks"])
            ]
            for reduce_part_id in range(self.everything["num_reduce_tasks"])
        ]

        # Log the generated partitions for debugging
        LOGGER.info("Generated partitions for reduce tasks: %s", partitions)

        for task_id, partition in enumerate(partitions):
            task_data = {
                "message_type": "new_reduce_task",
                "task_id": task_id,
                "executable": job.job_data["reducer_executable"],
                "input_paths": partition,
                "output_directory": job.job_data["output_directory"],
            }
            LOGGER.info(
                "Reduce task %d created: %s", task_id, task_data
            )
            job.add_task(task_data)
        LOGGER.info(
            "All reduce tasks queued. Sending to workers."
        )
        self.send_message(job)

    def send_message(self, job):
        """Send message."""
        while not self.signals["shutdown"] and not job.tasks.empty():
            # Check if there are any available workers
            available_worker = next(
                (w for w in self.workers.values() if w.state == "ready"),
                None
            )
            # gets the first available worke following order of registration
            if available_worker is None:
                # If no workers are available, wait and check again
                time.sleep(1)
                continue

            # Assign a task to the available worker
            task = job.next_task()
            if task:
                try:
                    # Update worker state to busy and send task
                    available_worker.state = "busy"
                    available_worker.current_task = task
                    try:
                        tcp_client(
                            available_worker.host,
                            available_worker.port,
                            task
                        )
                        LOGGER.info(
                            "Assigned task %s to worker %s",
                            task['task_id'],
                            available_worker.worker_id
                        )
                    except ConnectionRefusedError:
                        available_worker.state = 'dead'
                except socket.error:
                    # If there's an error sending the task, mark worker as dead
                    LOGGER.error(
                        "Failed to send task to worker %s:%s. Marking as dead",
                        available_worker.host,
                        available_worker.port
                    )
                    available_worker.mark_as_dead()
                    job.task_reset(task)  # Requeue the task for another worker

# QUESTION: Is this ever called??
#    def wait_for_task_completion(self, task):
#        """Wait until the task is completed by the worker."""
#    while any(w.current_task == task and
#         w.state == "busy" for w in self.workers.values()):
#         time.sleep(0.5)  # Check every 0.5 seconds if the task is completed


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
