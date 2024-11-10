"""MapReduce framework Worker node."""
import logging
import json
import time
import threading
from pathlib import Path
import subprocess
import hashlib
import tempfile
from contextlib import ExitStack
import shutil
import heapq

import click

from mapreduce.utils.network import tcp_server, tcp_client, udp_client
# DONE: remove the ones you didn't use, and change code to make use of these

# DONE: Use Worker.task_in_progress boolean. I am not using it rn.
# DONE: Add checks for shutdown signal every where

# Set up logger
LOGGER = logging.getLogger(__name__)


class Worker:
    """Represents a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        """Initialize a Worker instance and start listening for messages."""
        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.signals = {"shutdown": False}
        # self.task_in_progress = False
        # self.shutdown_event = threading.Event()  # Event to handle shutdown

        # Log initial worker info
        LOGGER.info("Worker:%d Starting worker at %s:%d", port, host, port)
        LOGGER.info(
            "Worker:%d connecting to %s:%d", port, manager_host, manager_port
        )

        # Start TCP server to listen for messages
        self.threads = []

        self.threads.append(
            threading.Thread(
                target=tcp_server,
                args=(host, port, self.signals, self.handle_func)
            )
        )
        # self.send_registration_message_to_manager()

        for thread in self.threads:
            thread.start()
        self.register_with_manager()

        for thread in self.threads:
            thread.join()
    # def send_registration_message_to_manager(self):
    #     message_dict = {
    #         "message_type": "register",
    #         "worker_host": self.host,
    #         "worker_port": self.port,
    #     }
    #     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    #         sock.connect((self.manager_host, self.manager_port))
    #         sock.sendall(json.dumps(message_dict).encode())
    # LOGGER.info(f"Worker:{self.port} Sent registration message to Manager at
    # {self.manager_host}:{self.manager_port}")

    def send_heartbeat(self):
        """Send heartbeat to manager."""
        # QUESTION: again, we should use udp client?

        # Sending heartbeat while worker isn't shutdown
        while not self.signals['shutdown']:

            heartbeat_message = {
                    "message_type": "heartbeat",
                    "worker_host": self.host,
                    "worker_port": self.port,
                }
            udp_client(self.manager_host, self.manager_port, heartbeat_message)
            time.sleep(2)

            # try:
            #     message = {
            #         "message_type": "heartbeat",
            #         "worker_host": self.host,
            #         "worker_port": self.port,
            #     }
            # with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            #         sock.sendto(json.dumps(message).encode(),
            #           (self.manager_host, self.manager_port))
            # Q:is this the same thing as doing all that stuff in 3 lines?
            #     time.sleep(2)  # Send heartbeat every 2 seconds
            # except Exception as e:
            #     LOGGER.error("Error sending heartbeat: %s", e)
            #     break

    def register_with_manager(self):
        """Register with manager."""
        register_message = {
            "message_type": "register",
            "worker_host": self.host,
            "worker_port": self.port
        }
        tcp_client(self.manager_host, self.manager_port, register_message)

        # try:
        #     register_message = json.dumps({
        #         "message_type": "register",
        #         "worker_host": self.host,
        #         "worker_port": self.port,
        #     }).encode("utf-8")
        # Q: why not use the TCP Client function we made in network.py?
        #     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        #         sock.connect((self.manager_host, self.manager_port))
        #         sock.sendall(register_message)
        #         LOGGER.debug("Sent registration message to manager")
        # except Exception as e:
        #     LOGGER.error("Failed to register with manager: %s", e)

    # The Mapping[worker] stage.
    def handle_map_task(self, task):
        """Complete the assigned map task."""
        # 1. Run the map executable on the specified input files.
        # 2. Partition output of the map executable into a new temporary
        # directory, local to the Worker.
        # 3. Sort each output file by line using UNIX sort.
        # 4. Move each sorted output file to the shared
        # temporary directory specified by the Manager.

        # executable = Path(task.get("executable"))

        # A list of input paths. []
        # input_paths = task.get("input_paths")

        # output_dir = Path(task.get("output_directory"))
        # task_id = task.get("task_id")
        # num_reducers = task.get("num_partitions")
        # 1. create a temp directory local to the worker.
        # prefix = f"mapreduce-local-task{task.get("task_id"):05d}-"
        with tempfile.TemporaryDirectory(
            prefix=f"mapreduce-local-task{task.get('task_id'):05d}-"
        ) as tmpdir:
            # with tempfile.TemporaryDirectory(prefix=
            #     f"mapreduce-local-task{task.get("task_id"):05d}-") as tmpdir:
            LOGGER.info(
                "Worker:%d [INFO] Tmpdir created at %s", self.port, tmpdir
            )
            # temp_dir_path = Path(tmpdir)
            partition_files = [
                Path(tmpdir) /
                f"maptask{task.get("task_id"):05d}-part{partition:05d}"
                for partition in range(task.get("num_partitions"))
            ]

            # A list of the open file objects
            # ExitStack() allows me to open multiple context maagers
            #  without having a crazy amount of with blocks
            # Refer to ExitStack() and contextlib documentation
            with ExitStack() as stack:
                # Open files in append mode.
                # This creates files if they don't exist
                # and I don't have to worry about
                # my data getting overwritten.
                partition_files_context = [
                    stack.enter_context(open(
                        partition_file, 'w', encoding='utf-8'))
                    for partition_file in partition_files
                ]
                # Loop through the input files
                for input_path in task.get("input_paths"):
                    # Open each input file + run map executable
                    with open(input_path, encoding='utf-8') as infile:
                        with subprocess.Popen(
                            [Path(task.get("executable"))],
                            stdin=infile,
                            stdout=subprocess.PIPE,
                            text=True,
                        ) as map_process:
                            # This is the output of 1 input file.
                            for line in map_process.stdout:
                                # Add line to correct partition output file
                                key, value = line.split("\t")

                                # hexdigest = hashlib.md5(
                                #     key.encode("utf-8")
                                # ).hexdigest()
                                keyhash = int(hashlib.md5(
                                    key.encode("utf-8")
                                ).hexdigest(), base=16)
                                p_n = keyhash % task.get("num_partitions")
                                partition_files_context[p_n].write(
                                    f"{key}\t{value}"
                                )
                    # Log for each input file
                    LOGGER.info("Worker:%d [INFO] Executed %s input=%s",
                                self.port,
                                Path(task.get("executable")), input_path)

            # Close ExitStack here.
            # Do the sorts outside the scope of the ExitStack
            # Step 3: sort the partition files
            for partition_file in partition_files:
                # LOGGER.info(f"Before sorting: File {partition_file}")

                # DONE: REMOVE THIS AFTER DEBUGGING!!!
                # Print the file contents before sorting
                # with open(partition_file, 'r') as file:
                #     file_contents = file.read()
                # LOGGER.info(f"Before sorting:
                # Contents of {partition_file}:\n{file_contents}")

                # partition_file_path = Path(partition_file)
                subprocess.run(
                    ["sort", "-o", partition_file, partition_file],
                    check=True
                )

                shutil.move(partition_file,
                            Path(task.get("output_directory")) /
                            partition_file.name)
                LOGGER.info("Worker:%d [INFO] Moved %s -> %s",
                            self.port, partition_file,
                            Path(task.get("output_directory")) /
                            partition_file.name)

                # Print the file contents after sorting
                # with open(partition_file, 'r') as file:
                #     file_contents = file.read()

            # Step 4: move the partition files to the output directory
            # for partition_file in partition_files:

            # make sure name stays same in output_dir, not necessary.
            # shutil.move(partition_file, output_dir / partition_file.name)
            # LOGGER.info(f"Worker:{self.port} [INFO]
            # Moved {partition_file} -> {output_dir /partition_file.name}")

            # Step 5: Send message
            # finished_message = {
            #     "message_type": "finished",
            #     "task_id": task.get("task_id"),
            #     "worker_host": self.host,
            #     "worker_port": self.port
            # }

            tcp_client(
                self.manager_host, self.manager_port,
                {
                    "message_type": "finished",
                    "task_id": task.get("task_id"),
                    "worker_host": self.host,
                    "worker_port": self.port
                }
            )

        # tempdir with ends here
        LOGGER.info("Worker:%d [INFO] Removed %s", self.port, Path(tmpdir))

    def handle_reduce_task(self, task):
        """Complete the assigned reduce task.

        1. Merge input files into one sorted output stream.
        2. Run the reduce executable on merged input,
            writing output to a single file.
        3. Move the output file to the final output
            directory specified by the Manager.
        Each input file should already be sorted from the Map Stage.
        """
        # Reduce executable
        executable = Path(task.get("executable"))

        # A list of input paths. []
        input_paths = task.get("input_paths")

        # output_dir = Path(task.get("output_directory"))
        task_id = task.get("task_id")

        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            # LOGGER.info("Worker: [INFO] Created tmpdir %s", tmpdir)
            temp_dir_path = Path(tmpdir)

            # partition number = task_id TODO: verify. Most likely
            temp_output_file = temp_dir_path/f"part-{task_id:05d}"

            with open(temp_output_file, "a", encoding="utf-8") as outfile:
                with ExitStack() as stack:
                    input_files = [
                        stack.enter_context(open(path, encoding="utf-8"))
                        for path in input_paths
                    ]

                    with subprocess.Popen(
                        [executable],
                        text=True,
                        stdin=subprocess.PIPE,
                        stdout=outfile,
                    ) as reduce_process:

                        for line in heapq.merge(*input_files):
                            reduce_process.stdin.write(line)

                        reduce_process.stdin.close()
                        reduce_process.wait()
            LOGGER.info("Worker:%d [INFO] Executed %s", self.port, executable)
            # Outside the context of temp output file
            # Move the output file to the final output directory
            shutil.move(
                temp_output_file,
                Path(task.get("output_directory")) / temp_output_file.name
            )
            # destination = output_dir / temp_output_file.name
            LOGGER.info(
                "Worker:%d [INFO] %s -> %s",
                self.port,
                temp_output_file,
                Path(task.get("output_directory")) / temp_output_file.name
            )

            finished_message = {
                "message_type": "finished",
                "task_id": task_id,
                "worker_host": self.host,
                "worker_port": self.port
            }

            tcp_client(self.manager_host, self.manager_port, finished_message)

        # tempdir with block ends here
        LOGGER.info("Worker:%d [INFO] Removed %s", self.port, temp_dir_path)

    def handle_func(self, port, message_dic):
        """Handle incoming messages from port."""
        # Log the received message at DEBUG level in the specified format
        LOGGER.debug(
            "Worker:%d msg:\n%s", port, json.dumps(message_dic, indent=2)
        )

        if message_dic.get("message_type") == "register_ack":
            LOGGER.info("Worker:%d registered. Heartbeat started.", port)
            heartbeat_thread = threading.Thread(target=self.send_heartbeat)
            heartbeat_thread.start()
            self.threads.append(heartbeat_thread)
        if message_dic.get("message_type") == "new_map_task":
            # print("HellooooQ")
            self.handle_map_task(message_dic)
        if message_dic.get("message_type") == "new_reduce_task":
            self.handle_reduce_task(message_dic)
        if message_dic.get("message_type") == "shutdown":
            self.signals["shutdown"] = True
            # Stops listening to messages

            # Overkill, shutdown message and tasks message
            # are handled in the same thread
            # means worker won't process shutdown message
            # until idone with previous task

            # Wait for any active task to finish before shutdown
            # while self.task_in_progress:
            #     time.sleep(1)

            # Log the shutdown event at INFO level in the specified format
            # LOGGER.info(f"Worker:{port} [INFO] shutting down")
            # self.shutdown_event.set() # QUESTION: what is this????


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
