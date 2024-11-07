import os
import logging
import json
import time
import click
import threading
from mapreduce.utils.network import tcp_server, udp_server, tcp_client, udp_client # TODO: remove the ones you didnt use, and change code to make use of these
import socket
from pathlib import Path
import subprocess
import hashlib
import tempfile
from contextlib import ExitStack
import shutil

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
        self.task_in_progress = False  # Flag to indicate if a task is being processed
        # self.shutdown_event = threading.Event()  # Event to handle shutdown

        # Log initial worker info
        LOGGER.info(f"Worker:{port} Starting worker at {host}:{port}")
        LOGGER.info(f"Worker:{port} Connecting to manager at {manager_host}:{manager_port}")

        # Start TCP server to listen for messages
        self.threads = []
        
        self.threads.append(threading.Thread(target=tcp_server, args=(host, port, self.signals, self.handle_func)))
        # self.send_registration_message_to_manager()

        for thread in self.threads:
            thread.start()

        self.register_with_manager(host, port, self.signals)
        
        
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
    #         LOGGER.info(f"Worker:{self.port} Sent registration message to Manager at {self.manager_host}:{self.manager_port}")

    def send_heartbeat(self, *args):
        # QUESTION: again, we should use udp client?
        while not self.signals['shutdown']:
            try:
                message = {
                    "message_type": "heartbeat",
                    "worker_host": self.host,
                    "worker_port": self.port,
                }
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                    sock.sendto(json.dumps(message).encode(), (self.manager_host, self.manager_port))
                    # QUESTION: cool syntax. is this the same thing as doing all that stuff in 3 lines?
                time.sleep(2)  # Send heartbeat every 2 seconds
            except Exception as e:
                LOGGER.error("Error sending heartbeat: %s", e)
                break
            
    def register_with_manager(self, host, port, signal):
        try:
            register_message = json.dumps({
                "message_type": "register",
                "worker_host": self.host,
                "worker_port": self.port,
            }).encode("utf-8")
            # QUESTION: why not use the TCP Client function we made in network.py?
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.manager_host, self.manager_port))
                sock.sendall(register_message)
                LOGGER.debug("Sent registration message to manager")
        except Exception as e:
            LOGGER.error(f"Failed to register with manager: {e}")

    # The Mapping[worker] stage.
    def handle_task(self, task):
        """ Complete the assigned map task """
        # 1. Run the map executable on the specified input files.
        # 2. Partition output of the map executable into a new temporary directory, local to the Worker.
        # 3. Sort each output file by line using UNIX sort.
        # 4. Move each sorted output file to the shared temporary directory specified by the Manager.

        executable =  Path(task.get("executable")) # map executable

        # A list of input paths. []
        input_paths =  task.get("input_paths") # map input filename
        
        output_dir = Path(task.get("output_directory"))
        task_id = task.get("task_id")
        num_reducers = task.get("num_partitions")

        # 1. create a temp directory local to the worker.
        prefix = f"mapreduce-local-task{task.get("task_id"):05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Worker:%d [INFO] Created tmpdir %s", self.port, tmpdir)
            temp_dir_path = Path(tmpdir)
            partition_files = [temp_dir_path/f"maptask{task_id:05d}-part{partition:05d}" for partition in range(num_reducers)]

            # A list of the open file objects
            # ExitStack() allows me to open multiple context maagers without having a crazy amount of with blocks
            # Refer to ExitStack() and contextlib documentation
            with ExitStack() as stack:
                # Open files in append mode. This creates the files if they don't exist,
                # and I don't have to worry about my data getting overwritten.
                partition_files_context = [stack.enter_context(open(partition_file, 'a')) for partition_file in partition_files]
            # with [open(file, "w") for file in partition_files] as partition_handles:
                
                # Loop through the input files
                for input_path in input_paths:
                    
                    # Open each input file and run the provided map executable
                    with open(input_path) as infile:
                        with subprocess.Popen(
                            [executable],
                            stdin=infile,
                            stdout=subprocess.PIPE,
                            text=True,
                        ) as map_process:

                            # Loop through the output of the map line by line
                            # This is the output of 1 input file.
                            for line in map_process.stdout:
                                # Add line to correct partition output file
                                key, value = line.split("\t")

                                hexdigest = hashlib.md5(key.encode("utf-8")).hexdigest()
                                keyhash = int(hexdigest, base=16)
                                partition_number = keyhash % num_reducers

                                # Add the line to the correct parition file.
                                # TODO: check if I need to write an extra /n at the end.
                                # Just manually write Key\tvalue\n in that case
                                partition_files_context[partition_number].write(line)

                    # Log for each input file
                    LOGGER.info(f"Worker:{self.port} [INFO] Executed {executable} input={input_path}")

            # Close ExitStack here. Do the sorts outside the scope of the ExitStack
            # Step 3: sort the partition files
            for file in partition_files:
                # LOGGER.info(f"Before sorting: File {partition_file}")

                # TODO: REMOVE THIS AFTER DEBUGGING!!!
                # Print the file contents before sorting
                # with open(partition_file, 'r') as file:
                #     file_contents = file.read()
                #     LOGGER.info(f"Before sorting: Contents of {partition_file}:\n{file_contents}")

                # partition_file_path = Path(partition_file)
                subprocess.run(["sort", "-o", file, file], check=True)

                # Print the file contents after sorting
                # with open(partition_file, 'r') as file:
                #     file_contents = file.read()
                #     LOGGER.info(f"After sorting: Contents of {partition_file}:\n{file_contents}")

                LOGGER.info(f"Worker:{self.port} [INFO] Sorted {file}")
            
            # Step 4: move the partition files to the output directory
            for partition_file in partition_files:

                # extra sanity code to make sure the name stays the same in the output_dir, not necessary.
                shutil.move(partition_file, output_dir / partition_file.name)
                LOGGER.info(f"Worker:{self.port} [INFO] Moved {partition_file} -> {output_dir / partition_file.name}")


            # Step 5: Send message
            finished_message = {
                "message_type": "finished",
                "task_id": task_id,
                "worker_host": self.host,
                "worker_port": self.port
            }

            tcp_client(self.manager_host, self.manager_port, finished_message)


            LOGGER.info(f"Worker:{self.port} [INFO] Removed {temp_dir_path}")
        # tempdir with ends here

    def handle_func(self, host, port, signals, message_dic):
        # Log the received message at DEBUG level in the specified format
        LOGGER.info(f"Worker:{port} [DEBUG] received\n{json.dumps(message_dic, indent=2)}")

        if message_dic.get("message_type") == "register_ack":
            LOGGER.info(f"Worker:{port} [INFO] Received register_ack from Manager. Starting heartbeat thread.")
            heartbeat_thread = threading.Thread(target=self.send_heartbeat)
            heartbeat_thread.start()
            self.threads.append(heartbeat_thread)
        if message_dic.get("message_type") == "new_map_task":
            # print("HellooooQ")
            self.handle_task(message_dic)
        if message_dic.get("message_type") == "shutdown":
            self.signals["shutdown"] = True

            # Wait for any active task to finish before shutdown
            while self.task_in_progress:
                LOGGER.info("Waiting for current task to complete before shutdown.")
                time.sleep(1) # QUESTION: why is this here? To let the current task enough time to finish?

            # Log the shutdown event at INFO level in the specified format
            LOGGER.info(f"Worker:{port} [INFO] shutting down")
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
