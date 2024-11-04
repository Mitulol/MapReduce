import os
import logging
import json
import time
import click
import threading
from mapreduce.utils.network import tcp_server
import socket

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
        self.shutdown_event = threading.Event()  # Event to handle shutdown

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
        while not self.signals['shutdown']:
            try:
                message = {
                    "message_type": "heartbeat",
                    "worker_host": self.host,
                    "worker_port": self.port,
                }
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                    sock.sendto(json.dumps(message).encode(), (self.manager_host, self.manager_port))
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
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.manager_host, self.manager_port))
                sock.sendall(register_message)
                LOGGER.debug("Sent registration message to manager")
        except Exception as e:
            LOGGER.error(f"Failed to register with manager: {e}")
        
        
    
    def handle_func(self, host, port, signals, message_dic):
        # Log the received message at DEBUG level in the specified format
        LOGGER.info(f"Worker:{port} [DEBUG] received\n{json.dumps(message_dic, indent=2)}")

        if message_dic.get("message_type") == "register_ack":
            LOGGER.info(f"Worker:{port} [INFO] Received register_ack from Manager. Starting heartbeat thread.")
            heartbeat_thread = threading.Thread(target=self.send_heartbeat)
            heartbeat_thread.start()
            self.threads.append(heartbeat_thread)
        if message_dic.get("message_type") == "new_map_task":
            print("HellooooQ")
        if message_dic.get("message_type") == "shutdown":
            self.signals["shutdown"] = True

            # Wait for any active task to finish before shutdown
            while self.task_in_progress:
                LOGGER.info("Waiting for current task to complete before shutdown.")
                time.sleep(1)

            # Log the shutdown event at INFO level in the specified format
            LOGGER.info(f"Worker:{port} [INFO] shutting down")
            self.shutdown_event.set() 
       


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
