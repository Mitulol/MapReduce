"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import threading
from mapreduce.utils import tcp_server, handle_func, udp_heartbeat
import socket


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.signals = {
            "shutdown": False
        }
        # LOGGER.info(
        #     "Starting worker host=%s port=%s pwd=%s",
        #     host, port, os.getcwd(),
        # )
        # LOGGER.info(
        #     "manager_host=%s manager_port=%s",
        #     manager_host, manager_port,
        # )

        #create new tcp socket on given port and call listen ->only one should remain open ->use tcp_server here
        #listen and send message to manager
        self.register_worker()
        self.threads.append(threading.Thread(target=tcp_server(host, port, self.signals, handle_func))) # listens to heartbeats 
        
        #self.threads.append(threading.Thread(target=udp_heartbeat(host, port, self.signals, handle_func))) # listens to heartbeats 

        # message_dict = {
        #     "message_type": "register_ack",
        #     "worker_host": "localhost",
        #     "worker_port": 6001,
        # }
        # LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        for thread in self.threads:
            thread.start()

        for thread in self.threads:
           thread.join()

    def register_worker(self):
        message_dict = {
            "message_type" : "register",
            "worker_host" : self.host,
            "worker_port" : self.post,
        }
        self.send_registration_message_to_manager(self, message_dict)

    def send_registration_message_to_manager(self, message_dict):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            msg = json.dumps(message_dict)
            sock.sendall(msg.encode())
        


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