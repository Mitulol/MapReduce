import os
import tempfile
import logging
import json
import time
import click
import threading
import socket

# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        self.port = port
        self.host = host
        self.signals = {"shutdown": False}
        self.workers = {}
        LOGGER.info("Starting manager host=%s port=%s pwd=%s", host, port, os.getcwd())

        self.threads = []
        self.threads.append(threading.Thread(target=self.listen_udp))
        self.threads.append(threading.Thread(target=self.fault_tolerance))

        for thread in self.threads:
            thread.start()

        self.listen_tcp()

        # Wait for all threads to finish
        for thread in self.threads:
            thread.join()


    def listen_tcp(self):
        """Listen for TCP connections to receive messages."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            sock.settimeout(1)

            while not self.signals["shutdown"]:
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)
                    message_bytes = b''.join(message_chunks)
                    message_str = message_bytes.decode("utf-8")
                    try:
                        message_dict = json.loads(message_str)
                        if message_dict.get("message_type") == "shutdown":
                            self.signals["shutdown"] = True
                            self.forward_shutdown()
                            break
                        elif message_dict.get("message_type") == "register":
                            self.handle_registration(message_dict)
                    except json.JSONDecodeError:
                        continue
                    print(message_dict)

    def listen_udp(self):
        """Listen for UDP messages for heartbeats."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.settimeout(1)
            while not self.signals["shutdown"]:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                print(message_dict)

    def fault_tolerance(self):
        """A placeholder for fault tolerance mechanism."""
        while not self.signals["shutdown"]:
            time.sleep(5)  # Example sleep time

    def forward_shutdown(self):
        """Forward shutdown message to all Workers."""
        # TODO: Implement forwarding shutdown messages to Workers.
        # This requires maintaining a list of worker connections or addresses.
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
        
        print("Shutting down all workers")

        # Add logic to forward the shutdown signal to all connected workers.
    def handle_registration(self, message_dict):
        """Handle registration messages from workers."""
        worker_host = message_dict["worker_host"]
        worker_port = message_dict["worker_port"]
        self.workers[(worker_host, worker_port)] = "ready"
        LOGGER.info("Worker registered: %s:%s", worker_host, worker_port)
        
        # Send registration acknowledgment
        ack_message = json.dumps({"message_type": "register_ack"}).encode("utf-8")
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((worker_host, worker_port))
            sock.sendall(ack_message)


@click.command()
@click.option("--host", default="localhost")
@click.option("--port", default=6000, type=int)
@click.option("--logfile", default=None)
@click.option("--loglevel", default="info")
@click.option("--shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Manager:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
