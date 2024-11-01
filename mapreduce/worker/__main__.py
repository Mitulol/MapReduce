import os
import logging
import json
import socket
import threading
import click
import time

# Configure logging
LOGGER = logging.getLogger(__name__)

class Worker:
    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.shutdown_event = threading.Event()
        self.threads = []
        
        LOGGER.info(f"Starting worker host={self.host} port={self.port}")
        LOGGER.info(f"PWD {os.getcwd()}")
        
        self.register_with_manager()
        self.start_tcp_listener()

    def register_with_manager(self):
        """Register the Worker with the Manager."""
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

    def start_tcp_listener(self):
        """Starts a thread that listens for commands from the Manager."""
        listener_thread = threading.Thread(target=self.listen_for_commands)
        self.threads.append(listener_thread)
        listener_thread.start()

    def listen_for_commands(self):
        """Listens for commands from the Manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            while not self.shutdown_event.is_set():
                try:
                    client_socket, _ = sock.accept()
                    self.handle_client(client_socket)
                except socket.error as e:
                    if not self.shutdown_event.is_set():
                        LOGGER.error(f"Error accepting connections: {e}")

    def handle_client(self, client_socket):
        with client_socket:
            while not self.shutdown_event.is_set():
                data = client_socket.recv(4096)
                if not data:
                    break
                try:
                    message = json.loads(data.decode('utf-8'))
                    LOGGER.debug(f"Received message: {json.dumps(message, indent=2)}")
                    if message.get('message_type') == 'shutdown':
                        self.shutdown_event.set()
                except json.JSONDecodeError:
                    continue

    def shutdown(self):
        """Signals all threads to shut down and waits for them to finish."""
        self.shutdown_event.set()
        for thread in self.threads:
            thread.join()

@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f'Worker:{port} [%(levelname)s] %(message)')
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)
    LOGGER.setLevel(loglevel.upper())

    worker = Worker(host, port, manager_host, manager_port)
    try:
        while not worker.shutdown_event.is_set():
            time.sleep(1)
    finally:
        worker.shutdown()

if __name__ == "__main__":
    main()
