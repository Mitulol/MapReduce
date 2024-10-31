import socket
import threading
import json
import logging
import click

class Worker:
    def __init__(self, host, port, manager_host, manager_port):
        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port

        # Setup Worker server socket with context manager
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as worker_socket:
            worker_socket.bind((host, port))
            worker_socket.listen(1)
            self.worker_socket = worker_socket

            self.connect_to_manager()

    def connect_to_manager(self):
        with socket.create_connection((self.manager_host, self.manager_port)) as connection:
            self.send_registration(connection)

    def send_registration(self, connection):
        message = json.dumps({"message_type": "register", "worker_host": self.host, "worker_port": self.port})
        connection.send(message.encode())
        logging.info("Registration sent")

    def listen_for_messages(self):
        while True:
            with self.worker_socket.accept() as (client_socket, _):
                try:
                    data = client_socket.recv(1024)
                    if data:
                        message = json.loads(data)
                        logging.info(f"Message from Manager: {message}")
                except json.JSONDecodeError:
                    continue

@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001, type=int)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000, type=int)
def main(host, port, manager_host, manager_port):
    logging.basicConfig(level=logging.INFO)
    worker = Worker(host, port, manager_host, manager_port)

if __name__ == "__main__":
    main()
