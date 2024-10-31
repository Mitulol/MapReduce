import socket
import threading
import json
import logging
import click
import tempfile

class Manager:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.active = True
        self.client_threads = []

        # Setup TCP server socket with context manager
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((host, port))
            server_socket.listen()
            self.server_socket = server_socket

            logging.info(f"Manager started at {host}:{port}")
            self.listen_thread = threading.Thread(target=self.accept_clients)
            self.listen_thread.start()

    def accept_clients(self):
        while self.active:
            
            with self.server_socket.accept() as (client_socket, addr):
                logging.info(f"Accepted connection from {addr}")
                thread = threading.Thread(target=self.handle_client, args=(client_socket,))
                thread.start()
                self.client_threads.append(thread)

    def handle_client(self, client_socket):
        try:
            while self.active:
                data = client_socket.recv(1024)
                if data:
                    try:
                        message = json.loads(data)
                        if message.get('message_type') == 'shutdown':
                            self.shutdown()
                            break
                    except json.JSONDecodeError:
                        logging.warning("Received invalid JSON")
        finally:
            client_socket.close()

    def shutdown(self):
        self.active = False
        self.server_socket.close()
        for thread in self.client_threads:
            thread.join()
        logging.info("Manager has been shut down")


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