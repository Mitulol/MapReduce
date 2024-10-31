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
        self.active = True  # To control the loop in case of shutdown

        # Start socket within a context manager
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((host, port))
            server_socket.listen()  # Listening for a single connection at a time

            self.server_socket = server_socket
            logging.info(f"Worker started at {host}:{port}")

            # Register with the Manager
            self.register_with_manager()

            # Start a thread to listen for Manager messages
            self.listen_thread = threading.Thread(target=self.listen_for_messages)
            self.listen_thread.start()
            self.listen_thread.join()  # Ensure the thread completes before closing

    def register_with_manager(self):
        """Send a registration message to the Manager."""
        try:
            with socket.create_connection((self.manager_host, self.manager_port)) as s:
                message = json.dumps({
                    "message_type": "register",
                    "worker_host": self.host,
                    "worker_port": self.port
                })
                s.send(message.encode())
                logging.info("Registration sent to Manager")
        except Exception as e:
            logging.error(f"Failed to register with Manager: {e}")

    def listen_for_messages(self):
        """Listen for incoming messages, such as 'shutdown', from the Manager."""
        while self.active:
            client_socket, addr = self.server_socket.accept()
            logging.info(f"Accepted connection from {addr}")
            with client_socket:
                try:
                    data = client_socket.recv(1024)
                    if data:
                        message = json.loads(data.decode())
                        logging.info(f"Message from Manager: {message}")
                        if message.get("message_type") == "shutdown":
                            logging.info("Shutdown message received")
                            self.shutdown()
                            break  # Exit the loop after shutdown
                except json.JSONDecodeError:
                    logging.warning("Received invalid JSON message")

    def shutdown(self):
        """Shut down the Worker cleanly."""
        self.active = False
        logging.info("Worker has been shut down")


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001, type=int)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000, type=int)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    # Configure logging
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(getattr(logging, loglevel.upper()))

    # Start Worker
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
