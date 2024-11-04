import socket
import logging
import json
import time

# Set up logger
LOGGER = logging.getLogger(__name__)

def tcp_server(host, port, signals, handle_func):
    """Continuously listens for messages and calls handle_func when a message is received."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen()
        LOGGER.info(f"TCP Server listening on {host}:{port}")
        sock.settimeout(1)

        while not signals["shutdown"]:
            try:
                clientsocket, address = sock.accept()
            except socket.timeout:
                continue
            LOGGER.info(f"Connection from {address}")

            clientsocket.settimeout(1)
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
                # Log the received message at DEBUG level in the exact format specified
                LOGGER.debug(f"{host}:{port} [DEBUG] received\n{json.dumps(message_dict, indent=2)}")
            except json.JSONDecodeError:
                LOGGER.error("Failed to decode JSON message")
                continue

            # Call the handler function with the received message
            handle_func(host, port, signals, message_dict)
            

def udp_server(host, port, signals, handle_func):
    """Listens for messages over UDP and calls handle_func when a message is received."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.settimeout(1)

        while not signals["shutdown"]:
            try:
                message_bytes = sock.recv(4096)
            except socket.timeout:
                continue
            message_str = message_bytes.decode("utf-8")

            try:
                message_dict = json.loads(message_str)
                # Log the received message at DEBUG level in the exact format specified
                LOGGER.info(f"{host}:{port} [DEBUG] received\n{json.dumps(message_dict, indent=2)}")
            except json.JSONDecodeError:
                LOGGER.error("Failed to decode JSON message")
                continue

            # Call the handler function with the received message
            handle_func(host, port, signals, message_dict)

