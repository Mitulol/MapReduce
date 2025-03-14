"""Network."""
import socket
import logging
import json

# Set up logger
LOGGER = logging.getLogger(__name__)


def tcp_server(host, port, signals, handle_func):
    """Listen for messages and calls handle_func when message received."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen()
        LOGGER.info("TCP Server listening on %s:%s", host, port)
        sock.settimeout(1)

        while not signals["shutdown"]:
            try:
                clientsocket, _ = sock.accept()
            except socket.timeout:
                continue
            # LOGGER.info(f"Connection from {address}")

            clientsocket.settimeout(1)
            with clientsocket:
                message_chunks = []
                while True:
                    try:
                        data = clientsocket.recv(4096)
                        if data:
                            LOGGER.debug("Received raw data: %s", data)
                    except socket.timeout:
                        continue
                    if not data:
                        break
                    message_chunks.append(data)

            message_bytes = b''.join(message_chunks)
            message_str = message_bytes.decode("utf-8")
            LOGGER.info("message_str: %s", message_str)

            # try:
            # message_dict = json.loads(message_str)
            # Log the received message at DEBUG level
            # in the exact format specified
            # except json.JSONDecodeError as e:
            #     continue
            try:
                message_dict = json.loads(message_str)
                LOGGER.info("This is the message dict:\n%s",
                            json.dumps(message_dict, indent=2))
                LOGGER.debug("%s:%s [DEBUG] received\n%s", host,
                             port, json.dumps(message_dict, indent=2))
            except json.JSONDecodeError:
                continue

            # Call the handler function with the received message
            # handle_func(host, port, signals, message_dict)
            handle_func(port, message_dict)


def udp_server(host, port, signals, handle_func):
    """Listen over UDP and calls handle_func when a msg is received."""
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
                # Log received message at DEBUG in exact format specified
                LOGGER.debug("%s:%s received", host, port)
                LOGGER.debug(json.dumps(message_dict, indent=2))

            except json.JSONDecodeError:
                LOGGER.error("Failed to decode JSON message")
                continue

            # Call the handler function with the received message
            handle_func(port, message_dict)


def tcp_client(host, port, msg):
    """Send over tcp."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

        # connect to the server
        sock.connect((host, port))
        message = json.dumps(msg).encode("utf-8")
        sock.sendall(message)


def udp_client(host, port, msg):
    """Send over udp."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

        # Connect to the UDP socket on server
        sock.connect((host, port))
        message = json.dumps(msg).encode("utf-8")
        sock.sendall(message)
