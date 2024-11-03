#shared code goes here
import socket
import logging
import json
import threading
from mapreduce.worker import register_worker, send_registration_message_to_manager

LOGGER = logging.getLogger(__name__)

def tcp_server(host, port, signals, handle_func):
    #function that continuously listens for messages on a socket
    # and calls a callback handle_func function when a message is received.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen()
        LOGGER.info("TCP Server listening on %s:%s", host, port)

        # Socket accept() will block for a maximum of 1 second.  If you
        # omit this, it blocks indefinitely, waiting for a connection.
        sock.settimeout(1)


        while not signals["shutdown"]:
            try:
                clientsocket, address = sock.accept()
            except socket.timeout:
                continue
            print("Connection from", address[0])

            # Socket recv() will block for a maximum of 1 second.  If you omit
            # this, it blocks indefinitely, waiting for packets.
            clientsocket.settimeout(1)

            # Receive data, one chunk at a time.  If recv() times out before we
            # can read a chunk, then go back to the top of the loop and try
            # again.  When the client closes the connection, recv() returns
            # empty data, which breaks out of the loop.  We make a simplifying
            # assumption that the client will always cleanly close the
            # connection.
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

            # Decode list-of-byte-strings to UTF8 and parse JSON data
            message_bytes = b''.join(message_chunks)
            message_str = message_bytes.decode("utf-8")

            try:
                message_dict = json.loads(message_str)
            except json.JSONDecodeError:
                continue
            LOGGER.info(message_dict)

            # if message_dict["message_type"] == "shutdown":
            #     #do the forwarding and stuff 
            #do all the different actions here to account for diff messages
            

def udp_server(host, port, signals, handle_func):
    #unction that continuously listens for messages on a socket
    # and calls a callback “handler” function when a message is received.
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

        # Bind the UDP socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.settimeout(1)

        # No sock.listen() since UDP doesn't establish connections like TCP
        # Receive incoming UDP messages
        while not signals["shutdown"]:
            try:
                message_bytes = sock.recv(4096)
            except socket.timeout:
                continue
            message_str = message_bytes.decode("utf-8")
            message_dict = json.loads(message_str)
            print(message_dict)

#basically decoding/exception handling
def handle_func(msg):
    # try:
    #     msg = json.loads(msg)
    # except JSONDecodeError:
    #     continue




#functions that can be reused to send messages over the network.
def tcp_client():
def udp_heartbeat(): 
    #send heaartbeat