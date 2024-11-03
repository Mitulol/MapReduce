"""Utils package.

This package is for code shared by the Manager and the Worker.
"""
from mapreduce.utils.ordered_dict import ThreadSafeOrderedDict
from mapreduce.utils.network import tcp_server,udp_client,udp_server,tcp_client,handle_func
