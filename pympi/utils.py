"""Utility functions"""

import copy
from collections import abc

import net_queue as nq
from net_queue.core.comm import CommunicatorOptions
from net_queue.utils.stream import PickleSerializer

from pympi import proto, rc


__all__ = (
    "byteview",
    "comm_options"
)


def byteview(b: abc.Buffer) -> memoryview:
    """Return a byte view of a buffer"""
    with memoryview(b) as view:
        return view.cast("B")


def comm_options(base: CommunicatorOptions = CommunicatorOptions()):
    """Generate MPI specific communicator options"""
    netloc = nq.NetworkLocation(host=rc.addr, port=rc.port)
    serializer_allow = PickleSerializer.allow_by_name(*proto.SERIALIZABLE, *rc.serial) if rc.serial else None
    serializer = PickleSerializer(allow=serializer_allow)
    serialization = nq.SerializationOptions(load=serializer.load, dump=serializer.dump)
    connection = copy.replace(base.connection, message_size=rc.msg_size, queue_size=rc.queue_size)
    security = nq.SecurityOptions(key=rc.ssl_key, certificate=rc.ssl_cert) if rc.ssl else None
    return copy.replace(base, netloc=netloc, connection=connection, serialization=serialization, security=security)
