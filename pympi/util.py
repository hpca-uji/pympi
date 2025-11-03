import copy

import net_queue as nq
from net_queue import CommunicatorOptions
from net_queue.stream import PickleSerializer
from pympi import proto, rc


def comm_options(base: CommunicatorOptions = CommunicatorOptions()):
    """Generate MPI specific comunicator options"""
    netloc = nq.NetworkLocation(host=rc.addr, port=rc.port)
    serialization_restrict = (*proto.SERIALIZABLE, *rc.serial) if rc.serial else None
    serialization = nq.SerializationOptions(max_size=rc.serial_size, load=PickleSerializer(restrict=serialization_restrict).load)
    security = nq.SecurityOptions(key=rc.ssl_key, certificate=rc.ssl_cert) if rc.ssl else None
    return copy.replace(base, netloc=netloc, serialization=serialization, security=security)
