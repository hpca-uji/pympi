# pympi
Python-based MPI implementation for research and experimentation

## Example
```python
# example.py
from pympi import MPI
comm = MPI.COMM_WORLD
size = comm.size
rank = comm.rank

# Synchronization
comm.barrier()

# Collective
message = comm.bcast("Hello, World!")

ranks = comm.allgather(rank)

# Point-to-point
match rank:
    case 0:
        comm.send("Hello, 1!", dest=1)
    case 1:
        comm.recv(source=1)

# Nonblocking
req = comm.ireduce(rank)

result = req.wait()

# Buffer-based
buffer = bytearray(10)

comm.Allreduce(MPI.IN_PLACE, buffer)
```

Execute with 2 processes:
```bash
python -m pympi -np 2 python example.py
```

Note: `mpirun`, `srun` and other job managers can also be used*

## Benchmark

| Configuration | Used |
|-|-|
| OS | Debian GNU/Linux 13 (trixie) |
| CPU | 13th Gen Intel® Core™ i5-13400 × 16 |
| RAM | 64 GB |

| Test | Transfer | Operations | Executed |
|-|-|-|-|
| Sync | 2.1GB | 500.0 | PYMPI_PROTO=proto mpirun -np 8 python test/iops.py sync --step-size 0 --max-size 21 --reps 500 |
| Async | 2.1GB | 500.0 | PYMPI_PROTO=proto mpirun -np 8 python test/iops.py async --step-size 0 --max-size 21 --reps 500 |
| Mix | 1.07GB | 2.05K | PYMPI_PROTO=proto mpirun -np 8 python test/iops.py async --min-size 8 --step-size 2 --step-expo 0.5 --max-size 28 |

| Time | TCP | MQTT | gRPC |
|-|-|-|-|
| Sync | 12.1 s | 33.1 s | 28.1 s |
| Async | 8.6 s | 23.7 s | 18.2 s |
| Mix | 7.2 s | 16.6 s | 12.4 s |

| Transfer | TCP | MQTT | gRPC |
|-|-|-|-|
| Sync | 1390.00 Mbps | 506.47 Mbps | 597.29 Mbps |
| Async | 1940.00 Mbps | 706.75 Mbps | 923.79 Mbps |
| Mix | 1200.00 Mbps | 517.83 Mbps | 691.85 Mbps |

| Operations | TCP | MQTT | gRPC |
|-|-|-|-|
| Sync | 41.46 IOPS | 15.09 IOPS | 17.8 IOPS |
| Async | 57.91 IOPS | 21.06 IOPS | 27.53 IOPS |
| Mix | 285.68 IOPS | 123.46 IOPS | 164.95 IOPS |

| Memory | TCP | MQTT | gRPC |
|-|-|-|-|
| Sync | 8.00 MB | 9.45 MB | 11.24 MB |
| Async | 1045.10 MB | 948.68 MB | 1058.11 MB |
| Mix | 560.32 MB | 507.15 MB | 577.45 MB |

## Install
### Production
```bash
pip install pympi
```

### Development
```bash
git clone https://github.com/hpca-uji/pympi.git
cd pympi
pip install -e .
```

#### With submodules
```bash
git clone https://github.com/hpca-uji/pympi.git
cd pympi
git submodule update --init net-queue
pip install -e ./net-queue
pip install -e .
```

## Limitations
Note: all the features mentioned here are planned for implementation,
however they are not currently available.

For communicators, only `COMM_WORLD` is supported.
`COMM_SELF`, `COMM_NULL`, subcommunicators and others can not be created.

Buffer-based operations are limited to the ones which the send and receive buffer are equally sized.

For point-to-point communications, only `send`, and its asynchronous and buffer-based variants, is supported.
`ssend`, `bsend` and `rsend` can not be used. Additionally, self-messaging and message tagging are also not supported.

## Implementation
For most cases `pympi` is a drop-in replacement for `mpi4py`,
therefore only mayor differences will be documented.

Documentation for `mpi4py` available here: <https://mpi4py.readthedocs.io/en/stable/>

However, unlike `mpi4py`, asynchronous responses will be available without needing to call `wait` on them.

---

For communications `net-queue` is used,
options can be customized via the `rc` module and environment variables.

Documentation for `net-queue` available here: <https://github.com/hpca-uji/net-queue>

Following it's memory handling methodology, no internal buffering is currently preformed,
so requests that typically block until a local copy is done, will block until the data is fully transmitted.
This is only meaningfully observed on the point-to-point `send`, elsewhere this difference is negligible.

---

`pympi` follows a client-server architecture, by default rank `0` launches the server on a separated thread.

The server can also be started externally by:
```bash
python -m pympi.server
```

## Personalized operations
Unlike traditional MPI and `mpi4py`,
with `pympi` you can define fully personalized operations,
not just reduce functions.

For example, on this operation we group ranks with its neighbor in pairs,
apply a custom reduce function, and return the result to the lower rank.

```python
# pair_reduce.py
from pympi import MPI, proto
comm = MPI.COMM_WORLD
size = comm.size
rank = comm.rank

# Define operation
@dataclass(slots=True, frozen=True)
class PairReduce(proto.OperationContext):
    reducer: Callable

    def group(self, size: int) -> proto.CommunicationGroup:
        src = range(size)
        dst = range(0, size, 2) # half of ranks
        return proto.CommunicationGroup(src, dst)

    def apply(self, src: Mapping[Rank, int], dst: Set[Rank]) -> Mapping[Rank, int]:
        values = [src[rank] for rank in sorted(src)]  # sort values by rank
        results = map(self.reducer, itertools.batched(values, 2))
        return dict(zip(dst, results))

# Inputs
value = rank
print(f"R{rank}: {value}")
# R0: 0
# R1: 1
# R2: 2
# R3: 3

# Execute operation
ctx = PairReduce(reducer=sum)
group = ctx.group(size=comm.size)
op = proto.OperationRequest(group, ctx, value)
result = comm.submit(op).wait()

# Outputs
print(f"R{rank}: {result}")
# R0: 1
# R1: None
# R2: 5
# R3: None
```

## Documentation
### Constants
- `rc.init: bool = True`

  Should server auto initialize

  Environment variables checked for defaults:
  - `PYMPI_ADDR` (if not defined)

- `rc.wait: float = 0.5` (seconds)

  Wait for auto server transitions

  If `init` is `False`, `wait` is ignored.

  Environment variables checked for defaults:
  - `PYMPI_WAIT`

- `rc.addr: str = "127.0.0.1"`

  Server address

  Environment variables checked for defaults:
  - `PYMPI_ADDR`

- `rc.port: int = 61642`

  Server port

  Environment variables checked for defaults:
  - `PYMPI_PORT`

- `rc.size: int = 1`

  Communication size

  Environment variables checked for defaults:
  - `PYMPI_SIZE`
  - `OMPI_COMM_WORLD_SIZE`
  - `PMI_SIZE`
  - `SLUM_NPROCS`

- `rc.rank: int = 0`

  Communication identifier

  Environment variables checked for defaults:
  - `PYMPI_RANK`
  - `OMPI_COMM_WORLD_RANK`
  - `PMI_RANK`
  - `SLUM_PROCID`

- `rc.serial: Iterable[str] = []`

  Serializable global names

  If `serial` is empty, no restrictions are applied.

  Internal protocol structures will be implicitly allowed.

  Environment variables checked for defaults:
  - `PYMPI_SERIAL` (comma separated list of global names)

  See `nq.stream.PickleSerializer(restrict)` for more information.

- `rc.serial_size: int = -1`

  Serializable message size

  If `serial_size` is negative, `serial_size` is ignored.

  Environment variables checked for defaults:
  - `PYMPI_SERIAL_SIZE`

  See `nq.SerializationOptions(...)` for more information.

- `rc.serial_queue: int = -1`

  Serializable queue size

  If `serial_queue` is negative, `serial_queue` is ignored.

  Environment variables checked for defaults:
  - `PYMPI_SERIAL_QUEUE`

  See `nq.SerializationOptions(...)` for more information.

- `rc.proto: nq.Protocol = Protocol.TCP`

  Communication protocol

  Environment variables checked for defaults:
  - `PYMPI_PROTO`

  See `nq.Protocol` for more information.

- `rc.ssl: bool = False`

  Use secure communications

  Environment variables checked for defaults:
  - `PYMPI_SSL`

  See `nq.SecurityOptions(...)` for more information.

- `rc.ssl_key: Path | None = None`

  Secure communications private key

  Environment variables checked for defaults:
  - `PYMPI_SSL_KEY`

  See `nq.SecurityOptions(...)` for more information.

- `rc.ssl_cert: Path | None = None`

  Secure communications certificate chain

  Environment variables checked for defaults:
  - `PYMPI_SSL_CERT`

  See `nq.SecurityOptions(...)` for more information.

- `rc.comm: nq.CommunicatorOptions = nq.CommunicatorOptions()`

  Additional net-queue communicator options

### Structures
- `proto.CommunicationGroup(...)`

  Communication group

  - `src: frozenset[Rank]`
  - `dst: frozenset[Rank]`

- `OperationContext(...)`

  Abstract dataclass base for operation contexts

  - `group(...) -> CommunicationGroup`

    Compute operation's communication group

  - `apply(src: Mapping[Rank, Any], dst: Set[Rank]) -> Mapping[Rank, Any]`

    Apply operation over objects

- `proto.OperationRequest(...)`

  Operation request

  - `group: CommunicationGroup`
  - `ctx: OperationContext | None = None`
  - `data: typing.Any | None = None`

  ---

  - `id: uuid.UUID = uuid.uuid4()` (random)

### Classes
- `Comm(comm_options)`

  Communicator

  - `comm_options: nq.CommunicatorOptions = nq.CommunicatorOptions()`

  ---

  - `submit(op, callback) -> Request`

    Schedule a new operation

    Operation can be a user defined instance, allowing for custom operations.

    - `op: proto.OperationRequest`
    - `callback: abc.Callable = lambda x: x`

      Called with the result of the operation prior to resolving the request, the return value of this function is the one provided to `wait`.

- `server.Server(thread_pool, comm_options)`

  MPI server

  - `thread_pool: concurrent.futures.ThreadPoolExecutor`
  - `comm_options: nq.CommunicatorOptions = nq.CommunicatorOptions()`

## Notes
Due to how Python and external libraries handle threading, there is no reliable way to track when the MPI context should be automatically finalized.

MPI for Python finalizes its context via a atexit handler, which waits for all non-daemon threads to finish before automatically finalizing (if not disabled).

This implementation attempts to finalizes its context specifically when the main thread finishes. As other threads might be internal implementation details and might be required during the finalization stage. Waiting for a atexit handler might lead to those internal threads being reaped, therefor being unable to gracefully finalize.

If this approach is not plausible, as it is Python implementation dependant, a classic atexit handler is also registered, but as mention before, this might lead to an undefined finalization behavior.

To ensure defined graceful finalization the only reliable method is to manually call Finalize, and ensure it is always called, even when exceptions might be unhandled and lead to thread termination.

## Acknowledgments
The library has been partially supported by:
- Project PID2023-146569NB-C22 "Inteligencia sostenible en el Borde-UJI" funded by the Spanish Ministry of Science, Innovation and Universities.
- Project C121/23 Convenio "CIBERseguridad post-Cuántica para el Aprendizaje FEderado en procesadores de bajo consumo y aceleradores (CIBER-CAFE)" funded by the Spanish National Cybersecurity Institute (INCIBE).