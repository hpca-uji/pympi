"""MPI server-client IOPS test"""

import math
import time
import enum
import random
from argparse import ArgumentParser, Namespace

import numpy


__all__ = ()


class Peer(enum.StrEnum):
    """Peer type"""
    SERVER = enum.auto()
    CLIENT = enum.auto()


class Mode(enum.StrEnum):
    """Test mode"""
    SYNC = enum.auto()
    ASYNC = enum.auto()


# Argument pasrser
parser = ArgumentParser(prog="pympi-test-iops", description="PyMPU IOPS test")
parser.add_argument("mode", choices=list(Mode), help="Synchronization mode")
parser.add_argument("--min-size", type=int, default=8, help="Exponenet of minimun message size")
parser.add_argument("--step-size", type=int, default=2, help="Exponenet between message sizes")
parser.add_argument("--max-size", type=int, default=32, help="Exponenet of maxmimun message size")
parser.add_argument("--step-expo", type=float, default=0.5, help="Exponenet of number of splits when stepping down a size")
parser.add_argument("--reps", type=int, default=1, help="Number of repetitions of messages")


def convert_size(units: float, scale: int = 1000):
    """Convert unit to use SI suffixes"""
    size_name = ("", "K", "M", "G", "T", "P", "E", "Z", "Y")
    if units > 0:
        i = int(math.log(units, scale))
        p = math.pow(scale, i)
        s = round(units / p, 2)
    else:
        i = 0
        s = 0
    return f"{s}{size_name[i]}"


def print_stats(sizes: list[int], time: float) -> None:
    """Print statistics"""
    ops = len(sizes)
    size = sum(sizes) * 2
    avg = sum(sizes) / len(sizes)
    print(f"Time:       {time:.1f}s")
    print(f"Data:       {convert_size(len(sizes))} @ {convert_size(avg)}B")
    print(f"Transfer:   {convert_size(size)}B @ {convert_size(size * 8 / time):>5}bps")  # type: ignore
    print(f"Operations: {convert_size(ops)} @ {convert_size(ops / time)}IOPS")  # type: ignore


def generate(config: Namespace) -> list[numpy.ndarray]:
    """Generate messages"""
    messages = []
    buffer = numpy.arange(2 ** config.max_size, dtype=numpy.uint8)
    if config.step_size:
        for i in range(config.min_size, config.max_size, config.step_size):
            splits = round(((2 ** config.max_size) / (2 ** i)) ** config.step_expo)
            for j in range(splits):
                messages.append(buffer[:2 ** i])
    messages.append(buffer)
    messages *= config.reps
    random.shuffle(messages)
    return messages


def main(config: Namespace):
    """Application entrypoint"""
    print(config)
    random.seed(0)

    messages = generate(config)
    sizes = list(map(len, messages))

    from pympi import MPI

    comm = MPI.COMM_WORLD
    size = comm.size

    messages = [[ary.copy() for ary in numpy.array_split(message, size)] for message in messages]

    comm.barrier()
    start_time = time.perf_counter()

    match config.mode:
        case Mode.SYNC:
            for message in messages:
                comm.alltoall(message)

        case Mode.ASYNC:
            reqs = [comm.ialltoall(message) for message in messages]
            while reqs:
                done = set(MPI.Request.Waitsome(reqs))
                reqs = [req for i, req in enumerate(reqs) if i not in done]

    end_time = time.perf_counter()
    MPI.Finalize()

    print_stats(sizes=sizes, time=end_time - start_time)


if __name__ == "__main__":
    main(parser.parse_args())
