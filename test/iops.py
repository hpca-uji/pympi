"""MPI server-client IOPS test"""

import math
import time
import enum
from argparse import ArgumentParser, Namespace

import numpy


__all__ = ()


class Mode(enum.StrEnum):
    """Test mode"""
    SYNC = enum.auto()
    ASYNC = enum.auto()


# Argument pasrser
parser = ArgumentParser(prog="pympi-test-iops", description="PyMPI IOPS test")
parser.add_argument("mode", choices=list(Mode))
parser.add_argument("--size", type=int, default=1_000)
parser.add_argument("--reps", type=int, default=1_000)


def convert_size(units: int, scale: int = 1000):
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


def print_stats(config: Namespace, time: float) -> None:
    """Print statistics"""
    ops = config.reps * 2
    size = config.size * ops
    print(f"Time:       {time:.1f}s")
    print(f"Data:       {convert_size(config.reps)} x {convert_size(config.size)}B")
    print(f"Transfer:   {convert_size(size)}B @ {convert_size(size * 8 / time):>5}bps")
    print(f"Operations: {convert_size(ops)} @ {convert_size(ops / time)}IOPS")


def main(config: Namespace):
    """Application entrypoint"""
    print(config)

    message = numpy.arange(config.size, dtype=numpy.uint8)

    from pympi import MPI

    comm = MPI.COMM_WORLD
    size = comm.size

    messages = [ary.copy() for ary in numpy.split(message, size)]

    comm.barrier()
    start_time = time.time()

    match config.mode:
        case Mode.SYNC:
            for _ in range(config.reps):
                comm.alltoall(messages)

        case Mode.ASYNC:
            reqs = [comm.ialltoall(messages) for _ in range(config.reps)]
            while reqs:
                done = set(MPI.Request.Waitsome(reqs))
                reqs = [req for i, req in enumerate(reqs) if i not in done]

    end_time = time.time()
    del message
    MPI.Finalize()

    print_stats(config=config, time=end_time - start_time)


if __name__ == "__main__":
    main(parser.parse_args())
