#!/usr/bin/env python3
"""Basic local mpirun"""

import os
import subprocess
from collections import abc
from argparse import ArgumentParser, Namespace


__all__ = (
    "main",
)


def main(config: Namespace, args: abc.Sequence[str]) -> None:
    """Application entrypoint"""

    for rank in range(config.size):
        environ = os.environ.copy()
        environ.update({"PMI_RANK": str(rank), "PMI_SIZE": str(config.size)})
        subprocess.Popen(args=args, env=environ)


def _start() -> int:
    """System entrypoint"""
    import sys, os  # noqa
    parser = ArgumentParser(prog="mpirun", description="basic local mpirun")
    parser.add_argument("-np", dest="size", type=int, default=os.process_cpu_count())
    config, args = parser.parse_known_args(sys.argv[1:])
    return main(config, args)  # type: ignore


if __name__ == "__main__":
    raise SystemExit(_start())
