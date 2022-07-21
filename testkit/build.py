"""
Executed in java driver container.
Responsible for building driver and test backend.
"""
import subprocess
import os


def run(args):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT, check=True)


if __name__ == "__main__" and "TEST_SKIP_BUILD" not in os.environ:
    run(["mvn", "--show-version", "--batch-mode", "clean", "install", "-P", "!determine-revision", "-DskipTests"])
