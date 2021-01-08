"""
Executed in java driver container.
Responsible for building driver and test backend.
"""
import os, subprocess

def run(args):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT, check=True)

if __name__ == "__main__":
    run(["mvn", "clean", "install", "-P", "!determine-revision", "-DskipTests"])

