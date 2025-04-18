"""
Executed in Java driver container.
Responsible for running unit tests.
Assumes driver has been setup by build script prior to this.
"""
import subprocess
import os


def run(args):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT, check=True)


if __name__ == "__main__":
    cmd = ["mvn", "test", "-Dmaven.gitcommitid.skip"]
    if os.getenv("TEST_NEO4J_BOLT_CONNECTION", "false") == "true" :
        cmd.append("-Dneo4j-bolt-connection-bom.version=0.0.0")
    run(cmd)
