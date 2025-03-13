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
    if os.getenv("TEST_NEO4J_BOLT_CONNECTION", "false") == "true" :
        run(["mvn", "-f", "/driver/bolt-connection/pom.xml", "--show-version", "--batch-mode", "clean", "versions:set", "-DnewVersion=0.0.0"])
        run(["mvn", "-f", "/driver/bolt-connection/pom.xml", "--show-version", "--batch-mode", "clean", "install", "-DskipTests"])
        run(["mvn", "--show-version", "--batch-mode", "clean", "install", "-P", "!determine-revision", "-DskipTests", "-Dneo4j-bolt-connection-bom.version=0.0.0"])
    else:
        run(["mvn", "--show-version", "--batch-mode", "clean", "install", "-P", "!determine-revision", "-DskipTests"])
