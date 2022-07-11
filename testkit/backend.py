"""
Executed in Java driver container.
Assumes driver and backend has been built.
Responsible for starting the test backend.
"""
import os
import subprocess
import sys


if __name__ == "__main__":
    subprocess.check_call(
        ["java", "-Djava.security.properties=/testkit.java.security", "-jar", "testkit-backend/target/testkit-backend.jar",
        os.getenv('TEST_BACKEND_SERVER', '')],
        stdout=sys.stdout, stderr=sys.stderr
    )

