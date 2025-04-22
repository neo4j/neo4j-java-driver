"""
Executed in java driver container.
Responsible for invoking Java stress test suite.
The stress test might be invoked multiple times against different versions
of Neo4j.
Assumes driver has been built before.
"""
import subprocess
import os

if __name__ == "__main__":
    uri = "%s://%s:%s" % (
            os.environ["TEST_NEO4J_SCHEME"],
            os.environ["TEST_NEO4J_HOST"],
            os.environ["TEST_NEO4J_PORT"])
    password = os.environ["TEST_NEO4J_PASS"]
    is_cluster = os.environ.get("TEST_NEO4J_IS_CLUSTER", False)
    if is_cluster:
        suite = "CausalClusteringStressIT"
    else:
        suite = "SingleInstanceStressIT"

    cmd = [
            "mvn", "surefire:test",
            "--file", "./driver/pom.xml",
            "-Dtest=%s,AbstractStressTestBase" % suite,
            "-DexternalClusterUri=%s" % uri,
            "-Dneo4jUserPassword=%s" % password,
            "-DthreadCount=10",
            "-DexecutionTimeSeconds=10",
            "-Dmaven.gitcommitid.skip=true",
    ]
    if os.getenv("TEST_NEO4J_BOLT_CONNECTION", "false") == "true" :
        cmd.append("-Dneo4j-bolt-connection-bom.version=0.0.0")
    subprocess.run(cmd, universal_newlines=True,
                   stderr=subprocess.STDOUT, check=True)
