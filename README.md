# Neo4j Java Driver

This repository holds the official Java Driver for Neo4j.

It works with both single instance and clustered databases.

Network communication is handled using [Bolt Protocol](https://7687.org/).

## Supported Driver Series

| Driver Series | Supported Java Runtime versions | Status | Changelog |
| --- | --- | --- | --- |
| 5.0 | 8, 11 | Primary development branch. | [link](https://github.com/neo4j/neo4j-java-driver/wiki/5.0-changelog) |
| 4.4 | 8, 11 | Maintenance. | [link](https://github.com/neo4j/neo4j-java-driver/wiki/4.4-changelog) |
| 4.3 | 8, 11 | Maintenance. | [link](https://github.com/neo4j/neo4j-java-driver/wiki/4.3-changelog) |
| 4.2 | 8, 11 | Maintenance. | [link](https://github.com/neo4j/neo4j-java-driver/wiki/4.2-changelog) |
| 4.1 | 8, 11 | Maintenance. | [link](https://github.com/neo4j/neo4j-java-driver/wiki/4.1-changelog) |

## Server Compatibility

The compatibility with Neo4j Server versions is documented in the [Neo4j Knowledge Base](https://neo4j.com/developer/kb/neo4j-supported-versions/).

## Usage

This section provides general information for engineers who are building Neo4j-backed applications.

### The `pom.xml` file

The driver is distributed exclusively via [Maven](https://search.maven.org/).
To use the driver in your Maven project, include the following within your `pom.xml` file:
```xml
<dependency>
    <groupId>org.neo4j.driver</groupId>
    <artifactId>neo4j-java-driver</artifactId>
    <version>x.y.z</version>
</dependency>
```
Here, `x.y.z` will need to be replaced with the appropriate driver version.
It is generally recommended to use the latest driver version wherever possible.
This ensures access to new features and recent bug fixes.
All available versions of this driver can be found at
[Maven Central](https://mvnrepository.com/artifact/org.neo4j.driver/neo4j-java-driver) or
[Releases](https://github.com/neo4j/neo4j-java-driver/releases).

### Java Module System

The automatic module name of the driver for the Java Module System is `org.neo4j.driver`.

### Example

To run a simple query, the following can be used:
```java
Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "PasSW0rd"));
try (Session session = driver.session()) {
    Result result = session.run("CREATE (n) RETURN n");
}
driver.close();
```

For full applications, a single ``Driver`` object should be created with application-wide scope and lifetime.
This allows full utilization of the driver connection pool.
The connection pool reduces network overhead added by sharing TCP connections between subsequent transactions.
Network connections are acquired on demand from the pool when running Cypher queries, and returned back to connection pool after query execution finishes.
As a result of this design, it is expensive to create and close a ``Driver`` object.
``Session`` objects, on the other hand, are very cheap to use.

### Thread Safety

``Driver`` objects are thread-safe, but ``Session`` and ``Transaction`` objects should only be used by a single thread.

### Further reading
Check out our [Wiki](https://github.com/neo4j/neo4j-java-driver/wiki) for detailed and most up-to-date manuals, driver API documentations, changelogs, etc.

### Bug Report
If you encounter any bugs while using this driver, please follow the instructions in our [Contribution Guide](https://github.com/neo4j/neo4j-java-driver/blob/1.6/CONTRIBUTING.md#need-to-raise-an-issue)
when raising an issue at [Issues](https://github.com/neo4j/neo4j-java-driver/issues).

When reporting, please mention the versions of the driver and server, as well as the server topology (single instance, causal cluster, etc).
Also include any error stacktraces and a code snippet to reproduce the error if possible, as well as anything else that you think might be helpful.

## For Driver Engineers

This section targets users who would like to compile the driver source code on their own machine for the purpose of, for example, contributing a PR.
Before contributing to this project, please take a few minutes to read our [Contribution Guide](https://github.com/neo4j/neo4j-java-driver/blob/1.6/CONTRIBUTING.md#want-to-contribute).

### Java Version

For the 1.5+ Driver Series, the source code _must_ compile on Java 8.

For the previous versions, the compilation requires Java 7.

### Building

The source code here reflects the current development status of a new driver version.

To use the driver in a project, please use the released driver via Maven Central or check out the code with git tags of corresponding versions instead.

#### Running Tests and Creating a Package

The following command may be used to unit test and install artifacts without running integration tests:
```
mvn clean install -DskipITs -P skip-testkit
```

Integration tests have the following prerequisites:
- Docker
- [`Testkit`](https://github.com/neo4j-drivers/testkit)

Testkit that is a tooling that is used to run integration tests for all official Neo4j drivers.
It can be executed using Docker during Maven build and in such case does not require additional setup. See the instructions below for more details.

There are 2 ways of running Testkit tests:
1. Using the `testkit-tests` module of this project.
2. Manually cloning Testkit and running it directly.

##### Using the testkit-tests module

The `testkit-tests` module will automatically checkout Testkit and run it during Maven build.

Prerequisites:
- Docker
- `/var/run/docker.sock` available to be passed through to running containers. 
  This is required because Testkit needs access to Docker in order to launch additional containers.
- The driver project location must be sharable with Docker containers as Testkit container needs to have access to it.

Make sure to run build for the whole project and not just for `testkit-tests` module. Sample commands:
- `mvn clean verify` - runs all tests.
- `mvn clean verify -DskipTests` - skips all tests.
- `mvn clean verify -DtestkitArgs='--tests STUB_TESTS'` - runs all project tests and Testkit stub tests.
- `mvn clean verify -DskipTests -P testkit-tests` - skips all project tests and runs Testkit tests.
- `mvn clean verify -DskipTests -DtestkitArgs='--tests STUB_TESTS'` - skips all project tests and runs Testkit stub tests.
- `mvn clean verify -DskipITs -DtestkitArgs='--tests STUB_TESTS'` - skips all integration tests and runs Testkit stub tests.
- `mvn clean verify -DskipTests -DtestkitArgs='--run-only-selected tests.neo4j.test_session_run.TestSessionRun.test_can_return_path --configs 4.4-enterprise-neo4j'` - skips all project tests and runs selected Testkit test on specific configuration.

If you interrupt Maven build, you have to remove Testkit containers manually.

##### Running Testkit manually

Prerequisites:
- Docker
- Python3
- Git

Clone Testkit and run as follows:

```
git clone git@github.com:neo4j/neo4j-java-driver.git 
git clone git@github.com:neo4j-drivers/testkit.git
cd testkit
TEST_DRIVER_NAME=java \
TEST_DRIVER_REPO=`realpath ../neo4j-java-driver` \
TEST_DOCKER_RMI=true \
python3 main.py --tests UNIT_TESTS --configs 4.3-enterprise
```

To run additional Testkit test, specify `TESTKIT_TESTS`:

```
TEST_DRIVER_NAME=java \
TEST_DRIVER_REPO=`realpath ../neo4j-java-driver` \
TEST_DOCKER_RMI=true \
python3 main.py --tests TESTKIT_TESTS UNIT_TESTS --configs 4.3-enterprise
````

On Windows or in the abscence of a Bash-compatible environment, the required steps are probably different.
A simple `mvn clean install` will require admin rights on Windows, because our integration tests require admin privileges to install and start a service.

If all of this fails and you only want to try out a local development version of the driver, you could skip all tests like this:

```
mvn clean install -DskipTests
```
