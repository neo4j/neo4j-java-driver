# Neo4j Java Driver

This repository holds the official Java Driver for Neo4j.

It works with both single instance and clustered databases.

Network communication is handled using [Bolt Protocol](https://7687.org/).

## Versioning

Starting with 5.0, the Neo4j Drivers will be moving to a monthly release cadence. A minor version will be released on 
the last Friday of each month so as to maintain versioning consistency with the core product (Neo4j DBMS) which has also moved to a monthly cadence.

As a policy, patch versions will not be released except on rare occasions. Bug fixes and updates will go into the latest minor version and users should upgrade to that. Driver upgrades within a major version will never contain breaking API changes.

## Supported Driver Series

| Driver Series | Supported Java Runtime versions | Status                      | Changelog                                                             |
|---------------|---------------------------------|-----------------------------|-----------------------------------------------------------------------|
| 5.x           | 17                              | Primary development branch. | [link](https://github.com/neo4j/neo4j-java-driver/wiki/5.x-changelog) |
| 4.4           | 8, 11                           | Maintenance.                | [link](https://github.com/neo4j/neo4j-java-driver/wiki/4.4-changelog) |

## Server Compatibility

The compatibility with Neo4j Server versions is documented in the [Neo4j Knowledge Base](https://neo4j.com/developer/kb/neo4j-supported-versions/).

## Preview features

The preview feature is a new feature that is a candidate for a future <abbr title="Generally Available">GA</abbr> 
status.

It enables users to try the feature out and maintainers to refine and update it.

The preview features are not considered to be experimental, temporary or unstable.

However, they may change more rapidly, without following the usual deprecation cycle.

Most preview features are expected to be granted the GA status unless some unexpected conditions arise.

Due to the increased flexibility of the preview status, user feedback is encouraged so that it can be considered before
the GA status.

Every feature gets a dedicated [GitHub Discussion](https://github.com/neo4j/neo4j-java-driver/discussions/categories/preview-features) 
where feedback may be shared.

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

### Java Platform Module System

Both `neo4j-java-driver` and `neo4j-java-driver-all` artifacts have `org.neo4j.driver` module name.

Starting from version 5.0 the `neo4j-java-driver` includes an explicit module declaration ([module-info.java](driver/src/main/java/module-info.java)).

The `neo4j-java-driver-all` includes an explicit module declaration ([module-info.java](bundle/src/main/jpms/module-info.java)) starting from version 5.7.

### Example

To run a simple query, the following can be used:
```java
var authToken = AuthTokens.basic("neo4j", "password");
try (var driver = GraphDatabase.driver("bolt://localhost:7687", authToken)) {
    var result = driver.executableQuery("CREATE (n)").execute();
    System.out.println(result.summary().counters().nodesCreated());
}
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

For the 5.x Driver Series, the source code _must_ compile on Java 17.

For the 4.x Driver Series, the compilation requires Java 8.

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

The `testkit-tests` module will automatically check out Testkit and run it during Maven build.

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

On Windows or in the absence of a Bash-compatible environment, the required steps are probably different.
A simple `mvn clean install` will require admin rights on Windows, because our integration tests require admin privileges to install and start a service.

If all of this fails and you only want to try out a local development version of the driver, you could skip all tests like this:

```
mvn clean install -DskipTests
```
