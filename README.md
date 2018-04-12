# Neo4j Java Driver

This is the official Neo4j java driver for connecting to a Neo4j database or a Neo4j cluster via in-house binary protocol BOLT.

## For Driver Users

This section provides some useful information for application developers
who would like to use this driver in their projects to send queries to their Neo4j database and/or Neo4j cluster.

### Java Runtime

Since 1.6 series, the driver supports Java 8 and Java 9 runtime.
The automatic module name of the driver required by Java 9 is `org.neo4j.driver`.

The table bellow shows all driver series and supported Java runtime.

|         Driver Series |         1.6       |   1.5  |   1.4 and bellow  |
| ---------------------:|:-----------------:|:------:|:-----------------:|
| Supported Java Runtime| Java 8 and Java 9 | Java 8 | Java 7 and Java 8 |



### Minimum Viable Snippet

The driver is distributed via Maven exclusively.
To use the driver in your Maven project:
```html
<dependency>
    <groupId>org.neo4j.driver</groupId>
    <artifactId>neo4j-java-driver</artifactId>
    <version>x.y.z</version>
</dependency>
```
The version `x.y.x` need to be replaced with a released driver version.
The latest driver version is always recommended for accessing newest features and recent bug fixes.
The latest and all available versions of this driver could be found at
[Maven Central](https://mvnrepository.com/artifact/org.neo4j.driver/neo4j-java-driver) or
[Releases](https://github.com/neo4j/neo4j-java-driver/releases).


To connect to a Neo4j 3.0.0+ database:
```java
Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "PasSW0rd" ) );
try ( Session session = driver.session() )
{
    StatementResult rs = session.run( "CREATE (n) RETURN n" );
}
driver.close();
```
While using this driver in an application project, we recommend **a single driver** during the whole lifetime of the application
to benefit from the connection pool maintained in the `driver` object.

The `driver` object holds a connection pool to avoid performance overhead added by establishing TCP connections for each query run.
Network connections are established on demands when running Cypher queries, and returned back to connection pool after query execution finishes.
As a result, it is expensive to create and close driver, while super cheap to open and close sessions.

The driver is thread-safe, but the session is not thread-safe. So make sure sessions are not passed among threads.

### More Manual and Documents
Check out our [Wiki](https://github.com/neo4j/neo4j-java-driver/wiki) for detailed and most up-to-date developer manuals, driver API documentations, changelogs, etc.

### Bug Report
If you encounter any bugs while using this driver, please following the instructions in our [Contributing Criteria](https://github.com/neo4j/neo4j-java-driver/blob/1.6/CONTRIBUTING.md#need-to-raise-an-issue)
when raising an issue at [Issues](https://github.com/neo4j/neo4j-java-driver/issues).

When reporting, please mention the version of the driver and version of the server, the setup of the server such as single instance or HA or causal cluster,
the error stacktrace, code snippet to reproduce the error if possible, and anything that you think it is helpful to reproduce the error.

## For Driver Developers
This section targets at people who would like to compile the source code on their own machine for the purpose of, for example, contributing a PR to this repository.
Before contributing to this project, please take a few minutes and read our [Contributing Criteria](https://github.com/neo4j/neo4j-java-driver/blob/1.6/CONTRIBUTING.md#want-to-contribute).

### Java Version

Starting from 1.5 series the driver is compiled on Java 8. For previous series, the code compilation requires Java 7.

### Building

The source code here reflects the current development status of a new driver version.
If you want to use the driver in your project, please use the released driver via Maven Central or check out the code with git tags of corresponding versions instead.

#### Running Tests and Creating a Package

The driver unit tests relies on latest [`boltkit`](https://github.com/neo4j-contrib/boltkit) installed on your local machine. 
If `boltkit` is not installed, then all tests that requires `boltkit` will be ignored and will not be executed.

The following Maven command shows how to run all tests and build the source code:
```
mvn clean install
```
When running integration tests, the driver would start a Neo4j instance and a Neo4j cluster on your local machine.
Your tests might fail due to
* a Neo4j server instance is already running on your machine and occupied default server ports,
* missing the right to download neo4j enterprise artifacts.

If you do not wish to run integration tests, then use the following command to run without integration tests:
```
mvn clean install -DskipITs
```

#### Windows

If you are building on windows, you need to run install with admin right, as Neo4j installation requires admin right to install as a service and
start for integration tests.
