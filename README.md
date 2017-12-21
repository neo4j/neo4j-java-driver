# Neo4j Java Driver

This is the first official Neo4j java driver for connecting to Neo4j-the-database via the newly designed remoting
protocol BOLT.

For detailed information such as manual, driver API documentations, changelogs, please refer to 
[wiki](https://github.com/neo4j/neo4j-java-driver/wiki).

## Java version

Starting from 1.5 driver requires Java 8 for building and at runtime.
Please use 1.4 series when Java 7 is required.

## Minimum viable snippet

Add the driver to your project:

    <dependencies>
        <dependency>
            <groupId>org.neo4j.driver</groupId>
            <artifactId>neo4j-java-driver</artifactId>
            <version>x.y.z</version>
        </dependency>
    </dependencies>

*Please check the [Releases](https://github.com/neo4j/neo4j-java-driver/releases) for the newest driver version
available.


Connect to a Neo4j 3.0.0+ database:

    Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "neo4j" ) );
    
    try ( Session session = driver.session() )
    {
        StatementResult rs = session.run( "CREATE (n) RETURN n" );
    }
    
    driver.close();

## Building

The source code here reflects the current development status of a new driver version.
If you want to use the driver in your products, please use the released driver via maven central or check out the
code with git tags instead.

### Running tests and creating a package

The driver unit tests relies on latest [`boltkit`](https://github.com/neo4j-contrib/boltkit) installed on your local machine. 
If `boltkit` is not installed, then all tests that requires `boltkit` will be ignored and not be executed.

Then execute the following Maven command:

    mvn clean install

### Windows

If you are building on windows, you need to run install as admin, so that Neo4j-the-database could be installed and 
started for integration tests.
