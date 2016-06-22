# Neo4j Java Driver

This is the first official Neo4j java driver for connecting to Neo4j-the-database via the newly designed remoting
protocol BOLT.

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

    Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "neo4j" ) );
    
    try ( Session session = driver.session() )
    {
        StatementResult rs = session.run( "CREATE (n) RETURN n" );
    }
    
    driver.close();

For more examples and details of usage, please refer to the [Driver Manual] (http://neo4j.com/docs/developer-manual/3.0/index.html#driver-manual-index).

## Binding

The source code here reflects the current development status of a new driver version.
If you want to use the driver in your products, please use the released driver via maven central or check out the
code with git tags instead.

### Java version

To compile the code and run all tests, if you are running Java 8:

    mvn clean install
    
If you are running Java 7, you need to also provide an environment variable telling the tests where to find
Java 8, because Neo4j-the-database needs it to run.

    export NEO4J_JAVA=<path/to/java/home>
    mvn clean install
    
    # For instance
    export NEO4J_JAVA=$(/usr/libexec/java_home -v 1.8)

### Windows

If you are building on windows, you need to have Python (v2.7) installed and have Python.exe to be added in your system `PATH` variables.
Then run install as admin, so that Neo4j-the-database could be installed and started with Python scripts for integration tests.

Or you could choose to ignore integration tests by running:

    mvn clean install -DskipITs 

Without integration tests, there is no need to install Python or run as admin.

For more information such as manual, driver API documentations, changelogs, please refer to [wiki](https://github.com/neo4j/neo4j-java-driver/wiki).