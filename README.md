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

    Driver driver = ( "bolt://localhost", AuthTokens.basic( "neo4j", "neo4j" ) );
    
    Session session = driver.session();
    
    StatementResult rs = session.run( "CREATE (n) RETURN n" );
    
    session.close();
    
    driver.close();

For more examples and details of usage, please refer to the [Driver Manual] (http://neo4j.com/docs/developer-manual/3
.0/index.html#driver-manual-index).

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

If you are building on windows, you need to have Python (v2.7) installed and run install as admin,
so that Neo4j-the-database could be installed and started with Python scripts for integration tests.
Or you could choose to ignore integration tests by running:

    mvn clean install -DskipITs

## Q&A

**Q**: Where can I find the changelogs?  
**A**: [wiki](https://github.com/neo4j/neo4j-java-driver/wiki)


**Q**: Why my driver stops working today after I upgrade my Neo4j server? It was working well yesterday.  
**A**: If the driver is configured to use [trust-on-first-use]
(http://neo4j.com/docs/developer-manual/3.0/index.html#_trust) mode,
then following the error message you got while using the driver, you might need to modify `known_hosts` file.


**Q**: Why I cannot connect the driver to the server at `bolt://localhost:7474`?  
**A**: Bolt uses port 7687 by default, so try `bolt://localhost:7687` or `bolt://localhost` instead.

For any other questions, please refer to Github issues.
