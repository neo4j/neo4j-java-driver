# Neo4j Java Driver

An alpha-level database driver for a new Neo4j remoting protocol. 

*Note*: This is in active development, the API is not stable. Please try it out and give us feedback, but expect 
things to break in the medium term!

## Minimum viable snippet

Add the driver to your project:

    <dependencies>
        [..]
        <dependency>
            <groupId>org.neo4j.driver</groupId>
            <artifactId>neo4j-java-driver</artifactId>
            <version>1.0-SNAPSHOT</version>
        </dependency>
    </dependencies>
    [..]
    <repositories>
        <repository>
            <id>neo4j-snapshot-repository</id>
            <name>Neo4j Maven 2 snapshot repository</name>
            <url>http://m2.neo4j.org/content/repositories/snapshots</url>
            <snapshots>
                <enabled>true</enabled>
            </snapshots>
            <releases>
                <enabled>false</enabled>
            </releases>
        </repository>
    </repositories>
    

Connect to a Neo4j 3.0.0+ database

    Driver driver = GraphDatabase.driver( "neo4j://localhost" );
    
    Session session = driver.session();
    
    Result rs = session.run("CREATE (n) RETURN n");
    
    session.close();
    
