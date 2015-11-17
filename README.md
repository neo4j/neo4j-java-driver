# Neo4j Java Driver

An alpha-level database driver for a new Neo4j remoting protocol. 

*Note*: This is in active development, the API is not stable. Please try it out and give us feedback, but expect 
things to break in the medium term!

## Minimum viable snippet

Add the driver to your project:

    <dependencies>
        <dependency>
            <groupId>org.neo4j.driver</groupId>
            <artifactId>neo4j-java-driver</artifactId>
            <version>1.0-SNAPSHOT</version>
        </dependency>
    </dependencies>
    
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

    Driver driver = GraphDatabase.driver( "bolt://localhost" );
    
    Session session = driver.session();
    
    Result rs = session.run("CREATE (n) RETURN n");
    
    session.close();
    
# Building

## Java version

If you are running Java 8:

    mvn clean install
    
If you are running Java 7, you need to also provide an environment variable telling the tests where to find
Java 8, because Neo4j-the-database needs it to run.

    export NEO4J_JAVA=<path/to/java/home>
    mvn clean install
    
    # For instance
    export NEO4J_JAVA=$(/usr/libexec/java_home -v 1.8)

## Windows

If you are building on windows, you need to run install as admin so that Neo4j-the-database could be registered as a
windows service and then be started and stopped correctly using its powershell scripts for windows.
To be able to run powershell script on windows, you might need to enable running scripts on the system.
This can for example be achieved by executing the following from an elevated PowerShell prompt:

    Set-ExecutionPolicy -ExecutionPolicy RemoteSigned
