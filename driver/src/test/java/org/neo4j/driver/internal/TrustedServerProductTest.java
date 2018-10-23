package org.neo4j.driver.internal;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.exceptions.UntrustedServerException;
import org.neo4j.driver.v1.util.StubServer;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.v1.Logging.none;

public class TrustedServerProductTest
{
    private static final Config config = Config.build()
            .withoutEncryption()
            .withLogging( none() )
            .toConfig();

    @Test
    void shouldRejectConnectionsToNonNeo4jServers() throws Exception
    {
        StubServer server = StubServer.start( "untrusted_server.script", 9001 );
        assertThrows( UntrustedServerException.class, () -> GraphDatabase.driver( "bolt://127.0.0.1:9001", config ));
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

}
