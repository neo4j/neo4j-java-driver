package org.neo4j.driver.internal.messaging.request;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.Bookmarks;
import org.neo4j.driver.v1.Value;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.v1.Values.value;

class RunWithMetadataMessageTest
{
    @Test
    void shouldHaveCorrectMetadata()
    {
        Bookmarks bookmarks = Bookmarks.from( asList( "neo4j:bookmark:v1:tx11", "neo4j:bookmark:v1:tx52" ) );

        Map<String,Value> txMetadata = new HashMap<>();
        txMetadata.put( "foo", value( "bar" ) );
        txMetadata.put( "baz", value( 111 ) );
        txMetadata.put( "time", value( LocalDateTime.now() ) );

        Duration txTimeout = Duration.ofSeconds( 7 );

        RunWithMetadataMessage message = new RunWithMetadataMessage( "RETURN 1", emptyMap(), bookmarks, txTimeout, txMetadata );

        Map<String,Value> expectedMetadata = new HashMap<>();
        expectedMetadata.put( "bookmarks", value( bookmarks.values() ) );
        expectedMetadata.put( "tx_timeout", value( 7000 ) );
        expectedMetadata.put( "tx_metadata", value( txMetadata ) );

        assertEquals( expectedMetadata, message.metadata() );
    }
}
