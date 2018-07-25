package org.neo4j.driver.internal.messaging.request;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.Bookmarks;
import org.neo4j.driver.v1.Value;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.v1.Values.value;

class BeginMessageTest
{
    @Test
    void shouldHaveCorrectMetadata()
    {
        Bookmarks bookmarks = Bookmarks.from( asList( "neo4j:bookmark:v1:tx42", "neo4j:bookmark:v1:tx4242", "neo4j:bookmark:v1:tx424242" ) );

        Map<String,Value> txMetadata = new HashMap<>();
        txMetadata.put( "hello", value( "world" ) );
        txMetadata.put( "answer", value( 4242 ) );
        txMetadata.put( "true", value( false ) );

        Duration txTimeout = Duration.ofSeconds( 13 );

        BeginMessage message = new BeginMessage( bookmarks, txTimeout, txMetadata );

        Map<String,Value> expectedMetadata = new HashMap<>();
        expectedMetadata.put( "bookmarks", value( bookmarks.values() ) );
        expectedMetadata.put( "tx_timeout", value( 13_000 ) );
        expectedMetadata.put( "tx_metadata", value( txMetadata ) );

        assertEquals( expectedMetadata, message.metadata() );
    }
}
