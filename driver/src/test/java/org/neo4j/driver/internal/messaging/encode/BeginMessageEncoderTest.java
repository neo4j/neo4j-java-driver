package org.neo4j.driver.internal.messaging.encode;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.Bookmarks;
import org.neo4j.driver.internal.messaging.ValuePacker;
import org.neo4j.driver.internal.messaging.request.BeginMessage;
import org.neo4j.driver.v1.Value;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.messaging.request.ResetMessage.RESET;
import static org.neo4j.driver.v1.Values.value;

class BeginMessageEncoderTest
{
    private final BeginMessageEncoder encoder = new BeginMessageEncoder();
    private final ValuePacker packer = mock( ValuePacker.class );

    @Test
    void shouldEncodeBeginMessage() throws Exception
    {
        Bookmarks bookmarks = Bookmarks.from( "neo4j:bookmark:v1:tx42" );

        Map<String,Value> txMetadata = new HashMap<>();
        txMetadata.put( "hello", value( "world" ) );
        txMetadata.put( "answer", value( 42 ) );

        Duration txTimeout = Duration.ofSeconds( 1 );

        encoder.encode( new BeginMessage( bookmarks, txTimeout, txMetadata ), packer );

        InOrder order = inOrder( packer );
        order.verify( packer ).packStructHeader( 1, BeginMessage.SIGNATURE );

        Map<String,Value> expectedMetadata = new HashMap<>();
        expectedMetadata.put( "bookmarks", value( bookmarks.values() ) );
        expectedMetadata.put( "tx_timeout", value( 1000 ) );
        expectedMetadata.put( "tx_metadata", value( txMetadata ) );

        order.verify( packer ).pack( expectedMetadata );
    }

    @Test
    void shouldFailToEncodeWrongMessage()
    {
        assertThrows( IllegalArgumentException.class, () -> encoder.encode( RESET, packer ) );
    }
}
