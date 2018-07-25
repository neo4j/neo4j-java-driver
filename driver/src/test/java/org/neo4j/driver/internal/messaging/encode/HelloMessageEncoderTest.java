package org.neo4j.driver.internal.messaging.encode;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.messaging.ValuePacker;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.v1.Value;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.messaging.request.PullAllMessage.PULL_ALL;
import static org.neo4j.driver.v1.Values.value;

class HelloMessageEncoderTest
{
    private final HelloMessageEncoder encoder = new HelloMessageEncoder();
    private final ValuePacker packer = mock( ValuePacker.class );

    @Test
    void shouldEncodeHelloMessage() throws Exception
    {
        Map<String,Value> authToken = new HashMap<>();
        authToken.put( "username", value( "bob" ) );
        authToken.put( "password", value( "secret" ) );

        encoder.encode( new HelloMessage( "MyDriver", authToken ), packer );

        InOrder order = inOrder( packer );
        order.verify( packer ).packStructHeader( 1, HelloMessage.SIGNATURE );

        Map<String,Value> expectedMetadata = new HashMap<>( authToken );
        expectedMetadata.put( "user_agent", value( "MyDriver" ) );
        order.verify( packer ).pack( expectedMetadata );
    }

    @Test
    void shouldFailToEncodeWrongMessage()
    {
        assertThrows( IllegalArgumentException.class, () -> encoder.encode( PULL_ALL, packer ) );
    }
}
