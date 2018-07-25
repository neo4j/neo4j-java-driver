package org.neo4j.driver.internal.messaging.request;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.v1.Value;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.internal.security.InternalAuthToken.CREDENTIALS_KEY;
import static org.neo4j.driver.internal.security.InternalAuthToken.PRINCIPAL_KEY;
import static org.neo4j.driver.v1.Values.value;

class HelloMessageTest
{
    @Test
    void shouldHaveCorrectMetadata()
    {
        Map<String,Value> authToken = new HashMap<>();
        authToken.put( "user", value( "Alice" ) );
        authToken.put( "credentials", value( "SecretPassword" ) );

        HelloMessage message = new HelloMessage( "MyDriver/1.0.2", authToken );

        Map<String,Value> expectedMetadata = new HashMap<>( authToken );
        expectedMetadata.put( "user_agent", value( "MyDriver/1.0.2" ) );
        assertEquals( expectedMetadata, message.metadata() );
    }

    @Test
    void shouldNotExposeCredentialsInToString()
    {
        Map<String,Value> authToken = new HashMap<>();
        authToken.put( PRINCIPAL_KEY, value( "Alice" ) );
        authToken.put( CREDENTIALS_KEY, value( "SecretPassword" ) );

        HelloMessage message = new HelloMessage( "MyDriver/1.0.2", authToken );

        assertThat( message.toString(), not( containsString( "SecretPassword" ) ) );
    }
}
