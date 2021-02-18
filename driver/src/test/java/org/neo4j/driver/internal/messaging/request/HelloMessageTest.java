/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal.messaging.request;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.Value;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.Values.NULL;
import static org.neo4j.driver.internal.security.InternalAuthToken.CREDENTIALS_KEY;
import static org.neo4j.driver.internal.security.InternalAuthToken.PRINCIPAL_KEY;
import static org.neo4j.driver.Values.value;

class HelloMessageTest
{
    @Test
    void shouldHaveCorrectMetadata()
    {
        Map<String,Value> authToken = new HashMap<>();
        authToken.put( "user", value( "Alice" ) );
        authToken.put( "credentials", value( "SecretPassword" ) );

        HelloMessage message = new HelloMessage( "MyDriver/1.0.2", authToken, Collections.emptyMap() );

        Map<String,Value> expectedMetadata = new HashMap<>( authToken );
        expectedMetadata.put( "user_agent", value( "MyDriver/1.0.2" ) );
        expectedMetadata.put( "routing", value ( Collections.emptyMap() ) );
        assertEquals( expectedMetadata, message.metadata() );
    }

    @Test
    void shouldHaveCorrectRoutingContext()
    {
        Map<String,Value> authToken = new HashMap<>();
        authToken.put( "user", value( "Alice" ) );
        authToken.put( "credentials", value( "SecretPassword" ) );

        Map<String,String> routingContext = new HashMap<>();
        routingContext.put( "region", "China" );
        routingContext.put( "speed", "Slow" );

        HelloMessage message = new HelloMessage( "MyDriver/1.0.2", authToken, routingContext );

        Map<String,Value> expectedMetadata = new HashMap<>( authToken );
        expectedMetadata.put( "user_agent", value( "MyDriver/1.0.2" ) );
        expectedMetadata.put( "routing", value( routingContext ) );
        assertEquals( expectedMetadata, message.metadata() );
    }


    @Test
    void shouldNotExposeCredentialsInToString()
    {
        Map<String,Value> authToken = new HashMap<>();
        authToken.put( PRINCIPAL_KEY, value( "Alice" ) );
        authToken.put( CREDENTIALS_KEY, value( "SecretPassword" ) );

        HelloMessage message = new HelloMessage( "MyDriver/1.0.2", authToken, Collections.emptyMap() );

        assertThat( message.toString(), not( containsString( "SecretPassword" ) ) );
    }
}
