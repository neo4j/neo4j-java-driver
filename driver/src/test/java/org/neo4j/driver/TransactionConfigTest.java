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
package org.neo4j.driver;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.exceptions.ClientException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.Values.value;

class TransactionConfigTest
{
    @Test
    void emptyConfigShouldHaveNoTimeout()
    {
        assertNull( TransactionConfig.empty().timeout() );
    }

    @Test
    void emptyConfigShouldHaveNoMetadata()
    {
        assertEquals( emptyMap(), TransactionConfig.empty().metadata() );
    }

    @Test
    void shouldDisallowNullTimeout()
    {
        assertThrows( NullPointerException.class, () -> TransactionConfig.builder().withTimeout( null ) );
    }

    @Test
    void shouldDisallowZeroTimeout()
    {
        assertThrows( IllegalArgumentException.class, () -> TransactionConfig.builder().withTimeout( Duration.ZERO ) );
    }

    @Test
    void shouldDisallowNegativeTimeout()
    {
        assertThrows( IllegalArgumentException.class, () -> TransactionConfig.builder().withTimeout( Duration.ofSeconds( -1 ) ) );
    }

    @Test
    void shouldDisallowNullMetadata()
    {
        assertThrows( NullPointerException.class, () -> TransactionConfig.builder().withMetadata( null ) );
    }

    @Test
    void shouldDisallowMetadataWithIllegalValues()
    {
        assertThrows( ClientException.class,
                () -> TransactionConfig.builder().withMetadata( singletonMap( "key", new InternalNode( 1 ) ) ) );

        assertThrows( ClientException.class,
                () -> TransactionConfig.builder().withMetadata( singletonMap( "key", new InternalRelationship( 1, 1, 1, "" ) ) ) );

        assertThrows( ClientException.class,
                () -> TransactionConfig.builder().withMetadata( singletonMap( "key", new InternalPath( new InternalNode( 1 ) ) ) ) );
    }

    @Test
    void shouldHaveTimeout()
    {
        TransactionConfig config = TransactionConfig.builder()
                .withTimeout( Duration.ofSeconds( 3 ) )
                .build();

        assertEquals( Duration.ofSeconds( 3 ), config.timeout() );
    }

    @Test
    void shouldHaveMetadata()
    {
        Map<String,Object> map = new HashMap<>();
        map.put( "key1", "value1" );
        map.put( "key2", true );
        map.put( "key3", 42 );

        TransactionConfig config = TransactionConfig.builder()
                .withMetadata( map )
                .build();

        Map<String,Value> metadata = config.metadata();

        assertEquals( 3, metadata.size() );
        assertEquals( value( "value1" ), metadata.get( "key1" ) );
        assertEquals( value( true ), metadata.get( "key2" ) );
        assertEquals( value( 42 ), metadata.get( "key3" ) );
    }
}
