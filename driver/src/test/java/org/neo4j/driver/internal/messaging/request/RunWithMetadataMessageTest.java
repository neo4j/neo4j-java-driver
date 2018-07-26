/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
