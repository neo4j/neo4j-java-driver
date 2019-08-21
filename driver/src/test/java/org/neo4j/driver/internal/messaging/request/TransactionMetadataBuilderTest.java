/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalBookmark;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;
import static org.neo4j.driver.internal.messaging.request.TransactionMetadataBuilder.buildMetadata;

public class TransactionMetadataBuilderTest
{
    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldHaveCorrectMetadata( AccessMode mode )
    {
        InternalBookmark bookmark = InternalBookmark.parse( asList( "neo4j:bookmark:v1:tx11", "neo4j:bookmark:v1:tx52" ) );

        Map<String,Value> txMetadata = new HashMap<>();
        txMetadata.put( "foo", value( "bar" ) );
        txMetadata.put( "baz", value( 111 ) );
        txMetadata.put( "time", value( LocalDateTime.now() ) );

        Duration txTimeout = Duration.ofSeconds( 7 );

        Map<String,Value> metadata = buildMetadata( txTimeout, txMetadata, ABSENT_DB_NAME, mode, bookmark );

        Map<String,Value> expectedMetadata = new HashMap<>();
        expectedMetadata.put( "bookmarks", value( bookmark.values() ) );
        expectedMetadata.put( "tx_timeout", value( 7000 ) );
        expectedMetadata.put( "tx_metadata", value( txMetadata ) );
        if ( mode == READ )
        {
            expectedMetadata.put( "mode", value( "r" ) );
        }

        assertEquals( expectedMetadata, metadata );
    }

    @ParameterizedTest
    @ValueSource( strings = {"", "foo", "data", ABSENT_DB_NAME} )
    void shouldHaveCorrectMetadataForDatabaseName( String databaseName )
    {
        InternalBookmark bookmark = InternalBookmark.parse( asList( "neo4j:bookmark:v1:tx11", "neo4j:bookmark:v1:tx52" ) );

        Map<String,Value> txMetadata = new HashMap<>();
        txMetadata.put( "foo", value( "bar" ) );
        txMetadata.put( "baz", value( 111 ) );
        txMetadata.put( "time", value( LocalDateTime.now() ) );

        Duration txTimeout = Duration.ofSeconds( 7 );

        Map<String,Value> metadata = buildMetadata( txTimeout, txMetadata, databaseName, WRITE, bookmark );

        Map<String,Value> expectedMetadata = new HashMap<>();
        expectedMetadata.put( "bookmarks", value( bookmark.values() ) );
        expectedMetadata.put( "tx_timeout", value( 7000 ) );
        expectedMetadata.put( "tx_metadata", value( txMetadata ) );
        if ( !ABSENT_DB_NAME.equals( databaseName ) )
        {
            expectedMetadata.put( "db", value( databaseName ) );
        }

        assertEquals( expectedMetadata, metadata );
    }
}
