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
package org.neo4j.driver.internal.messaging.encode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.messaging.ValuePacker;
import org.neo4j.driver.internal.messaging.request.BeginMessage;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.messaging.request.ResetMessage.RESET;

class BeginMessageEncoderTest
{
    private final BeginMessageEncoder encoder = new BeginMessageEncoder();
    private final ValuePacker packer = mock( ValuePacker.class );

    @ParameterizedTest
    @MethodSource( "arguments" )
    void shouldEncodeBeginMessage( AccessMode mode, String impersonatedUser ) throws Exception
    {
        Set<Bookmark> bookmarks = Collections.singleton( InternalBookmark.parse( "neo4j:bookmark:v1:tx42" ) );

        Map<String,Value> txMetadata = new HashMap<>();
        txMetadata.put( "hello", value( "world" ) );
        txMetadata.put( "answer", value( 42 ) );

        Duration txTimeout = Duration.ofSeconds( 1 );

        encoder.encode( new BeginMessage( bookmarks, txTimeout, txMetadata, mode, defaultDatabase(), impersonatedUser ), packer );

        InOrder order = inOrder( packer );
        order.verify( packer ).packStructHeader( 1, BeginMessage.SIGNATURE );

        Map<String,Value> expectedMetadata = new HashMap<>();
        expectedMetadata.put( "bookmarks", value( bookmarks.stream().map( Bookmark::value ).collect( Collectors.toSet() ) ) );
        expectedMetadata.put( "tx_timeout", value( 1000 ) );
        expectedMetadata.put( "tx_metadata", value( txMetadata ) );
        if ( mode == READ )
        {
            expectedMetadata.put( "mode", value( "r" ) );
        }
        if ( impersonatedUser != null )
        {
            expectedMetadata.put( "imp_user", value( impersonatedUser ) );
        }

        order.verify( packer ).pack( expectedMetadata );
    }

    private static Stream<Arguments> arguments()
    {
        return Arrays.stream( AccessMode.values() )
                     .flatMap( accessMode -> Stream.of( Arguments.of( accessMode, "user" ), Arguments.of( accessMode, null ) ) );
    }

    @Test
    void shouldFailToEncodeWrongMessage()
    {
        assertThrows( IllegalArgumentException.class, () -> encoder.encode( RESET, packer ) );
    }
}
