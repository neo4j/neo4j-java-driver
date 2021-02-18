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
package org.neo4j.driver.internal.messaging.v4;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.stream.Stream;

import org.neo4j.driver.Query;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.BeginMessage;
import org.neo4j.driver.internal.messaging.request.DiscardMessage;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.request.InitMessage;
import org.neo4j.driver.internal.messaging.request.PullMessage;
import org.neo4j.driver.internal.messaging.request.RunMessage;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.util.messaging.AbstractMessageWriterTestBase;

import static java.time.Duration.ofSeconds;
import static java.util.Calendar.DECEMBER;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.AuthTokens.basic;
import static org.neo4j.driver.Values.point;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.messaging.request.CommitMessage.COMMIT;
import static org.neo4j.driver.internal.messaging.request.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.driver.internal.messaging.request.GoodbyeMessage.GOODBYE;
import static org.neo4j.driver.internal.messaging.request.PullAllMessage.PULL_ALL;
import static org.neo4j.driver.internal.messaging.request.ResetMessage.RESET;
import static org.neo4j.driver.internal.messaging.request.RollbackMessage.ROLLBACK;
import static org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage.autoCommitTxRunMessage;
import static org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage.unmanagedTxRunMessage;

/**
 * The MessageWriter under tests is the one provided by the {@link BoltProtocolV3} and not an specific class implementation.
 * <p>
 * It's done on this way to make easy to replace the implementation and still getting the same behaviour.
 */
class MessageWriterV4Test extends AbstractMessageWriterTestBase
{
    @Override
    protected MessageFormat.Writer newWriter( PackOutput output )
    {
        return BoltProtocolV4.INSTANCE.createMessageFormat().newWriter( output );
    }

    @Override
    protected Stream<Message> supportedMessages()
    {
        return Stream.of(
                // Bolt V2 Data Types
                unmanagedTxRunMessage( new Query( "RETURN $point", singletonMap( "point", point( 42, 12.99, -180.0 ) ) ) ),
                unmanagedTxRunMessage( new Query( "RETURN $point", singletonMap( "point", point( 42, 0.51, 2.99, 100.123 ) ) ) ),
                unmanagedTxRunMessage( new Query( "RETURN $date", singletonMap( "date", value( LocalDate.ofEpochDay( 2147483650L ) ) ) ) ),
                unmanagedTxRunMessage( new Query( "RETURN $time", singletonMap( "time", value( OffsetTime.of( 4, 16, 20, 999, ZoneOffset.MIN ) ) ) ) ),
                unmanagedTxRunMessage( new Query( "RETURN $time", singletonMap( "time", value( LocalTime.of( 12, 9, 18, 999_888 ) ) ) ) ),
                unmanagedTxRunMessage(
                        new Query( "RETURN $dateTime", singletonMap( "dateTime", value( LocalDateTime.of( 2049, DECEMBER, 12, 17, 25, 49, 199 ) ) ) ) ),
                unmanagedTxRunMessage( new Query( "RETURN $dateTime", singletonMap( "dateTime", value( ZonedDateTime.of( 2000, 1, 10, 12, 2, 49, 300, ZoneOffset
                        .ofHoursMinutes( 9, 30 ) ) ) ) ) ),
                unmanagedTxRunMessage( new Query( "RETURN $dateTime", singletonMap( "dateTime", value( ZonedDateTime.of( 2000, 1, 10, 12, 2, 49, 300, ZoneId.of(
                        "Europe/Stockholm" ) ) ) ) ) ),

                // New Bolt V4 messages
                new PullMessage( 100, 200 ),
                new DiscardMessage( 300, 400 ),

                // Bolt V3 messages
                new HelloMessage( "MyDriver/1.2.3", ((InternalAuthToken) basic( "neo4j", "neo4j" )).toMap(), Collections.emptyMap() ),
                GOODBYE,
                new BeginMessage( InternalBookmark.parse( "neo4j:bookmark:v1:tx123" ), ofSeconds( 5 ), singletonMap( "key", value( 42 ) ), READ,
                                  defaultDatabase() ),
                new BeginMessage( InternalBookmark.parse( "neo4j:bookmark:v1:tx123" ), ofSeconds( 5 ), singletonMap( "key", value( 42 ) ), WRITE,
                                  database( "foo" ) ),
                COMMIT,
                ROLLBACK,

                RESET,
                autoCommitTxRunMessage( new Query( "RETURN 1" ), ofSeconds( 5 ), singletonMap( "key", value( 42 ) ), defaultDatabase(), READ,
                                        InternalBookmark.parse( "neo4j:bookmark:v1:tx1" ) ),
                autoCommitTxRunMessage( new Query( "RETURN 1" ), ofSeconds( 5 ), singletonMap( "key", value( 42 ) ), database( "foo" ), WRITE,
                                        InternalBookmark.parse( "neo4j:bookmark:v1:tx1" ) ),
                unmanagedTxRunMessage( new Query( "RETURN 1" ) ),

                // Bolt V3 messages with struct values
                autoCommitTxRunMessage( new Query( "RETURN $x", singletonMap( "x", value( ZonedDateTime.now() ) ) ), ofSeconds( 1 ), emptyMap(),
                                        defaultDatabase(), READ, InternalBookmark.empty() ),
                autoCommitTxRunMessage( new Query( "RETURN $x", singletonMap( "x", value( ZonedDateTime.now() ) ) ), ofSeconds( 1 ), emptyMap(), database( "foo" ),
                                        WRITE, InternalBookmark.empty() ),
                unmanagedTxRunMessage( new Query( "RETURN $x", singletonMap( "x", point( 42, 1, 2, 3 ) )  ) )
        );
    }

    @Override
    protected Stream<Message> unsupportedMessages()
    {
        return Stream.of(
                // Bolt V1, V2 and V3 messages
                new InitMessage( "Apa", emptyMap() ),
                new RunMessage( "RETURN 1" ),
                PULL_ALL,
                DISCARD_ALL
        );
    }
}
