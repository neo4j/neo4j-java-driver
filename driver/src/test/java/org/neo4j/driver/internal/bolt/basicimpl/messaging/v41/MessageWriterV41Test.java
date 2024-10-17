/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.v41;

import static java.time.Duration.ofSeconds;
import static java.util.Calendar.DECEMBER;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.neo4j.driver.AuthTokens.basic;
import static org.neo4j.driver.Values.point;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.bolt.api.AccessMode.READ;
import static org.neo4j.driver.internal.bolt.api.AccessMode.WRITE;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.CommitMessage.COMMIT;
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.GoodbyeMessage.GOODBYE;
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.PullAllMessage.PULL_ALL;
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.ResetMessage.RESET;
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.RollbackMessage.ROLLBACK;
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.RunWithMetadataMessage.autoCommitTxRunMessage;
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.RunWithMetadataMessage.unmanagedTxRunMessage;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.stream.Stream;
import org.neo4j.driver.internal.bolt.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.api.BoltAgentUtil;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.Message;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.BeginMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.DiscardMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.HelloMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.PullMessage;
import org.neo4j.driver.internal.bolt.basicimpl.packstream.PackOutput;
import org.neo4j.driver.internal.bolt.basicimpl.util.messaging.AbstractMessageWriterTestBase;
import org.neo4j.driver.internal.security.InternalAuthToken;

/**
 * The MessageWriter under tests is the one provided by the {@link BoltProtocolV41} and not an specific class
 * implementation.
 * <p>
 * It's done on this way to make easy to replace the implementation and still getting the same behaviour.
 */
class MessageWriterV41Test extends AbstractMessageWriterTestBase {
    @Override
    protected MessageFormat.Writer newWriter(PackOutput output) {
        return BoltProtocolV41.INSTANCE.createMessageFormat().newWriter(output);
    }

    @Override
    protected Stream<Message> supportedMessages() {
        return Stream.of(
                // Bolt V2 Data Types
                unmanagedTxRunMessage("RETURN $point", singletonMap("point", point(42, 12.99, -180.0))),
                unmanagedTxRunMessage("RETURN $point", singletonMap("point", point(42, 0.51, 2.99, 100.123))),
                unmanagedTxRunMessage("RETURN $date", singletonMap("date", value(LocalDate.ofEpochDay(2147483650L)))),
                unmanagedTxRunMessage(
                        "RETURN $time", singletonMap("time", value(OffsetTime.of(4, 16, 20, 999, ZoneOffset.MIN)))),
                unmanagedTxRunMessage("RETURN $time", singletonMap("time", value(LocalTime.of(12, 9, 18, 999_888)))),
                unmanagedTxRunMessage(
                        "RETURN $dateTime",
                        singletonMap("dateTime", value(LocalDateTime.of(2049, DECEMBER, 12, 17, 25, 49, 199)))),
                unmanagedTxRunMessage(
                        "RETURN $dateTime",
                        singletonMap(
                                "dateTime",
                                value(ZonedDateTime.of(
                                        2000, 1, 10, 12, 2, 49, 300, ZoneOffset.ofHoursMinutes(9, 30))))),
                unmanagedTxRunMessage(
                        "RETURN $dateTime",
                        singletonMap(
                                "dateTime",
                                value(ZonedDateTime.of(2000, 1, 10, 12, 2, 49, 300, ZoneId.of("Europe/Stockholm"))))),

                // New Bolt V4 messages
                new PullMessage(100, 200),
                new DiscardMessage(300, 400),

                // Bolt V3 messages
                new HelloMessage(
                        "MyDriver/1.2.3",
                        BoltAgentUtil.VALUE,
                        ((InternalAuthToken) basic("neo4j", "neo4j")).toMap(),
                        Collections.emptyMap(),
                        false,
                        null,
                        false),
                GOODBYE,
                new BeginMessage(
                        Collections.singleton("neo4j:bookmark:v1:tx123"),
                        ofSeconds(5),
                        singletonMap("key", value(42)),
                        READ,
                        defaultDatabase(),
                        null,
                        null,
                        null,
                        false,
                        NoopLoggingProvider.INSTANCE),
                new BeginMessage(
                        Collections.singleton("neo4j:bookmark:v1:tx123"),
                        ofSeconds(5),
                        singletonMap("key", value(42)),
                        WRITE,
                        database("foo"),
                        null,
                        null,
                        null,
                        false,
                        NoopLoggingProvider.INSTANCE),
                COMMIT,
                ROLLBACK,
                RESET,
                autoCommitTxRunMessage(
                        "RETURN 1",
                        Collections.emptyMap(),
                        ofSeconds(5),
                        singletonMap("key", value(42)),
                        defaultDatabase(),
                        READ,
                        Collections.singleton("neo4j:bookmark:v1:tx1"),
                        null,
                        null,
                        false,
                        NoopLoggingProvider.INSTANCE),
                autoCommitTxRunMessage(
                        "RETURN 1",
                        Collections.emptyMap(),
                        ofSeconds(5),
                        singletonMap("key", value(42)),
                        database("foo"),
                        WRITE,
                        Collections.singleton("neo4j:bookmark:v1:tx1"),
                        null,
                        null,
                        false,
                        NoopLoggingProvider.INSTANCE),
                unmanagedTxRunMessage("RETURN 1", Collections.emptyMap()),

                // Bolt V3 messages with struct values
                autoCommitTxRunMessage(
                        "RETURN $x",
                        singletonMap("x", value(ZonedDateTime.now())),
                        ofSeconds(1),
                        emptyMap(),
                        defaultDatabase(),
                        READ,
                        Collections.emptySet(),
                        null,
                        null,
                        false,
                        NoopLoggingProvider.INSTANCE),
                autoCommitTxRunMessage(
                        "RETURN $x",
                        singletonMap("x", value(ZonedDateTime.now())),
                        ofSeconds(1),
                        emptyMap(),
                        database("foo"),
                        WRITE,
                        Collections.emptySet(),
                        null,
                        null,
                        false,
                        NoopLoggingProvider.INSTANCE),
                unmanagedTxRunMessage("RETURN $x", singletonMap("x", point(42, 1, 2, 3))));
    }

    @Override
    protected Stream<Message> unsupportedMessages() {
        return Stream.of(
                // Bolt V1, V2 and V3 messages
                PULL_ALL, DISCARD_ALL);
    }
}
