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
package org.neo4j.driver.internal.messaging.v57;

import static java.util.Arrays.asList;
import static java.util.Calendar.APRIL;
import static java.util.Calendar.AUGUST;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.Values.value;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.stream.Stream;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.InternalPoint2D;
import org.neo4j.driver.internal.InternalPoint3D;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.DiscardAllMessage;
import org.neo4j.driver.internal.messaging.response.IgnoredMessage;
import org.neo4j.driver.internal.messaging.response.RecordMessage;
import org.neo4j.driver.internal.messaging.response.SuccessMessage;
import org.neo4j.driver.internal.messaging.v56.BoltProtocolV56;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.util.messaging.AbstractMessageReaderTestBase;

public class MessageReaderV57Test extends AbstractMessageReaderTestBase {
    @Override
    protected Stream<Message> supportedMessages() {
        return Stream.of(
                // V2 Record types
                record(value(new InternalPoint2D(42, 120.65, -99.2))),
                record(value(new InternalPoint3D(42, 85.391, 98.8, 11.1))),
                record(value(LocalDate.of(2012, AUGUST, 3))),
                record(value(OffsetTime.of(23, 59, 59, 999, ZoneOffset.MAX))),
                record(value(LocalTime.of(12, 25))),
                record(value(LocalDateTime.of(1999, APRIL, 3, 19, 5, 5, 100_200_300))),
                record(value(Values.isoDuration(
                                Long.MAX_VALUE - 1, Integer.MAX_VALUE - 1, Short.MAX_VALUE - 1, Byte.MAX_VALUE - 1)
                        .asIsoDuration())),
                record(value(Values.isoDuration(17, 22, 99, 15).asIsoDuration())),

                // Bolt previous versions valid messages
                IgnoredMessage.IGNORED,
                new SuccessMessage(new HashMap<>()),
                record(value(1337L)),
                record(value(parameters("cat", null, "dog", null))),
                record(value(parameters("k", 12, "a", "banana"))),
                record(value(asList("k", 12, "a", "banana"))));
    }

    @Override
    protected Stream<Message> unsupportedMessages() {
        return Stream.of(DiscardAllMessage.DISCARD_ALL);
    }

    @Override
    protected MessageFormat.Reader newReader(PackInput input) {
        return BoltProtocolV56.INSTANCE.createMessageFormat().newReader(input);
    }

    private Message record(Value value) {
        return new RecordMessage(new Value[] {value});
    }
}
