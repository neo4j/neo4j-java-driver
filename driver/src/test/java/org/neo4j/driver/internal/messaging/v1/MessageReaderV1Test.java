/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.driver.internal.messaging.v1;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;

import org.neo4j.driver.internal.util.messaging.AbstractMessageReaderTestBase;
import org.neo4j.driver.internal.messaging.response.RecordMessage;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.Value;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.messaging.MessageFormat.Reader;
import static org.neo4j.driver.Values.point;
import static org.neo4j.driver.Values.value;

class MessageReaderV1Test extends AbstractMessageReaderTestBase
{
    @Test
    void shouldFailToReadMessageWithTemporalValue()
    {
        Value[] fields = {value( LocalDateTime.now() )};

        assertThrows( IOException.class, () -> testMessageReading( new RecordMessage( fields ) ) );
    }

    @Test
    void shouldFailToReadMessageWithSpatialValue()
    {
        Value[] fields = {point( 42, 1, 2 )};

        assertThrows( IOException.class, () -> testMessageReading( new RecordMessage( fields ) ) );
    }

    @Override
    protected Reader newReader( PackInput input )
    {
        return new MessageReaderV1( input );
    }
}
