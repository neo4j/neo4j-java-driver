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
package org.neo4j.driver.internal.messaging.v2;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;

import org.neo4j.driver.internal.util.messaging.AbstractMessageReaderTestBase;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.ResponseMessageHandler;
import org.neo4j.driver.internal.messaging.response.RecordMessage;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.Value;

import static org.mockito.Mockito.verify;
import static org.neo4j.driver.Values.point;
import static org.neo4j.driver.Values.value;

class MessageReaderV2Test extends AbstractMessageReaderTestBase
{
    @Test
    void shouldReadMessageWithTemporalValues() throws Exception
    {
        Value[] fields = {value( LocalDateTime.now() ), value( OffsetTime.now() ), value( ZonedDateTime.now() )};

        ResponseMessageHandler handler = testMessageReading( new RecordMessage( fields ) );

        verify( handler ).handleRecordMessage( fields );
    }

    @Test
    void shouldReadMessageWithSpatialValues() throws Exception
    {
        Value[] fields = {point( 42, 1.1, 2.2 ), point( 4242, 3.3, 4.4 ), point( 24, 5.5, 6.6, 7.7 ), point( 2424, 8.8, 9.9, 10.1 )};

        ResponseMessageHandler handler = testMessageReading( new RecordMessage( fields ) );

        verify( handler ).handleRecordMessage( fields );
    }

    @Override
    protected MessageFormat.Reader newReader( PackInput input )
    {
        return new MessageReaderV2( input );
    }
}
