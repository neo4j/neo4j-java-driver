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
package org.neo4j.driver.internal.messaging.v2;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import org.neo4j.driver.internal.messaging.AbstractMessageWriterTestBase;
import org.neo4j.driver.internal.messaging.MessageFormat.Writer;
import org.neo4j.driver.internal.messaging.request.RunMessage;
import org.neo4j.driver.internal.packstream.PackOutput;

import static java.util.Collections.singletonMap;
import static org.neo4j.driver.v1.Values.point;
import static org.neo4j.driver.v1.Values.value;

class MessageWriterV2Test extends AbstractMessageWriterTestBase
{
    @Test
    void shouldWriteMessageWithTemporalValue() throws Exception
    {
        RunMessage message = new RunMessage( "RETURN $now", singletonMap( "now", value( LocalDateTime.now() ) ) );

        testMessageWriting( message, 2 );
    }

    @Test
    void shouldWriteMessageWithSpatialValue() throws Exception
    {
        RunMessage message = new RunMessage( "RETURN $here", singletonMap( "now", point( 42, 1, 1 ) ) );

        testMessageWriting( message, 2 );
    }

    @Override
    protected Writer newWriter( PackOutput output )
    {
        return new MessageWriterV2( output );
    }
}
