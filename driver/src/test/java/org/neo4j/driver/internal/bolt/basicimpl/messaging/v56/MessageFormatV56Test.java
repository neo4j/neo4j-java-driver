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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.v56;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v5.MessageReaderV5;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v54.MessageWriterV54;
import org.neo4j.driver.internal.bolt.basicimpl.packstream.PackInput;
import org.neo4j.driver.internal.bolt.basicimpl.packstream.PackOutput;

class MessageFormatV56Test {
    private static final MessageFormat format = BoltProtocolV56.INSTANCE.createMessageFormat();

    @Test
    void shouldCreateCorrectWriter() {
        var writer = format.newWriter(mock(PackOutput.class));

        assertThat(writer, instanceOf(MessageWriterV54.class));
    }

    @Test
    void shouldCreateCorrectReader() {
        var reader = format.newReader(mock(PackInput.class));

        assertThat(reader, instanceOf(MessageReaderV5.class));
    }
}
