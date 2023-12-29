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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.v3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.common.CommonMessageReader;
import org.neo4j.driver.internal.bolt.basicimpl.packstream.PackInput;
import org.neo4j.driver.internal.bolt.basicimpl.packstream.PackOutput;

/**
 * The MessageFormat under tests is the one provided by the {@link BoltProtocolV3} and not an specific class implementation.
 * <p>
 * It's done on this way to make easy to replace the implementation and still getting the same behaviour.
 */
class MessageFormatV3Test {
    private static final MessageFormat messageFormat = BoltProtocolV3.INSTANCE.createMessageFormat();

    @Test
    void shouldCreateCorrectWriter() {
        var writer = messageFormat.newWriter(mock(PackOutput.class));

        assertThat(writer, instanceOf(MessageWriterV3.class));
    }

    @Test
    void shouldCreateCorrectReader() {
        var reader = messageFormat.newReader(mock(PackInput.class));

        assertThat(reader, instanceOf(CommonMessageReader.class));
    }
}
