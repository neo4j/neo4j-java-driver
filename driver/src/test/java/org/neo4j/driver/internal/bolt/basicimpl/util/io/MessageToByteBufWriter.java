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
package org.neo4j.driver.internal.bolt.basicimpl.util.io;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.Message;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageFormat;

public class MessageToByteBufWriter {
    private final MessageFormat messageFormat;

    public MessageToByteBufWriter(MessageFormat messageFormat) {
        this.messageFormat = messageFormat;
    }

    public ByteBuf asByteBuf(Message message) {
        try {
            var buf = Unpooled.buffer();
            var output = new ByteBufOutput(buf);
            messageFormat.newWriter(output).write(message);
            return buf;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
