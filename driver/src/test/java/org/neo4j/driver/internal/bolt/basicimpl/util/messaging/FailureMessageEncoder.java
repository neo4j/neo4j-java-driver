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
package org.neo4j.driver.internal.bolt.basicimpl.util.messaging;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.Message;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageEncoder;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.ValuePacker;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.response.FailureMessage;

public class FailureMessageEncoder implements MessageEncoder {
    @Override
    public void encode(Message message, ValuePacker packer) throws IOException {
        var failureMessage = (FailureMessage) message;
        packer.packStructHeader(1, failureMessage.signature());
        Map<String, Value> body = new HashMap<>();
        body.put("code", Values.value(failureMessage.code()));
        body.put("message", Values.value(failureMessage.message()));
        packer.pack(body);
    }
}
