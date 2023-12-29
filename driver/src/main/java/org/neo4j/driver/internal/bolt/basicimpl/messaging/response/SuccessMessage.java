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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.response;

import static java.lang.String.format;

import java.util.Map;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.Message;

/**
 * SUCCESS response message
 * <p>
 * Sent by the server to signal a successful operation.
 * Terminates response sequence.
 */
public record SuccessMessage(Map<String, Value> metadata) implements Message {
    public static final byte SIGNATURE = 0x70;

    @Override
    public byte signature() {
        return SIGNATURE;
    }

    @Override
    public String toString() {
        return format("SUCCESS %s", metadata);
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && obj.getClass() == getClass();
    }

    @Override
    public int hashCode() {
        return 1;
    }
}
