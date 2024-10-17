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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.request;

import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.security.InternalAuthToken.CREDENTIALS_KEY;

import java.util.HashMap;
import java.util.Map;
import org.neo4j.driver.Value;

public class LogonMessage extends MessageWithMetadata {
    public static final byte SIGNATURE = 0x6A;

    public LogonMessage(Map<String, Value> authMap) {
        super(authMap);
    }

    @Override
    public byte signature() {
        return SIGNATURE;
    }

    @Override
    public String toString() {
        Map<String, Value> metadataCopy = new HashMap<>(metadata());
        metadataCopy.replace(CREDENTIALS_KEY, value("******"));
        return "LOGON " + metadataCopy;
    }
}
