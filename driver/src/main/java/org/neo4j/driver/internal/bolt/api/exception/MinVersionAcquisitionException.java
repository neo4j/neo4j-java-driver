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
package org.neo4j.driver.internal.bolt.api.exception;

import java.io.Serial;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;

public class MinVersionAcquisitionException extends Neo4jException {
    @Serial
    private static final long serialVersionUID = 2620821821322630443L;

    private final BoltProtocolVersion version;

    public MinVersionAcquisitionException(String message, BoltProtocolVersion version) {
        super(message);
        this.version = version;
    }

    public BoltProtocolVersion version() {
        return version;
    }
}
