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
package org.neo4j.driver.types;

import java.util.Optional;

/**
 * An object instance of {@link TypeSystem#UNSUPPORTED()} type.
 * <p>
 * This object holds information about the unsupported type and the {@link #minProtocolVersion()} needed to support it.
 * <p>
 * Note that this object MUST NOT be sent to the server.
 *
 * @since 6.0.0
 */
public interface UnsupportedType {
    /**
     * Returns the type name.
     * @return the type name
     */
    String name();

    /**
     * The minimum Bolt Protocol version needed to support this type.
     * @return the minimum Bolt Protocol version
     */
    String minProtocolVersion();

    /**
     * An optional message.
     * @return the message
     */
    Optional<String> message();
}
