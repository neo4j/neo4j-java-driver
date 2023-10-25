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
package org.neo4j.driver.exceptions.value;

import java.io.Serial;

/**
 * A <em>NotMultiValued</em> exception indicates that the value does not consist of multiple values, a.k.a. not a map
 * or array.
 * @since 1.0
 */
public class NotMultiValued extends ValueException {
    @Serial
    private static final long serialVersionUID = -7380569883011364090L;

    /**
     * Creates a new instance.
     * @param message the message
     */
    public NotMultiValued(String message) {
        super(message);
    }
}
