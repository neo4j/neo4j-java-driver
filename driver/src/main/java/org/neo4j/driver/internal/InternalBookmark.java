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
package org.neo4j.driver.internal;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import org.neo4j.driver.Bookmark;

public record InternalBookmark(String value) implements Bookmark, Serializable {
    public InternalBookmark {
        requireNonNull(value);
        if (value.isEmpty()) {
            throw new IllegalArgumentException("The value must not be empty");
        }
    }
}
