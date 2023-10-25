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
package org.neo4j.driver.internal.summary;

import java.util.Objects;
import org.neo4j.driver.summary.DatabaseInfo;

public record InternalDatabaseInfo(String name) implements DatabaseInfo {
    public static final DatabaseInfo DEFAULT_DATABASE_INFO = new InternalDatabaseInfo(null);

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (InternalDatabaseInfo) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public String toString() {
        return "InternalDatabaseInfo{" + "name='" + name + '\'' + '}';
    }
}
