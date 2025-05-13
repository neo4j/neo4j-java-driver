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
package org.neo4j.driver.internal.logging;

import java.io.Serial;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;

@SuppressWarnings("deprecation")
public final class SystemLogging implements Logging {
    public static final SystemLogging INSTANCE = new SystemLogging();

    @Serial
    private static final long serialVersionUID = -2244895422671419713L;

    @Override
    public Logger getLog(Class<?> clazz) {
        return new SystemLogger(System.getLogger(clazz.getName()));
    }

    @Override
    public Logger getLog(String name) {
        return new SystemLogger(System.getLogger(name));
    }
}
