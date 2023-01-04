/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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

import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;

import java.io.Serializable;

/**
 * Internal implementation of the java platform logging module.
 * <b>This class should not be used directly.</b> Please use {@link Logging#javaPlatformLogging()} factory method instead.
 *
 * @see Logging#javaPlatformLogging()
 */
public class JavaPlatformLogging implements Logging, Serializable {
    private static final long serialVersionUID = -1145576859241657833L;

    public JavaPlatformLogging() {
    }

    @Override
    public Logger getLog(String name) {
        return new JavaPlatformLogger(System.getLogger(name));
    }
}
