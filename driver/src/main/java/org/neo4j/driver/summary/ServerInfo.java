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
package org.neo4j.driver.summary;

/**
 * Provides some basic information of the server where the result is obtained from.
 */
public interface ServerInfo
{

    /**
     * Returns a string telling the address of the server the query was executed.
     * @return The address of the server the query was executed.
     */
    String address();

    /**
     * Returns a string telling which version of the server the query was executed.
     * Supported since neo4j 3.1.
     * @return The server version.
     */
    String version();
}
