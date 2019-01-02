/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.internal.metrics.spi;

public interface ConnectionMetrics
{
    /**
     * The unique name of this connection metrics among all connection metrics
     * @return An unique name of this connection metrics among all connection metrics
     */
    String uniqueName();

    /**
     * The connection time histogram describes how long it takes to establish a connection
     * @return The connection time histogram
     */
    Histogram connectionTimeHistogram ();

    /**
     * The in-use time histogram records how long each connection is borrowed out of the pool
     * @return The in-use time histogram
     */
    Histogram inUseTimeHistogram();
}
