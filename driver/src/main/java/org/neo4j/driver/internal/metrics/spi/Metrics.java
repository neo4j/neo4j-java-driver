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

import java.util.Map;

public interface Metrics
{
    // TODO Once this interface become public, find a better way to enable metrics and detect metrics availability.
    String DRIVER_METRICS_ENABLED_KEY = "driver.metrics.enabled";
    static boolean isMetricsEnabled()
    {
        return Boolean.getBoolean( DRIVER_METRICS_ENABLED_KEY );
    }

    /**
     * A map of connection pool metrics.
     * The {@link ConnectionPoolMetrics#uniqueName()} are used as the keys of the map.
     * @return The connection pool metrics.
     */
    Map<String, ConnectionPoolMetrics> connectionPoolMetrics();

    /***
     * A map of connection metrics.
     * The {@link ConnectionMetrics#uniqueName()} are used as the keys of the map.
     * @return The connection metrics.
     */
    Map<String,ConnectionMetrics> connectionMetrics();

}
