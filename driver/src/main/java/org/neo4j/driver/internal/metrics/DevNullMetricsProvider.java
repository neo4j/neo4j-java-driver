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
package org.neo4j.driver.internal.metrics;

import org.neo4j.bolt.connection.MetricsListener;
import org.neo4j.driver.Metrics;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.GqlStatusError;

public enum DevNullMetricsProvider implements MetricsProvider {
    INSTANCE;

    @Override
    public Metrics metrics() {
        // To outside users, we forbid access to the metrics API
        var message =
                "Driver metrics are not enabled. You need to enable driver metrics in driver configuration in order to access them.";
        throw new ClientException(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                "N/A",
                message,
                GqlStatusError.DIAGNOSTIC_RECORD,
                null);
    }

    @Override
    public MetricsListener metricsListener() {
        // Internally we can still register callbacks to this empty metrics listener.
        return DevNullMetricsListener.INSTANCE;
    }
}
