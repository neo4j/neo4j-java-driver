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
package org.neo4j.driver.internal.async.pool;

import java.util.concurrent.TimeUnit;

public record PoolSettings(
        int maxConnectionPoolSize,
        long connectionAcquisitionTimeout,
        long maxConnectionLifetime,
        long idleTimeBeforeConnectionTest) {
    public static final int NOT_CONFIGURED = -1;

    public static final int DEFAULT_MAX_CONNECTION_POOL_SIZE = 100;
    public static final long DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST = NOT_CONFIGURED;
    public static final long DEFAULT_MAX_CONNECTION_LIFETIME = TimeUnit.HOURS.toMillis(1);
    public static final long DEFAULT_CONNECTION_ACQUISITION_TIMEOUT = TimeUnit.SECONDS.toMillis(60);

    public boolean idleTimeBeforeConnectionTestEnabled() {
        return idleTimeBeforeConnectionTest >= 0;
    }

    public boolean maxConnectionLifetimeEnabled() {
        return maxConnectionLifetime > 0;
    }
}
