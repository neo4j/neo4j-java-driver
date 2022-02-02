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
package org.neo4j.driver.internal.metrics;

import org.neo4j.driver.metrics.ListenerEvent;
import org.neo4j.driver.metrics.ConnectionPoolMetricsListener;

public enum DevNullPoolMetricsListener implements ConnectionPoolMetricsListener
{
    INSTANCE;

    @Override
    public void beforeCreating( ListenerEvent<?> listenerEvent )
    {
    }

    @Override
    public void afterCreated( ListenerEvent<?> listenerEvent )
    {
    }

    @Override
    public void afterFailedToCreate()
    {
    }

    @Override
    public void afterClosed()
    {
    }

    @Override
    public void beforeAcquiringOrCreating( ListenerEvent<?> acquireEvent )
    {
    }

    @Override
    public void afterAcquiringOrCreating()
    {
    }

    @Override
    public void afterAcquiredOrCreated( ListenerEvent<?> acquireEvent )
    {
    }

    @Override
    public void afterTimedOutToAcquireOrCreate()
    {
    }

    @Override
    public void acquired( ListenerEvent<?> inUseEvent )
    {
    }

    @Override
    public void released( ListenerEvent<?> inUseEvent )
    {
    }
}