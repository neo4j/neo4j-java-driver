/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.driver.internal.metrics.spi.ConnectionPoolMetricsTracker;

public final class SimpleConnectionPoolMetricsTracker implements ConnectionPoolMetricsTracker
{
    private final AtomicLong totalConnectionTime = new AtomicLong();
    private final AtomicLong totalAcquisitionTime = new AtomicLong();
    private final AtomicLong totalInUseTime = new AtomicLong();
    private final String poolId;

    public SimpleConnectionPoolMetricsTracker( String poolId )
    {
        this.poolId = poolId;
    }

    @Override
    public void recordAcquisitionTime( long timeInMs )
    {
        this.totalAcquisitionTime.addAndGet( timeInMs );
    }

    @Override
    public void recordConnectionTime( long timeInMs )
    {
        this.totalConnectionTime.addAndGet( timeInMs );
    }

    @Override
    public void recordInUseTime( long timeInMs )
    {
        this.totalInUseTime.addAndGet( timeInMs );
    }

    public long totalAcquisitionTime()
    {
        return this.totalAcquisitionTime.get();
    }

    public long totalConnectionTime()
    {
        return this.totalConnectionTime.get();
    }

    public long totalInUseTime()
    {
        return this.totalInUseTime.get();
    }
}
