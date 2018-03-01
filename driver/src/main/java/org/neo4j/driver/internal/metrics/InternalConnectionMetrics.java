/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.time.Duration;
import java.util.Objects;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.metrics.spi.ConnectionMetrics;
import org.neo4j.driver.internal.metrics.spi.Histogram;


public class InternalConnectionMetrics implements ConnectionMetrics, ConnectionMetricsListener
{
    private final InternalHistogram connHistogram;
    private final InternalHistogram inUseHistogram;
    private final BoltServerAddress serverAddress;

    public InternalConnectionMetrics( BoltServerAddress serverAddress, int connectionTimeoutMillis )
    {
        Objects.requireNonNull( serverAddress );
        this.serverAddress = serverAddress;
        connHistogram = new InternalHistogram( Duration.ofMillis( connectionTimeoutMillis ).toNanos() );
        inUseHistogram = new InternalHistogram();
    }

    @Override
    public String uniqueName()
    {
        return serverAddress.toString();
    }

    @Override
    public Histogram connectionTimeHistogram()
    {
        return connHistogram.snapshot();
    }

    @Override
    public Histogram inUseTimeHistogram()
    {
        return inUseHistogram.snapshot();
    }

    @Override
    public void beforeCreating( ListenerEvent connEvent )
    {
         // creating a conn
        connEvent.start();
    }

    @Override
    public void afterCreating( ListenerEvent connEvent )
    {
        // finished conn creation
        long elapsed = connEvent.elapsed();
        connHistogram.recordValue( elapsed );
    }

    @Override
    public void acquiredOrCreated( ListenerEvent inUseEvent )
    {
        // created + acquired = inUse
        inUseEvent.start();
    }

    @Override
    public void released(ListenerEvent inUseEvent)
    {
        // idle
        long elapsed = inUseEvent.elapsed();
        inUseHistogram.recordValue( elapsed );
    }

    @Override
    public String toString()
    {
        return String.format( "connectionTimeHistogram=%s, inUseTimeHistogram=%s", connectionTimeHistogram(), inUseTimeHistogram() );
    }

}
