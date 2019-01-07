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
package org.neo4j.driver.internal.metrics;

import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.ConcurrentHistogram;

import java.time.Duration;

import org.neo4j.driver.internal.metrics.spi.Histogram;

import static java.lang.String.format;

public class InternalHistogram implements Histogram
{
    private static final long DEFAULT_HIGHEST_TRACKABLE_NS = Duration.ofMinutes( 10 ).toNanos();
    private static final int DEFAULT_NUMBER_OF_SIGNIFICANT_VALUE_DIGITS = 3;

    private final AbstractHistogram delegate;

    public InternalHistogram()
    {
        this( DEFAULT_HIGHEST_TRACKABLE_NS );
    }

    public InternalHistogram( long highestTrackableValueNS )
    {
        this.delegate = createHdrHistogram( highestTrackableValueNS );
    }

    public InternalHistogram( AbstractHistogram histogram )
    {
        this.delegate = histogram;
    }

    public void recordValue( long value )
    {
        long newValue = truncateValue( value, delegate );
        this.delegate.recordValue( newValue );
    }

    @Override
    public long min()
    {
        return delegate.getMinValue();
    }

    @Override
    public long max()
    {
        return delegate.getMaxValue();
    }

    @Override
    public double mean()
    {
        return delegate.getMean();
    }

    @Override
    public double stdDeviation()
    {
        return delegate.getStdDeviation();
    }

    @Override
    public long totalCount()
    {
        return delegate.getTotalCount();
    }

    @Override
    public long valueAtPercentile( double percentile )
    {
        return delegate.getValueAtPercentile( percentile );
    }

    @Override
    public void reset()
    {
        delegate.reset();
    }

    public static ConcurrentHistogram createHdrHistogram( long highestTrackableValue )
    {
        return new ConcurrentHistogram( highestTrackableValue, DEFAULT_NUMBER_OF_SIGNIFICANT_VALUE_DIGITS );
    }

    private static long truncateValue( long value, AbstractHistogram histogram )
    {
        if ( value > histogram.getHighestTrackableValue() )
        {
            return histogram.getHighestTrackableValue();
        }
        else
        {
            return value;
        }
    }

    public Histogram snapshot()
    {
        return new HistogramSnapshot( new InternalHistogram( this.delegate.copy() ), this );
    }

    @Override
    public String toString()
    {
        return format("[min=%sns, max=%sns, mean=%sns, stdDeviation=%s, totalCount=%s]",
                min(), max(), mean(), stdDeviation(), totalCount());
    }
}
