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

import org.junit.jupiter.api.Test;

import org.neo4j.driver.internal.metrics.spi.Histogram;

import static java.lang.Math.abs;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.junit.MatcherAssert.assertThat;

class InternalHistogramTest
{
    @Test
    void shouldRecordSmallValuesPrecisely()
    {
        // Given
        InternalHistogram histogram = new InternalHistogram();
        histogram.recordValue( 0 );
        histogram.recordValue( 1 );
        histogram.recordValue( 2 );
        histogram.recordValue( 3 );
        histogram.recordValue( 4 );

        // When
        assertThat( histogram.min(), equalTo( 0L ) );
        assertThat( histogram.max(), equalTo( 4L ) );
        assertThat( histogram.mean(), equalTo( 2.0 ) );
        assertThat( histogram.totalCount(), equalTo( 5L ) );
    }

    @Test
    void shouldRecordBigValuesPrecisely()
    {
        // Given
        InternalHistogram histogram = new InternalHistogram();
        histogram.recordValue( 0 );
        histogram.recordValue( 100000 );
        histogram.recordValue( 200000 );
        histogram.recordValue( 300000 );
        histogram.recordValue( 400000 );

        // When
        assertThat( histogram.min(), equalTo( 0L ) );
        assertThat( abs( histogram.max() - 400000L ), lessThan( 500L ) );
        assertThat( abs( histogram.mean() - 200000.0 ), lessThan( 500.0 ) );
        assertThat( histogram.totalCount(), equalTo( 5L ) );
    }

    @Test
    void shouldResetOnOriginalHistogram()
    {
        // Given
        InternalHistogram histogram = new InternalHistogram();
        histogram.recordValue( 0 );
        histogram.recordValue( 1 );
        histogram.recordValue( 2 );
        histogram.recordValue( 3 );
        histogram.recordValue( 4 );

        // When
        assertThat( histogram.totalCount(), equalTo( 5L ) );
        Histogram snapshot = histogram.snapshot();
        snapshot.reset();

        // Then
        assertThat( histogram.totalCount(), equalTo( 0L ) );
    }

}
