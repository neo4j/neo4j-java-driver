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

public interface Histogram
{
    /**
     * The minimum value recorded in this histogram.
     * @return The minimum value recorded in this histogram.
     */
    long min();

    /**
     * The maximum value recorded in this histogram.
     * @return The maximum value recorded in this histogram.
     */
    long max();

    /**
     * The mean value of this histogram.
     * @return The mean value.
     */
    double mean();

    /**
     * The standard deviation of this histogram.
     * @return The standard deviation.
     */
    double stdDeviation();

    /**
     * The total count of the values recorded in this histogram.
     * @return The total number of values.
     */
    long totalCount();

    /**
     * Returns the value at the given percentile.
     * @param percentile The interested percentile such as 50 (mean value), 80 etc.
     * @return The value at the given percentile.
     */
    long valueAtPercentile(double percentile);

    /**
     * Reset cleans all the values recorded in this histogram.
     */
    void reset();
}
