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
package org.neo4j.driver.types;

import java.time.temporal.TemporalAmount;

import org.neo4j.driver.Values;
import org.neo4j.driver.util.Immutable;

/**
 * Represents temporal amount containing months, days, seconds and nanoseconds of the second. A duration can be negative.
 * <p>
 * Value that represents a duration can be created using {@link Values#isoDuration(long, long, long, int)} method.
 */
@Immutable
public interface IsoDuration extends TemporalAmount
{
    /**
     * Retrieve amount of months in this duration.
     *
     * @return number of months.
     */
    long months();

    /**
     * Retrieve amount of days in this duration.
     *
     * @return number of days.
     */
    long days();

    /**
     * Retrieve amount of seconds in this duration.
     *
     * @return number of seconds.
     */
    long seconds();

    /**
     * Retrieve amount of nanoseconds of the second in this duration.
     *
     * @return number of nanoseconds.
     */
    int nanoseconds();
}
