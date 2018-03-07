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
package org.neo4j.driver.v1.types;

import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.util.Immutable;

/**
 * Represents a single two-dimensional point in a particular coordinate reference system.
 * <p>
 * Value that represents a 2D point can be created using {@link Values#point2D(long, double, double)} method.
 */
@Immutable
public interface Point2D
{
    /**
     * Retrieve identifier of the coordinate reference system for this point.
     *
     * @return coordinate reference system identifier.
     */
    long srid();

    /**
     * Retrieve {@code x} coordinate of this point.
     *
     * @return the {@code x} coordinate value.
     */
    double x();

    /**
     * Retrieve {@code y} coordinate of this point.
     *
     * @return the {@code y} coordinate value.
     */
    double y();
}
