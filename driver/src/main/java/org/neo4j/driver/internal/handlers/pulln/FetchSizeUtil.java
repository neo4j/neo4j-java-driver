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
package org.neo4j.driver.internal.handlers.pulln;

public class FetchSizeUtil
{
    public static final long UNLIMITED_FETCH_SIZE = -1;
    public static final long DEFAULT_FETCH_SIZE = 1000;

    public static long assertValidFetchSize( long size )
    {
        if ( size <= 0 && size != UNLIMITED_FETCH_SIZE )
        {
            throw new IllegalArgumentException( String.format( "The record fetch size may not be 0 or negative. Illegal record fetch size: %s.", size ) );
        }
        return size;
    }
}
