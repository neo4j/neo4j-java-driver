/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.v1;

/**
 * A {@link Result} that has been fully retrieved and stored from the server.
 * It can therefore be kept outside the scope of the current transaction, iterated over multiple times and used while
 * other statements are issued.
 * For instance:
 * <p>
 * {@code
 * for(Record v : session.run( ".." ).retain() )
 * {
 * session.run( ".." );
 * }
 * }
 */
public interface ReusableResult extends Iterable<Record>
{
    /**
     * The number of values in this result.
     *
     * @return the result record count
     */
    long size();

    /**
     * Retrieve a record from this result based on its position in the original result stream.
     *
     * @param index retrieve an item in the result by index
     * @return the requested record
     */
    Record get( long index );
}
