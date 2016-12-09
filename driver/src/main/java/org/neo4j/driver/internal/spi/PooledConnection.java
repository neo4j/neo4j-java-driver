/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.internal.spi;

import org.neo4j.driver.internal.exceptions.InvalidOperationException;

public interface PooledConnection extends Connection
{
    /**
     * If there are any errors that occur on this connection, invoke the given
     * runnable. This is used in the driver to clean up resources associated with
     * the connection, like an open transaction.
     *
     * @param runnable To be run on error.
     */
    void onError( Runnable runnable );

    boolean hasUnrecoverableErrors();

    void dispose() throws InvalidOperationException;
}
