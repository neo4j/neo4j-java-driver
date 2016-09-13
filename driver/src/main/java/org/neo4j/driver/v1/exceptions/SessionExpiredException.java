/**
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
package org.neo4j.driver.v1.exceptions;

/**
 * A <em>SessionExpiredException</em> indicates that the session can no longer satisfy the criteria under which it
 * was acquired, e.g. a server no longer accepts write requests. A new session needs to be acquired from the driver
 * and all actions taken on the expired session must be replayed.
 * @since 1.1
 */
public class SessionExpiredException extends Neo4jException
{
    public SessionExpiredException( String message, Throwable throwable )
    {
        super( message, throwable );
    }
}
