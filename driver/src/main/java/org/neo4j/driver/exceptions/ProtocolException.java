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
package org.neo4j.driver.exceptions;

/**
 * A signal that the contract for client-server communication has broken down.
 * The user should contact support and cannot resolve this his or herself.
 */
public class ProtocolException extends Neo4jException
{
    private static final String CODE = "Protocol violation: ";

    public ProtocolException( String message )
    {
        super( CODE + message );
    }

    public ProtocolException( String message, Throwable e )
    {
        super( CODE + message, e );
    }
}
