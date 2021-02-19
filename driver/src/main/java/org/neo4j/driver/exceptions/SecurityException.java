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
 * Failed to communicate with the server due to security errors.
 * When this type of error happens, the security cause of the error should be fixed to ensure the safety of your data.
 * Restart of server/driver/cluster might be required to recover from this error.
 * @since 1.1
 */
public class SecurityException extends ClientException
{
    public SecurityException( String code, String message )
    {
        super( code, message );
    }

    public SecurityException( String message, Throwable t )
    {
        super( message, t );
    }
}
