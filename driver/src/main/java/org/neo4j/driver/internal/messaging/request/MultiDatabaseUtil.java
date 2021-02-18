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
package org.neo4j.driver.internal.messaging.request;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.ServerVersion;

public final class MultiDatabaseUtil
{
    public static void assertEmptyDatabaseName( DatabaseName databaseName, BoltProtocolVersion boltVersion )
    {
        if ( databaseName.databaseName().isPresent() )
        {
            throw new ClientException( String.format( "Database name parameter for selecting database is not supported in Bolt Protocol Version %s. " +
                    "Database name: '%s'", boltVersion, databaseName.description() ) );
        }
    }

    public static boolean supportsMultiDatabase( Connection connection )
    {
        return connection.serverVersion().greaterThanOrEqual( ServerVersion.v4_0_0 ) &&
               connection.protocol().version().compareTo( BoltProtocolV4.VERSION ) >= 0;
    }
}
