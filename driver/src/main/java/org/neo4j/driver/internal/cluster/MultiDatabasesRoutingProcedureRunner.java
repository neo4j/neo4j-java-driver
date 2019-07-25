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
package org.neo4j.driver.internal.cluster;

import java.util.Objects;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Statement;
import org.neo4j.driver.internal.BookmarkHolder;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.ReadOnlyBookmarkHolder;
import org.neo4j.driver.internal.async.connection.DirectConnection;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.ServerVersion;

import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.SYSTEM_DB_NAME;

public class MultiDatabasesRoutingProcedureRunner extends RoutingProcedureRunner
{
    static final String DATABASE_NAME = "database";
    static final String MULTI_DB_GET_ROUTING_TABLE = String.format( "CALL dbms.routing.getRoutingTable($%s, $%s)", ROUTING_CONTEXT, DATABASE_NAME );

    public MultiDatabasesRoutingProcedureRunner( RoutingContext context )
    {
        super( context );
    }

    @Override
    BookmarkHolder bookmarkHolder( InternalBookmark bookmark )
    {
        return new ReadOnlyBookmarkHolder( bookmark );
    }

    @Override
    Statement procedureStatement( ServerVersion serverVersion, String databaseName )
    {
        if ( Objects.equals( ABSENT_DB_NAME, databaseName ) )
        {
            databaseName = null;
        }
        return new Statement( MULTI_DB_GET_ROUTING_TABLE, parameters( ROUTING_CONTEXT, context.asMap(), DATABASE_NAME, databaseName ) );
    }

    @Override
    DirectConnection connection( Connection connection )
    {
        return new DirectConnection( connection, SYSTEM_DB_NAME, AccessMode.READ );
    }
}
