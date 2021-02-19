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
package org.neo4j.driver.internal.cluster;

import java.util.List;

import org.neo4j.driver.Query;
import org.neo4j.driver.Record;

public class RoutingProcedureResponse
{
    private final Query procedure;
    private final List<Record> records;
    private final Throwable error;

    public RoutingProcedureResponse(Query procedure, List<Record> records )
    {
        this( procedure, records, null );
    }

    public RoutingProcedureResponse(Query procedure, Throwable error )
    {
        this( procedure, null, error );
    }

    private RoutingProcedureResponse(Query procedure, List<Record> records, Throwable error )
    {
        this.procedure = procedure;
        this.records = records;
        this.error = error;
    }

    public boolean isSuccess()
    {
        return records != null;
    }

    public Query procedure()
    {
        return procedure;
    }

    public List<Record> records()
    {
        if ( !isSuccess() )
        {
            throw new IllegalStateException( "Can't access records of a failed result", error );
        }
        return records;
    }

    public Throwable error()
    {
        if ( isSuccess() )
        {
            throw new IllegalStateException( "Can't access error of a succeeded result " + records );
        }
        return error;
    }
}
