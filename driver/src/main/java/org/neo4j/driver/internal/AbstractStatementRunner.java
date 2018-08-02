/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.internal;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.util.Extract;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.StatementRunner;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.TypeSystem;

abstract class AbstractStatementRunner implements StatementRunner
{
    @Override
    public final StatementResult run( String statementTemplate, Value parameters )
    {
        return run( new Statement( statementTemplate, parameters ) );
    }

    @Override
    public final CompletionStage<StatementResultCursor> runAsync( String statementTemplate, Value parameters )
    {
        return runAsync( new Statement( statementTemplate, parameters ) );
    }

    @Override
    public final StatementResult run( String statementTemplate, Map<String,Object> statementParameters )
    {
        return run( statementTemplate, parameters( statementParameters ) );
    }

    @Override
    public final CompletionStage<StatementResultCursor> runAsync( String statementTemplate, Map<String,Object> statementParameters )
    {
        return runAsync( statementTemplate, parameters( statementParameters ) );
    }

    @Override
    public final StatementResult run( String statementTemplate, Record statementParameters )
    {
        return run( statementTemplate, parameters( statementParameters ) );
    }

    @Override
    public final CompletionStage<StatementResultCursor> runAsync( String statementTemplate, Record statementParameters )
    {
        return runAsync( statementTemplate, parameters( statementParameters ) );
    }

    @Override
    public final StatementResult run( String statementText )
    {
        return run( statementText, Values.EmptyMap );
    }

    @Override
    public final CompletionStage<StatementResultCursor> runAsync( String statementText )
    {
        return runAsync( statementText, Values.EmptyMap );
    }

    @Override
    public final TypeSystem typeSystem()
    {
        return InternalTypeSystem.TYPE_SYSTEM;
    }

    private static Value parameters( Record record )
    {
        return record == null ? Values.EmptyMap : parameters( record.asMap() );
    }

    private static Value parameters( Map<String,Object> map )
    {
        if ( map == null || map.isEmpty() )
        {
            return Values.EmptyMap;
        }
        return new MapValue( Extract.mapOfValues( map ) );
    }
}
