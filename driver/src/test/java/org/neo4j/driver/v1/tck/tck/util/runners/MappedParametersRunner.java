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
package org.neo4j.driver.v1.tck.tck.util.runners;

import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;

import static org.neo4j.driver.v1.tck.Environment.driver;

public class MappedParametersRunner implements CypherStatementRunner
{
    private String query;
    private Session session;
    private Value parameters;
    private StatementResult result;

    public static MappedParametersRunner createParameterRunner( String st, String key, Value value )
    {
        return new MappedParametersRunner(st, Collections.singletonMap( key, value ));
    }

    public MappedParametersRunner( String st, Map<String,Value> params )
    {
        session = driver.session();
        query = st;
        parameters = Values.value(params);
    }

    @Override
    public CypherStatementRunner runCypherStatement()
    {

        result = session.run( new Statement( query, parameters ) );
        return this;
    }

    @Override
    public StatementResult result()
    {
        return result;
    }

    @Override
    public Value parameters()
    {
        return parameters;
    }

    @Override
    public void close()
    {
        session.close();
    }
}
