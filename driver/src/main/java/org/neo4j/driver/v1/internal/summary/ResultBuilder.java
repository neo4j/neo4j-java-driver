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
package org.neo4j.driver.v1.internal.summary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Notification;
import org.neo4j.driver.v1.Plan;
import org.neo4j.driver.v1.ProfiledPlan;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Result;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementType;
import org.neo4j.driver.v1.UpdateStatistics;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.internal.SimpleRecord;
import org.neo4j.driver.v1.internal.SimpleResult;
import org.neo4j.driver.v1.internal.spi.StreamCollector;

import static java.util.Collections.unmodifiableMap;

import static org.neo4j.driver.v1.internal.ParameterSupport.NO_PARAMETERS;

public class ResultBuilder implements StreamCollector
{
    private final SummaryBuilder summaryBuilder;

    private List<Record> body = new ArrayList<>();
    private List<String> keys = null;
    private Map<String, Integer> keyIndexLookup = null;

    public ResultBuilder( String statement, Map<String, Value> parameters )
    {
        Map<String, Value> unmodifiableParameters =
            parameters.isEmpty() ? NO_PARAMETERS : unmodifiableMap( parameters );
        this.summaryBuilder = new SummaryBuilder( new Statement( statement, unmodifiableParameters ) );
    }

    @Override
    public void fieldKeys( String[] names )
    {
        if ( keys == null )
        {
            int numFields = names.length;
            if ( numFields == 0 )
            {
                this.keys = Collections.emptyList();
                this.keyIndexLookup = Collections.emptyMap();
            }
            else
            {
                Map<String, Integer> fieldLookup = new HashMap<>( numFields );
                List<String> fields = new ArrayList<>( numFields );
                for ( int i = 0; i < numFields; i++ )
                {
                    String name = names[i];
                    fields.add( name );
                    fieldLookup.put( name, i );
                }
                this.keys = fields;
                this.keyIndexLookup = fieldLookup;
            }
        }
        else
        {
            throw new ClientException( "Received field names twice" );
        }
    }

    @Override
    public void record( Value[] values )
    {
        body.add( new SimpleRecord( keys, keyIndexLookup, values ) );
    }

    @Override
    public void statementType( StatementType type )
    {
        summaryBuilder.statementType( type );
    }

    @Override
    public void statementStatistics( UpdateStatistics statistics )
    {
        summaryBuilder.statementStatistics( statistics );
    }

    @Override
    public void plan( Plan plan )
    {
        summaryBuilder.plan( plan );
    }

    @Override
    public void profile( ProfiledPlan plan )
    {
        summaryBuilder.profile( plan );
    }

    @Override
    public void notifications( List<Notification> notifications )
    {
        summaryBuilder.notifications( notifications );
    }

    public Result build()
    {
        return new SimpleResult( keys, body, summaryBuilder.build() );
    }
}
