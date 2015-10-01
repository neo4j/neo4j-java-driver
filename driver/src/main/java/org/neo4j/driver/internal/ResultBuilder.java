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
package org.neo4j.driver.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.ResultSummary;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.spi.StreamCollector;

public class ResultBuilder implements StreamCollector
{
    private List<Record> body = new ArrayList<>();
    private Map<String,Integer> fieldLookup = Collections.emptyMap();
    private ResultSummary summary;

    @Override
    public void head( List<String> fields )
    {
        if ( fields.size() > 0 )
        {
            Map<String,Integer> fieldLookup = new HashMap<>();
            for ( int i = 0; i < fields.size(); i++ )
            {
                fieldLookup.put( fields.get( i ), i );
            }
            this.fieldLookup = fieldLookup;
        }
    }

    @Override
    public void record( Value[] fields )
    {
        body.add( new SimpleRecord( fieldLookup, fields ) );
    }

    @Override
    public void tail( ResultSummary summary )
    {
        this.summary = summary;
    }

    public Result build()
    {
        return new SimpleResult( fieldLookup.keySet(), body, summary );
    }
}
