/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.internal.handlers;

import io.netty.util.concurrent.Promise;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;

public class RunResponseHandler implements ResponseHandler
{
    private final Promise<Void> runCompletedPromise;

    private List<String> statementKeys;
    private long resultAvailableAfter;

    public RunResponseHandler( Promise<Void> runCompletedPromise )
    {
        this.runCompletedPromise = runCompletedPromise;
    }

    @Override
    public void onSuccess( Map<String,Value> metadata )
    {
        statementKeys = extractKeys( metadata );
        resultAvailableAfter = extractResultAvailableAfter( metadata );

        if ( runCompletedPromise != null )
        {
            runCompletedPromise.setSuccess( null );
        }
    }

    @Override
    public void onFailure( Throwable error )
    {
        if ( runCompletedPromise != null )
        {
            runCompletedPromise.setFailure( error );
        }
    }

    @Override
    public void onRecord( Value[] fields )
    {
        throw new UnsupportedOperationException();
    }

    public List<String> statementKeys()
    {
        return statementKeys;
    }

    public long resultAvailableAfter()
    {
        return resultAvailableAfter;
    }

    private static List<String> extractKeys( Map<String,Value> metadata )
    {
        Value keysValue = metadata.get( "fields" );
        if ( keysValue != null )
        {
            if ( !keysValue.isEmpty() )
            {
                List<String> keys = new ArrayList<>( keysValue.size() );
                for ( Value value : keysValue.values() )
                {
                    keys.add( value.asString() );
                }

                return keys;
            }
        }
        return Collections.emptyList();
    }

    private static long extractResultAvailableAfter( Map<String,Value> metadata )
    {
        Value resultAvailableAfterValue = metadata.get( "result_available_after" );
        if ( resultAvailableAfterValue != null )
        {
            return resultAvailableAfterValue.asLong();
        }
        return -1;
    }
}
