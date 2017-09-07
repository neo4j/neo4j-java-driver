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

import java.util.Arrays;
import java.util.Map;

import org.neo4j.driver.internal.Bookmark;
import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;

import static java.util.Objects.requireNonNull;

public class CommitTxResponseHandler implements ResponseHandler
{
    private final Promise<Void> commitTxPromise;
    private final ExplicitTransaction tx;

    public CommitTxResponseHandler( Promise<Void> commitTxPromise, ExplicitTransaction tx )
    {
        this.commitTxPromise = requireNonNull( commitTxPromise );
        this.tx = requireNonNull( tx );
    }

    @Override
    public void onSuccess( Map<String,Value> metadata )
    {
        Value bookmarkValue = metadata.get( "bookmark" );
        if ( bookmarkValue != null )
        {
            if ( tx != null )
            {
                tx.setBookmark( Bookmark.from( bookmarkValue.asString() ) );
            }
        }

        commitTxPromise.setSuccess( null );
    }

    @Override
    public void onFailure( Throwable error )
    {
        commitTxPromise.setFailure( error );
    }

    @Override
    public void onRecord( Value[] fields )
    {
        throw new UnsupportedOperationException(
                "Transaction commit is not expected to receive records: " + Arrays.toString( fields ) );
    }
}
