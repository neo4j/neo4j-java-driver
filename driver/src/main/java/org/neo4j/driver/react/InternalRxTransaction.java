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
package org.neo4j.driver.react;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.react.result.RxStatementResultCursor;
import org.neo4j.driver.v1.Statement;

public class InternalRxTransaction extends AbstractRxStatementRunner implements RxTransaction
{
    private final ExplicitTransaction asyncTx;

    public InternalRxTransaction( ExplicitTransaction asyncTx )
    {
        this.asyncTx = asyncTx;
    }

    @Override
    public Publisher<Void> commit()
    {
        return close( true );
    }

    private Publisher<Void> close( boolean commit )
    {
        return Mono.create( sink -> {
            CompletionStage<Void> close;
            if ( commit )
            {
                close = asyncTx.commitAsync();
            }
            else
            {
                close = asyncTx.rollbackAsync();
            }
            close.whenComplete( ( ignored, error ) -> {
                if ( error != null )
                {
                    sink.error( error );
                }
                else
                {
                    sink.success();
                }
            } );
        } );
    }

    @Override
    public Publisher<Void> rollback()
    {
        return close( false );
    }

    @Override
    public RxResult run( Statement statement )
    {
        CompletionStage<RxStatementResultCursor> cursor = asyncTx.runRx( statement );
        return new InternalRxResult( cursor );
    }
}
