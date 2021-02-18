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
package org.neo4j.driver.internal.async;

import java.util.EnumSet;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.BookmarkHolder;
import org.neo4j.driver.internal.cursor.AsyncResultCursor;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Futures;

import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

public class UnmanagedTransaction
{
    private enum State
    {
        /** The transaction is running with no explicit success or failure marked */
        ACTIVE,

        /**
         * This transaction has been terminated either because of explicit {@link Session#reset()} or because of a
         * fatal connection error.
         */
        TERMINATED,

        /** This transaction has successfully committed */
        COMMITTED,

        /** This transaction has been rolled back */
        ROLLED_BACK
    }

    /**
     * This is a holder so that we can have ony the state volatile in the tx without having to synchronize the whole block.
     */
    private static final class StateHolder
    {
        private static final EnumSet<State> OPEN_STATES = EnumSet.of( State.ACTIVE, State.TERMINATED );
        private static final StateHolder ACTIVE_HOLDER = new StateHolder( State.ACTIVE, null );
        private static final StateHolder COMMITTED_HOLDER = new StateHolder( State.COMMITTED, null );
        private static final StateHolder ROLLED_BACK_HOLDER = new StateHolder( State.ROLLED_BACK, null );

        /**
         * The actual state.
         */
        final State value;

        /**
         * If this holder contains a state of {@link State#TERMINATED}, this represents the cause if any.
         */
        final Throwable causeOfTermination;

        static StateHolder of( State value )
        {
            switch ( value )
            {
            case ACTIVE:
                return ACTIVE_HOLDER;
            case COMMITTED:
                return COMMITTED_HOLDER;
            case ROLLED_BACK:
                return ROLLED_BACK_HOLDER;
            case TERMINATED:
            default:
                throw new IllegalArgumentException( "Cannot provide a default state holder for state " + value );
            }
        }

        static StateHolder terminatedWith( Throwable cause )
        {
            return new StateHolder( State.TERMINATED, cause );
        }

        private StateHolder( State value, Throwable causeOfTermination )
        {
            this.value = value;
            this.causeOfTermination = causeOfTermination;
        }

        boolean isOpen()
        {
            return OPEN_STATES.contains( this.value );
        }
    }

    private final Connection connection;
    private final BoltProtocol protocol;
    private final BookmarkHolder bookmarkHolder;
    private final ResultCursorsHolder resultCursors;
    private final long fetchSize;

    private volatile StateHolder state = StateHolder.of( State.ACTIVE );

    public UnmanagedTransaction(Connection connection, BookmarkHolder bookmarkHolder, long fetchSize )
    {
        this.connection = connection;
        this.protocol = connection.protocol();
        this.bookmarkHolder = bookmarkHolder;
        this.resultCursors = new ResultCursorsHolder();
        this.fetchSize = fetchSize;
    }

    public CompletionStage<UnmanagedTransaction> beginAsync(Bookmark initialBookmark, TransactionConfig config )
    {
        return protocol.beginTransaction( connection, initialBookmark, config )
                .handle( ( ignore, beginError ) ->
                {
                    if ( beginError != null )
                    {
                        // release connection if begin failed, transaction can't be started
                        connection.release();
                        throw Futures.asCompletionException( beginError );
                    }
                    return this;
                } );
    }

    public CompletionStage<Void> closeAsync()
    {
        if ( isOpen() )
        {
            return rollbackAsync();
        }
        else
        {
            return completedWithNull();
        }
    }

    public CompletionStage<Void> commitAsync()
    {
        if ( state.value == State.COMMITTED )
        {
            return failedFuture( new ClientException( "Can't commit, transaction has been committed" ) );
        }
        else if ( state.value == State.ROLLED_BACK )
        {
            return failedFuture( new ClientException( "Can't commit, transaction has been rolled back" ) );
        }
        else
        {
            return resultCursors.retrieveNotConsumedError()
                    .thenCompose( error -> doCommitAsync().handle( handleCommitOrRollback( error ) ) )
                    .whenComplete( ( ignore, error ) -> transactionClosed( error == null ) );
        }
    }

    public CompletionStage<Void> rollbackAsync()
    {
        if ( state.value == State.COMMITTED )
        {
            return failedFuture( new ClientException( "Can't rollback, transaction has been committed" ) );
        }
        else if ( state.value == State.ROLLED_BACK )
        {
            return failedFuture( new ClientException( "Can't rollback, transaction has been rolled back" ) );
        }
        else
        {
            return resultCursors.retrieveNotConsumedError()
                    .thenCompose( error -> doRollbackAsync().handle( handleCommitOrRollback( error ) ) )
                    .whenComplete( ( ignore, error ) -> transactionClosed( false ) );
        }
    }

    public CompletionStage<ResultCursor> runAsync(Query query, boolean waitForRunResponse )
    {
        ensureCanRunQueries();
        CompletionStage<AsyncResultCursor> cursorStage =
                protocol.runInUnmanagedTransaction( connection, query, this, waitForRunResponse, fetchSize ).asyncResult();
        resultCursors.add( cursorStage );
        return cursorStage.thenApply( cursor -> cursor );
    }

    public CompletionStage<RxResultCursor> runRx(Query query)
    {
        ensureCanRunQueries();
        CompletionStage<RxResultCursor> cursorStage =
                protocol.runInUnmanagedTransaction( connection, query, this, false, fetchSize ).rxResult();
        resultCursors.add( cursorStage );
        return cursorStage;
    }

    public boolean isOpen()
    {
        return state.isOpen();
    }

    public void markTerminated( Throwable cause )
    {
        state = StateHolder.terminatedWith( cause );
    }

    public Connection connection()
    {
        return connection;
    }

    private void ensureCanRunQueries()
    {
        if ( state.value == State.COMMITTED )
        {
            throw new ClientException( "Cannot run more queries in this transaction, it has been committed" );
        }
        else if ( state.value == State.ROLLED_BACK )
        {
            throw new ClientException( "Cannot run more queries in this transaction, it has been rolled back" );
        }
        else if ( state.value == State.TERMINATED )
        {
            throw new ClientException( "Cannot run more queries in this transaction, " +
                    "it has either experienced an fatal error or was explicitly terminated", state.causeOfTermination );
        }
    }

    private CompletionStage<Void> doCommitAsync()
    {
        if ( state.value == State.TERMINATED )
        {
            return failedFuture( new ClientException( "Transaction can't be committed. " +
                                                      "It has been rolled back either because of an error or explicit termination", state.causeOfTermination ) );
        }
        return protocol.commitTransaction( connection ).thenAccept( bookmarkHolder::setBookmark );
    }

    private CompletionStage<Void> doRollbackAsync()
    {
        if ( state.value == State.TERMINATED )
        {
            return completedWithNull();
        }
        return protocol.rollbackTransaction( connection );
    }

    private static BiFunction<Void,Throwable,Void> handleCommitOrRollback( Throwable cursorFailure )
    {
        return ( ignore, commitOrRollbackError ) ->
        {
            CompletionException combinedError = Futures.combineErrors( cursorFailure, commitOrRollbackError );
            if ( combinedError != null )
            {
                throw combinedError;
            }
            return null;
        };
    }

    private void transactionClosed( boolean isCommitted )
    {
        if ( isCommitted )
        {
            state = StateHolder.of( State.COMMITTED );
        }
        else
        {
            state = StateHolder.of( State.ROLLED_BACK );
        }
        connection.release(); // release in background
    }
}
