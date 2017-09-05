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
package org.neo4j.driver.v1.util;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.Map;

import org.neo4j.driver.internal.netty.ListenableFuture;
import org.neo4j.driver.internal.netty.StatementResultCursor;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.TypeSystem;

/**
 * A little utility for integration testing, this provides tests with a session they can work with.
 * If you want more direct control, have a look at {@link TestNeo4j} instead.
 */
public class TestNeo4jSession extends TestNeo4j implements Session
{
    private Session realSession;

    public TestNeo4jSession()
    {
        super();
    }

    public TestNeo4jSession( Neo4jResetMode resetMode )
    {
        super();
    }

    public TestNeo4jSession( Neo4jSettings initialSettings )
    {
        super();
    }

    public TestNeo4jSession( Neo4jSettings initialSettings, Neo4jResetMode resetMode )
    {
        super();
    }

    @Override
    public Statement apply( final Statement base, Description description )
    {
        return super.apply( new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                try
                {
                    realSession = driver().session();
                    base.evaluate();
                }
                finally
                {
                    if ( realSession != null )
                    {
                        realSession.close();
                    }
                }
            }
        }, description );
    }

    @Override
    public boolean isOpen()
    {
        return realSession.isOpen();
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException( "Disallowed on this test session" );
    }

    @Override
    public ListenableFuture<Void> closeAsync()
    {
        return realSession.closeAsync();
    }

    @Override
    public StatementResultCursor runAsync( String statement, Map<String,Object> params )
    {
        return realSession.runAsync( statement, params );
    }

    @Override
    public Transaction beginTransaction()
    {
        return realSession.beginTransaction();
    }

    @Deprecated
    @Override
    public Transaction beginTransaction( String bookmark )
    {
        return realSession.beginTransaction( bookmark );
    }

    @Override
    public ListenableFuture<Transaction> beginTransactionAsync()
    {
        return realSession.beginTransactionAsync();
    }

    @Override
    public <T> T readTransaction( TransactionWork<T> work )
    {
        return realSession.readTransaction( work );
    }

    @Override
    public <T> T writeTransaction( TransactionWork<T> work )
    {
        return realSession.writeTransaction( work );
    }

    @Override
    public String lastBookmark()
    {
        return realSession.lastBookmark();
    }

    @Deprecated
    @Override
    public void reset()
    {
        realSession.reset();
    }

    @Override
    public StatementResult run( String statementText, Map<String,Object> statementParameters )
    {
        return realSession.run( statementText, statementParameters );
    }

    @Override
    public StatementResult run( String statementText, Value parameters )
    {
        return realSession.run( statementText, parameters );
    }

    @Override
    public StatementResultCursor runAsync( String statementText, Value parameters )
    {
        return realSession.runAsync( statementText, parameters );
    }

    @Override
    public StatementResult run( String statementText, Record parameters )
    {
        return realSession.run( statementText, parameters );
    }

    @Override
    public StatementResultCursor runAsync( String statementTemplate, Record statementParameters )
    {
        return realSession.runAsync( statementTemplate, statementParameters );
    }

    @Override
    public StatementResult run( String statementTemplate )
    {
        return realSession.run( statementTemplate );
    }

    @Override
    public StatementResultCursor runAsync( String statementTemplate )
    {
        return realSession.runAsync( statementTemplate );
    }

    @Override
    public StatementResult run( org.neo4j.driver.v1.Statement statement )
    {
        return realSession.run( statement.text(), statement.parameters() );
    }

    @Override
    public StatementResultCursor runAsync( org.neo4j.driver.v1.Statement statement )
    {
        return realSession.runAsync( statement );
    }

    @Override
    public TypeSystem typeSystem()
    {
        return realSession.typeSystem();
    }
}
