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
package org.neo4j.driver.v1.stress;

import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.util.TestNeo4j;

import static java.util.Arrays.asList;
import static org.neo4j.driver.v1.GraphDatabase.driver;

public class SessionPoolingStressIT
{
    @Rule
    public TestNeo4j neo4j = new TestNeo4j();

    private static final int N_THREADS = 50;
    private final ExecutorService executor = Executors.newFixedThreadPool( N_THREADS );
    private static final List<String> QUERIES = asList( "RETURN 1295 + 42", "UNWIND range(1,10000) AS x CREATE (n {prop:x}) DELETE n " );
    private static final int MAX_TIME = 10000;
    private final AtomicBoolean hasFailed = new AtomicBoolean( false );

    @Test
    public void shouldWorkFine() throws InterruptedException
    {
        Driver driver = driver( neo4j.uri(),
                Config.build()
                        .withEncryptionLevel( Config.EncryptionLevel.NONE )
                        .withMaxSessions( N_THREADS ).toConfig() );

        doWork( driver );
        executor.awaitTermination( MAX_TIME + (int)(MAX_TIME * 0.2), TimeUnit.MILLISECONDS );
        driver.close();
    }

    private void doWork( final Driver driver )
    {
        for ( int i = 0; i < N_THREADS; i++ )
        {
            executor.execute( new Worker( driver ) );
        }
    }

    private class Worker implements Runnable
    {
        private final Random random = ThreadLocalRandom.current();
        private final Driver driver;

        public Worker( Driver driver )
        {
            this.driver = driver;
        }

        @Override
        public void run()
        {
            try
            {
                long deadline = System.currentTimeMillis() + MAX_TIME;
                for (;;)
                {
                    for ( String query : QUERIES )
                    {
                        runQuery( query );
                    }
                    long left = deadline - System.currentTimeMillis();
                    if ( left <= 0 )
                    {
                        break;
                    }
                }
            }
            catch ( Throwable e )
            {
                e.printStackTrace();
                hasFailed.set( true );
            }
        }

        private void runQuery( String query ) throws InterruptedException
        {
            try ( Session session = driver.session() )
            {
                StatementResult run = session.run( query );
                Thread.sleep( random.nextInt( 100 ) );
                run.consume();
                Thread.sleep( random.nextInt( 100 ) );
            }
        }
    }
}
