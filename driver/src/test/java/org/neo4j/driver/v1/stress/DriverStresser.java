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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.util.Neo4jRunner;
import org.neo4j.driver.v1.util.Neo4jSettings;

import static org.neo4j.driver.v1.Values.parameters;

public class DriverStresser
{

    private static Neo4jRunner server;
    private static Driver driver;

    public static void main( String... args ) throws Throwable
    {
        int iterations = 100_000;

        bench( iterations, 1, 10_000 );
        bench( (long) iterations / 2, 2, 10_000 );
        bench( (long) iterations / 4, 4, 10_000 );
        bench( (long) iterations / 8, 8, 10_000 );
        bench( (long) iterations / 16, 16, 10_000 );
        bench( (long) iterations / 32, 32, 10_000 );
    }

    public static void setup() throws Exception
    {
        server = Neo4jRunner.getOrCreateGlobalRunner();
        server.ensureRunning( Neo4jSettings.TEST_SETTINGS );
        driver = GraphDatabase.driver( "bolt://localhost" );
    }

    static class Worker
    {
        private final Session session;

        public Worker()
        {
            session = driver.session();
        }

        public int operation()
        {
            String statement = "RETURN 1 AS n";       // = "CREATE (a {name:{n}}) RETURN a.name";
            Value parameters = parameters();          // = Values.parameters( "n", "Bob" );

            int total = 0;
            StatementResult result = session.run( statement, parameters );
            while ( result.hasNext() )
            {
                total += result.next().get( "n" ).asInt();
            }
            return total;
        }
    }

    public static void tearDown() throws Exception
    {
        driver.close();
        server.stopNeo4j();
    }


    private static void bench( long iterations, int concurrency, long warmupIterations ) throws Exception
    {
        ExecutorService executorService = Executors.newFixedThreadPool( concurrency );

        setup();
        try
        {
            // Warmup
            awaitAll( executorService.invokeAll( workers( warmupIterations, concurrency ) ) );

            long start = System.nanoTime();
            List<Future<Object>> futures = executorService.invokeAll( workers( iterations, concurrency ) );
            awaitAll( futures );
            long delta = System.nanoTime() - start;

            System.out.printf( "With %d threads: %s ops/s%n",
                               concurrency, (iterations * concurrency) / (delta / 1_000_000_000.0) );
        }
        finally
        {
            tearDown();
        }

        executorService.shutdownNow();
        executorService.awaitTermination( 10, TimeUnit.SECONDS );
    }

    private static void awaitAll( List<Future<Object>> futures ) throws Exception
    {
        for ( Future<Object> future : futures )
        {
            future.get();
        }
    }

    private static List<Callable<Object>> workers( final long iterations, final int numWorkers )
    {
        List<Callable<Object>> workers = new ArrayList<>();
        for ( int i = 0; i < numWorkers; i++ )
        {
            final Worker worker = new Worker();
            workers.add( new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    int dontRemoveMyCode = 0;
                    for ( int i = 0; i < iterations; i++ )
                    {
                        dontRemoveMyCode += worker.operation();
                    }
                    return dontRemoveMyCode;
                }
            } );
        }
        return workers;
    }
}
