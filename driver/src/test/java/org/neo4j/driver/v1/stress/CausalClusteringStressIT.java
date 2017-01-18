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
package org.neo4j.driver.v1.stress;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.util.DaemonThreadFactory;
import org.neo4j.driver.v1.util.cc.LocalOrRemoteClusterRule;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.v1.AuthTokens.basic;

public class CausalClusteringStressIT
{
    private static final int THREAD_COUNT = Integer.getInteger( "threadCount", 8 );
    private static final int EXECUTION_TIME_SECONDS = Integer.getInteger( "executionTimeSeconds", 20 );

    @Rule
    public final LocalOrRemoteClusterRule clusterRule = new LocalOrRemoteClusterRule();

    private ExecutorService executor;
    private Driver driver;

    @Before
    public void setUp() throws Exception
    {
        URI clusterUri = clusterRule.getClusterUri();
        AuthToken authToken = clusterRule.getAuthToken();
        Config config = Config.build().withLogging( DEV_NULL_LOGGING ).toConfig();
        driver = GraphDatabase.driver( clusterUri, authToken, config );

        ThreadFactory threadFactory = new DaemonThreadFactory( getClass().getSimpleName() + "-worker-" );
        executor = Executors.newCachedThreadPool( threadFactory );
    }

    @After
    public void tearDown() throws Exception
    {
        executor.shutdownNow();
        driver.close();
    }

    @Test
    public void basicStressTest() throws Throwable
    {
        AtomicBoolean stop = new AtomicBoolean();
        List<Future<?>> resultFutures = launchWorkerThreads( stop );

        long openFileDescriptors = sleepAndGetOpenFileDescriptorCount();
        stop.set( true );

        Throwable firstError = null;
        for ( Future<?> future : resultFutures )
        {
            try
            {
                assertNull( future.get( 10, TimeUnit.SECONDS ) );
            }
            catch ( Throwable error )
            {
                firstError = withSuppressed( firstError, error );
            }
        }

        if ( firstError != null )
        {
            throw firstError;
        }

        assertNoFileDescriptorLeak( openFileDescriptors );
    }

    private List<Future<?>> launchWorkerThreads( AtomicBoolean stop )
    {
        List<Command> commands = createCommands();
        List<Future<?>> futures = new ArrayList<>();

        for ( int i = 0; i < THREAD_COUNT; i++ )
        {
            Future<Void> future = launchWorkerThread( executor, commands, stop );
            futures.add( future );
        }

        return futures;
    }

    private List<Command> createCommands()
    {
        List<Command> commands = new ArrayList<>();

        commands.add( new ReadQuery( driver ) );
        commands.add( new ReadQueryInTx( driver ) );
        commands.add( new WriteQuery( driver ) );
        commands.add( new WriteQueryInTx( driver ) );
        commands.add( new WriteQueryUsingReadSession( driver ) );
        commands.add( new WriteQueryUsingReadSessionInTx( driver ) );
        commands.add( new FailedAuth( clusterRule.getClusterUri() ) );

        return commands;
    }

    private static Future<Void> launchWorkerThread( final ExecutorService executor, final List<Command> commands,
            final AtomicBoolean stop )
    {
        return executor.submit( new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                while ( !stop.get() )
                {
                    int randomCommandIdx = ThreadLocalRandom.current().nextInt( commands.size() );
                    Command command = commands.get( randomCommandIdx );
                    command.execute();
                }
                return null;
            }
        } );
    }

    private static long sleepAndGetOpenFileDescriptorCount() throws InterruptedException
    {
        int halfSleepSeconds = Math.max( 1, EXECUTION_TIME_SECONDS / 2 );
        TimeUnit.SECONDS.sleep( halfSleepSeconds );
        long openFileDescriptorCount = getOpenFileDescriptorCount();
        TimeUnit.SECONDS.sleep( halfSleepSeconds );
        return openFileDescriptorCount;
    }

    private void assertNoFileDescriptorLeak( long previousOpenFileDescriptors )
    {
        // number of open file descriptors should not go up for more than 20%
        long maxOpenFileDescriptors = (long) (previousOpenFileDescriptors * 1.2);
        long currentOpenFileDescriptorCount = getOpenFileDescriptorCount();
        assertThat( "Unexpectedly high number of open file descriptors",
                currentOpenFileDescriptorCount, lessThanOrEqualTo( maxOpenFileDescriptors ) );
    }

    private static long getOpenFileDescriptorCount()
    {
        try
        {
            OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
            Method method = osBean.getClass().getDeclaredMethod( "getOpenFileDescriptorCount" );
            method.setAccessible( true );
            return (long) method.invoke( osBean );
        }
        catch ( Throwable t )
        {
            return 0;
        }
    }

    private static Throwable withSuppressed( Throwable firstError, Throwable newError )
    {
        if ( firstError == null )
        {
            return newError;
        }
        firstError.addSuppressed( newError );
        return firstError;
    }

    private interface Command
    {
        void execute();
    }

    private static abstract class BaseQuery implements Command
    {
        final Driver driver;

        BaseQuery( Driver driver )
        {
            this.driver = driver;
        }
    }

    private static class ReadQuery extends BaseQuery
    {
        ReadQuery( Driver driver )
        {
            super( driver );
        }

        @Override
        public void execute()
        {
            try ( Session session = driver.session( AccessMode.READ ) )
            {
                StatementResult result = session.run( "MATCH (n) RETURN n LIMIT 1" );
                List<Record> records = result.list();
                if ( !records.isEmpty() )
                {
                    Record record = single( records );
                    Node node = record.get( 0 ).asNode();
                    assertNotNull( node );
                }
            }
        }
    }

    private static class ReadQueryInTx extends BaseQuery
    {
        ReadQueryInTx( Driver driver )
        {
            super( driver );
        }

        @Override
        public void execute()
        {
            try ( Session session = driver.session( AccessMode.READ );
                  Transaction tx = session.beginTransaction() )
            {
                StatementResult result = tx.run( "MATCH (n) RETURN n LIMIT 1" );
                List<Record> records = result.list();
                if ( !records.isEmpty() )
                {
                    Record record = single( records );
                    Node node = record.get( 0 ).asNode();
                    assertNotNull( node );
                }
            }
        }
    }

    private static class WriteQuery extends BaseQuery
    {
        WriteQuery( Driver driver )
        {
            super( driver );
        }

        @Override
        public void execute()
        {
            StatementResult result;
            try ( Session session = driver.session( AccessMode.WRITE ) )
            {
                result = session.run( "CREATE ()" );
            }
            assertEquals( 1, result.summary().counters().nodesCreated() );
        }
    }

    private static class WriteQueryInTx extends BaseQuery
    {
        WriteQueryInTx( Driver driver )
        {
            super( driver );
        }

        @Override
        public void execute()
        {
            StatementResult result;
            try ( Session session = driver.session( AccessMode.WRITE );
                  Transaction tx = session.beginTransaction() )
            {
                result = tx.run( "CREATE ()" );
            }
            assertEquals( 1, result.summary().counters().nodesCreated() );
        }
    }

    private static class WriteQueryUsingReadSession extends BaseQuery
    {
        WriteQueryUsingReadSession( Driver driver )
        {
            super( driver );
        }

        @Override
        public void execute()
        {
            StatementResult result = null;
            try
            {
                try ( Session session = driver.session( AccessMode.READ ) )
                {
                    result = session.run( "CREATE ()" );
                }
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( ClientException.class ) );
                assertNotNull( result );
                assertEquals( 0, result.summary().counters().nodesCreated() );
            }
        }
    }

    private static class WriteQueryUsingReadSessionInTx extends BaseQuery
    {
        WriteQueryUsingReadSessionInTx( Driver driver )
        {
            super( driver );
        }

        @Override
        public void execute()
        {
            StatementResult result = null;
            try
            {
                try ( Session session = driver.session( AccessMode.READ );
                      Transaction tx = session.beginTransaction() )
                {
                    result = tx.run( "CREATE ()" );
                }
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( ClientException.class ) );
                assertNotNull( result );
                assertEquals( 0, result.summary().counters().nodesCreated() );
            }
        }
    }

    private static class FailedAuth implements Command
    {
        final URI clusterUri;

        FailedAuth( URI clusterUri )
        {
            this.clusterUri = clusterUri;
        }

        @Override
        public void execute()
        {
            Logger logger = mock( Logger.class );
            Logging logging = mock( Logging.class );
            when( logging.getLog( anyString() ) ).thenReturn( logger );
            Config config = Config.build().withLogging( logging ).toConfig();

            try
            {
                GraphDatabase.driver( clusterUri, basic( "wrongUsername", "wrongPassword" ), config );
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( ServiceUnavailableException.class ) );

                ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass( Throwable.class );
                verify( logger ).error( startsWith( "Failed to connect to routing server" ), captor.capture() );

                Throwable loggedThrowable = captor.getValue();
                assertThat( loggedThrowable, instanceOf( ClientException.class ) );
                assertThat( loggedThrowable.getMessage().toLowerCase(), containsString( "authentication failure" ) );
            }
        }
    }
}
