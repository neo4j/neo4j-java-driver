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
import java.util.concurrent.atomic.AtomicLong;

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
import org.neo4j.driver.v1.exceptions.AuthenticationException;
import org.neo4j.driver.v1.exceptions.ClientException;
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
    private static final int EXECUTION_TIME_SECONDS = Integer.getInteger( "executionTimeSeconds", 30 );

    @Rule
    public final LocalOrRemoteClusterRule clusterRule = new LocalOrRemoteClusterRule();

    private ExecutorService executor;
    private Driver driver;

    @Before
    public void setUp() throws Exception
    {
        URI clusterUri = clusterRule.getClusterUri();
        AuthToken authToken = clusterRule.getAuthToken();
        Config config = Config.build().withLogging( DEV_NULL_LOGGING ).withMaxIdleSessions( THREAD_COUNT ).toConfig();
        driver = GraphDatabase.driver( clusterUri, authToken, config );

        ThreadFactory threadFactory = new DaemonThreadFactory( getClass().getSimpleName() + "-worker-" );
        executor = Executors.newCachedThreadPool( threadFactory );
    }

    @After
    public void tearDown() throws Exception
    {
        executor.shutdownNow();
        if ( driver != null )
        {
            driver.close();
        }
    }

    @Test
    public void basicStressTest() throws Throwable
    {
        Context context = new Context();
        List<Future<?>> resultFutures = launchWorkerThreads( context );

        long openFileDescriptors = sleepAndGetOpenFileDescriptorCount();
        context.stop();

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
        assertExpectedNumberOfNodesCreated( context.getCreatedNodesCount() );
    }

    private List<Future<?>> launchWorkerThreads( Context context )
    {
        List<Command> commands = createCommands();
        List<Future<?>> futures = new ArrayList<>();

        for ( int i = 0; i < THREAD_COUNT; i++ )
        {
            Future<Void> future = launchWorkerThread( executor, commands, context );
            futures.add( future );
        }

        return futures;
    }

    private List<Command> createCommands()
    {
        List<Command> commands = new ArrayList<>();

        commands.add( new ReadQuery( driver, false ) );
        commands.add( new ReadQuery( driver, true ) );
        commands.add( new ReadQueryInTx( driver, false ) );
        commands.add( new ReadQueryInTx( driver, true ) );
        commands.add( new WriteQuery( driver, false ) );
        commands.add( new WriteQuery( driver, true ) );
        commands.add( new WriteQueryInTx( driver, false ) );
        commands.add( new WriteQueryInTx( driver, true ) );
        commands.add( new WriteQueryUsingReadSession( driver, false ) );
        commands.add( new WriteQueryUsingReadSession( driver, true ) );
        commands.add( new WriteQueryUsingReadSessionInTx( driver, false ) );
        commands.add( new WriteQueryUsingReadSessionInTx( driver, true ) );
        commands.add( new FailedAuth( clusterRule.getClusterUri() ) );

        return commands;
    }

    private static Future<Void> launchWorkerThread( final ExecutorService executor, final List<Command> commands,
            final Context context )
    {
        return executor.submit( new Callable<Void>()
        {
            final ThreadLocalRandom random = ThreadLocalRandom.current();

            @Override
            public Void call() throws Exception
            {
                while ( !context.isStopped() )
                {
                    int randomCommandIdx = random.nextInt( commands.size() );
                    Command command = commands.get( randomCommandIdx );
                    command.execute( context );
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

    private void assertExpectedNumberOfNodesCreated( long expectedCount )
    {
        try ( Session session = driver.session() )
        {
            List<Record> records = session.run( "MATCH (n) RETURN count(n) AS nodesCount" ).list();
            assertEquals( 1, records.size() );
            Record record = records.get( 0 );
            long actualCount = record.get( "nodesCount" ).asLong();
            assertEquals( "Unexpected number of nodes in the database", expectedCount, actualCount );
        }
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

    private static class Context
    {
        volatile boolean stopped;
        volatile String bookmark;
        final AtomicLong createdNodesCount = new AtomicLong();

        boolean isStopped()
        {
            return stopped;
        }

        void stop()
        {
            this.stopped = true;
        }

        String getBookmark()
        {
            return bookmark;
        }

        void setBookmark( String bookmark )
        {
            this.bookmark = bookmark;
        }

        void nodeCreated()
        {
            createdNodesCount.incrementAndGet();
        }

        long getCreatedNodesCount()
        {
            return createdNodesCount.get();
        }
    }

    private interface Command
    {
        void execute( Context context );
    }

    private static abstract class BaseQuery implements Command
    {
        final Driver driver;
        final boolean useBookmark;

        BaseQuery( Driver driver, boolean useBookmark )
        {
            this.driver = driver;
            this.useBookmark = useBookmark;
        }

        Session newSession( AccessMode mode, Context context )
        {
            if ( useBookmark )
            {
                return driver.session( mode, context.getBookmark() );
            }
            return driver.session( mode );
        }
    }

    private static class ReadQuery extends BaseQuery
    {
        ReadQuery( Driver driver, boolean useBookmark )
        {
            super( driver, useBookmark );
        }

        @Override
        public void execute( Context context )
        {
            try ( Session session = newSession( AccessMode.READ, context ) )
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
        ReadQueryInTx( Driver driver, boolean useBookmark )
        {
            super( driver, useBookmark );
        }

        @Override
        public void execute( Context context )
        {
            try ( Session session = newSession( AccessMode.READ, context );
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
                tx.success();
            }
        }
    }

    private static class WriteQuery extends BaseQuery
    {
        WriteQuery( Driver driver, boolean useBookmark )
        {
            super( driver, useBookmark );
        }

        @Override
        public void execute( Context context )
        {
            StatementResult result;
            try ( Session session = newSession( AccessMode.WRITE, context ) )
            {
                result = session.run( "CREATE ()" );
            }
            assertEquals( 1, result.summary().counters().nodesCreated() );
            context.nodeCreated();
        }
    }

    private static class WriteQueryInTx extends BaseQuery
    {
        WriteQueryInTx( Driver driver, boolean useBookmark )
        {
            super( driver, useBookmark );
        }

        @Override
        public void execute( Context context )
        {
            StatementResult result;
            try ( Session session = newSession( AccessMode.WRITE, context ) )
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    result = tx.run( "CREATE ()" );
                    tx.success();
                }

                context.setBookmark( session.lastBookmark() );
            }
            assertEquals( 1, result.summary().counters().nodesCreated() );
            context.nodeCreated();
        }
    }

    private static class WriteQueryUsingReadSession extends BaseQuery
    {
        WriteQueryUsingReadSession( Driver driver, boolean useBookmark )
        {
            super( driver, useBookmark );
        }

        @Override
        public void execute( Context context )
        {
            StatementResult result = null;
            try
            {
                try ( Session session = newSession( AccessMode.READ, context ) )
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
        WriteQueryUsingReadSessionInTx( Driver driver, boolean useBookmark )
        {
            super( driver, useBookmark );
        }

        @Override
        public void execute( Context context )
        {
            StatementResult result = null;
            try
            {
                try ( Session session = newSession( AccessMode.READ, context );
                      Transaction tx = session.beginTransaction() )
                {
                    result = tx.run( "CREATE ()" );
                    tx.success();
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
        public void execute( Context context )
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
                assertThat( e, instanceOf( AuthenticationException.class ) );
                assertThat( e.getMessage(), containsString( "authentication failure" ) );

                ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass( Throwable.class );
                verify( logger ).debug( startsWith( "~~ [CLOSED SECURE CHANNEL]" ), captor.capture() );
                verify( logger ).debug( startsWith( "~~ [DISCONNECT]" ), captor.capture() );
            }
        }
    }
}
