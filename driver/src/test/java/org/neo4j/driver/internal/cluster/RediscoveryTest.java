/*
 * Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.driver.internal.cluster;

import io.netty.util.concurrent.GlobalEventExecutor;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.cluster.ClusterCompositionResponse.Failure;
import org.neo4j.driver.internal.cluster.ClusterCompositionResponse.Success;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.ImmediateSchedulingEventExecutor;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.exceptions.AuthenticationException;
import org.neo4j.driver.v1.exceptions.ProtocolException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.startsWith;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.B;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.C;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.D;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.E;
import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.v1.util.TestUtil.asOrderedSet;
import static org.neo4j.driver.v1.util.TestUtil.await;

public class RediscoveryTest
{
    private final ConnectionPool pool = asyncConnectionPoolMock();

    @Test
    public void shouldUseFirstRouterInTable()
    {
        ClusterComposition expectedComposition = new ClusterComposition( 42,
                asOrderedSet( B, C ), asOrderedSet( C, D ), asOrderedSet( B ) );

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( B, new Success( expectedComposition ) ); // first -> valid cluster composition

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        Rediscovery rediscovery = newRediscovery( A, compositionProvider, mock( HostNameResolver.class ) );
        RoutingTable table = routingTableMock( B );

        ClusterComposition actualComposition = await( rediscovery.lookupClusterComposition( table, pool ) );

        assertEquals( expectedComposition, actualComposition );
        verify( table, never() ).forget( B );
    }

    @Test
    public void shouldSkipFailingRouters()
    {
        ClusterComposition expectedComposition = new ClusterComposition( 42,
                asOrderedSet( A, B, C ), asOrderedSet( B, C, D ), asOrderedSet( A, B ) );

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( A, new RuntimeException( "Hi!" ) ); // first -> non-fatal failure
        responsesByAddress.put( B, new ServiceUnavailableException( "Hi!" ) ); // second -> non-fatal failure
        responsesByAddress.put( C, new Success( expectedComposition ) ); // third -> valid cluster composition

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        Rediscovery rediscovery = newRediscovery( A, compositionProvider, mock( HostNameResolver.class ) );
        RoutingTable table = routingTableMock( A, B, C );

        ClusterComposition actualComposition = await( rediscovery.lookupClusterComposition( table, pool ) );

        assertEquals( expectedComposition, actualComposition );
        verify( table ).forget( A );
        verify( table ).forget( B );
        verify( table, never() ).forget( C );
    }

    @Test
    public void shouldFailImmediatelyOnAuthError()
    {
        AuthenticationException authError = new AuthenticationException( "Neo.ClientError.Security.Unauthorized",
                "Wrong password" );

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( A, new RuntimeException( "Hi!" ) ); // first router -> non-fatal failure
        responsesByAddress.put( B, authError ); // second router -> fatal auth error

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        Rediscovery rediscovery = newRediscovery( A, compositionProvider, mock( HostNameResolver.class ) );
        RoutingTable table = routingTableMock( A, B, C );

        try
        {
            await( rediscovery.lookupClusterComposition( table, pool ) );
            fail( "Exception expected" );
        }
        catch ( AuthenticationException e )
        {
            assertEquals( authError, e );
            verify( table ).forget( A );
        }
    }

    @Test
    public void shouldFallbackToInitialRouterWhenKnownRoutersFail()
    {
        BoltServerAddress initialRouter = A;
        ClusterComposition expectedComposition = new ClusterComposition( 42,
                asOrderedSet( C, B, A ), asOrderedSet( A, B ), asOrderedSet( D, E ) );

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( B, new ServiceUnavailableException( "Hi!" ) ); // first -> non-fatal failure
        responsesByAddress.put( C, new ServiceUnavailableException( "Hi!" ) ); // second -> non-fatal failure
        responsesByAddress.put( initialRouter, new Success( expectedComposition ) ); // initial -> valid response

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        HostNameResolver resolver = hostNameResolverMock( initialRouter, initialRouter );
        Rediscovery rediscovery = newRediscovery( initialRouter, compositionProvider, resolver );
        RoutingTable table = routingTableMock( B, C );

        ClusterComposition actualComposition = await( rediscovery.lookupClusterComposition( table, pool ) );

        assertEquals( expectedComposition, actualComposition );
        verify( table ).forget( B );
        verify( table ).forget( C );
    }

    @Test
    public void shouldFailImmediatelyWhenClusterCompositionProviderReturnsFailure()
    {
        ClusterComposition validComposition = new ClusterComposition( 42,
                asOrderedSet( A ), asOrderedSet( B ), asOrderedSet( C ) );
        ProtocolException protocolError = new ProtocolException( "Wrong record!" );

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( B, new Failure( protocolError ) ); // first -> fatal failure
        responsesByAddress.put( C, new Success( validComposition ) ); // second -> valid cluster composition

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        Rediscovery rediscovery = newRediscovery( A, compositionProvider, mock( HostNameResolver.class ) );
        RoutingTable table = routingTableMock( B, C );

        try
        {
            await( rediscovery.lookupClusterComposition( table, pool ) );
            fail( "Exception expected" );
        }
        catch ( ProtocolException e )
        {
            assertEquals( protocolError, e );
        }
    }

    @Test
    public void shouldResolveInitialRouterAddress()
    {
        BoltServerAddress initialRouter = A;
        ClusterComposition expectedComposition = new ClusterComposition( 42,
                asOrderedSet( A, B ), asOrderedSet( A, B ), asOrderedSet( A, B ) );

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( B, new ServiceUnavailableException( "Hi!" ) ); // first -> non-fatal failure
        responsesByAddress.put( C, new ServiceUnavailableException( "Hi!" ) ); // second -> non-fatal failure
        responsesByAddress.put( D, new IOException( "Hi!" ) ); // resolved first -> non-fatal failure
        responsesByAddress.put( E, new Success( expectedComposition ) ); // resolved second -> valid response

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        // initial router resolved to two other addresses
        HostNameResolver resolver = hostNameResolverMock( initialRouter, D, E );
        Rediscovery rediscovery = newRediscovery( initialRouter, compositionProvider, resolver );
        RoutingTable table = routingTableMock( B, C );

        ClusterComposition actualComposition = await( rediscovery.lookupClusterComposition( table, pool ) );

        assertEquals( expectedComposition, actualComposition );
        verify( table ).forget( B );
        verify( table ).forget( C );
        verify( table ).forget( D );
    }

    @Test
    public void shouldFailWhenNoRoutersRespond()
    {
        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( A, new ServiceUnavailableException( "Hi!" ) ); // first -> non-fatal failure
        responsesByAddress.put( B, new SessionExpiredException( "Hi!" ) ); // second -> non-fatal failure
        responsesByAddress.put( C, new IOException( "Hi!" ) ); // third -> non-fatal failure

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        Rediscovery rediscovery = newRediscovery( A, compositionProvider, mock( HostNameResolver.class ) );
        RoutingTable table = routingTableMock( A, B, C );

        try
        {
            await( rediscovery.lookupClusterComposition( table, pool ) );
            fail( "Exception expected" );
        }
        catch ( ServiceUnavailableException e )
        {
            assertEquals( "Could not perform discovery. No routing servers available.", e.getMessage() );
        }
    }

    @Test
    public void shouldUseInitialRouterAfterDiscoveryReturnsNoWriters()
    {
        BoltServerAddress initialRouter = A;
        ClusterComposition noWritersComposition = new ClusterComposition( 42,
                asOrderedSet( D, E ), emptySet(), asOrderedSet( D, E ) );
        ClusterComposition validComposition = new ClusterComposition( 42,
                asOrderedSet( B, A ), asOrderedSet( B, A ), asOrderedSet( B, A ) );

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( B, new Success( noWritersComposition ) ); // first -> valid cluster composition
        responsesByAddress.put( initialRouter, new Success( validComposition ) ); // initial -> valid composition

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        HostNameResolver resolver = hostNameResolverMock( initialRouter, initialRouter );
        Rediscovery rediscovery = newRediscovery( initialRouter, compositionProvider, resolver );
        RoutingTable table = routingTableMock( B );

        ClusterComposition composition1 = await( rediscovery.lookupClusterComposition( table, pool ) );
        assertEquals( noWritersComposition, composition1 );

        ClusterComposition composition2 = await( rediscovery.lookupClusterComposition( table, pool ) );
        assertEquals( validComposition, composition2 );
    }

    @Test
    public void shouldUseInitialRouterToStartWith()
    {
        BoltServerAddress initialRouter = A;
        ClusterComposition validComposition = new ClusterComposition( 42,
                asOrderedSet( A ), asOrderedSet( A ), asOrderedSet( A ) );

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( initialRouter, new Success( validComposition ) ); // initial -> valid composition

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        HostNameResolver resolver = hostNameResolverMock( initialRouter, initialRouter );
        Rediscovery rediscovery = newRediscovery( initialRouter, compositionProvider, resolver, true );
        RoutingTable table = routingTableMock( B, C, D );

        ClusterComposition composition = await( rediscovery.lookupClusterComposition( table, pool ) );
        assertEquals( validComposition, composition );
    }

    @Test
    public void shouldUseKnownRoutersWhenInitialRouterFails()
    {
        BoltServerAddress initialRouter = A;
        ClusterComposition validComposition = new ClusterComposition( 42,
                asOrderedSet( D, E ), asOrderedSet( E, D ), asOrderedSet( A, B ) );

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( initialRouter, new ServiceUnavailableException( "Hi" ) ); // initial -> non-fatal error
        responsesByAddress.put( D, new IOException( "Hi" ) ); // first known -> non-fatal failure
        responsesByAddress.put( E, new Success( validComposition ) ); // second known -> valid composition

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        HostNameResolver resolver = hostNameResolverMock( initialRouter, initialRouter );
        Rediscovery rediscovery = newRediscovery( initialRouter, compositionProvider, resolver, true );
        RoutingTable table = routingTableMock( D, E );

        ClusterComposition composition = await( rediscovery.lookupClusterComposition( table, pool ) );
        assertEquals( validComposition, composition );
        verify( table ).forget( initialRouter );
        verify( table ).forget( D );
    }

    @Test
    public void shouldRetryConfiguredNumberOfTimesWithDelay()
    {
        int maxRoutingFailures = 3;
        long retryTimeoutDelay = 15;
        ClusterComposition expectedComposition = new ClusterComposition( 42,
                asOrderedSet( A, C ), asOrderedSet( B, D ), asOrderedSet( A, E ) );

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( A, new ServiceUnavailableException( "Hi!" ) );
        responsesByAddress.put( B, new ServiceUnavailableException( "Hi!" ) );
        responsesByAddress.put( E, new Success( expectedComposition ) );

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        HostNameResolver resolver = mock( HostNameResolver.class );
        when( resolver.resolve( A ) ).thenReturn( asOrderedSet( A ) )
                .thenReturn( asOrderedSet( A ) )
                .thenReturn( asOrderedSet( E ) );

        ImmediateSchedulingEventExecutor eventExecutor = new ImmediateSchedulingEventExecutor();
        RoutingSettings settings = new RoutingSettings( maxRoutingFailures, retryTimeoutDelay );
        Rediscovery rediscovery = new Rediscovery( A, settings, compositionProvider, resolver, eventExecutor,
                DEV_NULL_LOGGER, false );
        RoutingTable table = routingTableMock( A, B );

        ClusterComposition actualComposition = await( rediscovery.lookupClusterComposition( table, pool ) );

        assertEquals( expectedComposition, actualComposition );
        verify( table, times( maxRoutingFailures ) ).forget( A );
        verify( table, times( maxRoutingFailures ) ).forget( B );
        assertEquals( asList( retryTimeoutDelay, retryTimeoutDelay * 2 ), eventExecutor.scheduleDelays() );
    }

    @Test
    public void shouldNotLogWhenSingleRetryAttemptFails()
    {
        int maxRoutingFailures = 1;
        long retryTimeoutDelay = 10;

        Map<BoltServerAddress,Object> responsesByAddress = singletonMap( A, new ServiceUnavailableException( "Hi!" ) );
        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        HostNameResolver resolver = hostNameResolverMock( A, A );

        ImmediateSchedulingEventExecutor eventExecutor = new ImmediateSchedulingEventExecutor();
        RoutingSettings settings = new RoutingSettings( maxRoutingFailures, retryTimeoutDelay );
        Logger logger = mock( Logger.class );
        Rediscovery rediscovery = new Rediscovery( A, settings, compositionProvider, resolver, eventExecutor,
                logger, false );
        RoutingTable table = routingTableMock( A );

        try
        {
            await( rediscovery.lookupClusterComposition( table, pool ) );
            fail( "Exception expected" );
        }
        catch ( ServiceUnavailableException e )
        {
            assertEquals( "Could not perform discovery. No routing servers available.", e.getMessage() );
        }

        // rediscovery should not log about retries and should not schedule any retries
        verify( logger, never() ).info( startsWith( "Unable to fetch new routing table, will try again in " ) );
        assertEquals( 0, eventExecutor.scheduleDelays().size() );
    }

    private Rediscovery newRediscovery( BoltServerAddress initialRouter, ClusterCompositionProvider compositionProvider,
            HostNameResolver hostNameResolver )
    {
        return newRediscovery( initialRouter, compositionProvider, hostNameResolver, false );
    }

    private Rediscovery newRediscovery( BoltServerAddress initialRouter, ClusterCompositionProvider compositionProvider,
            HostNameResolver hostNameResolver, boolean useInitialRouter )
    {
        RoutingSettings settings = new RoutingSettings( 1, 0 );
        return new Rediscovery( initialRouter, settings, compositionProvider, hostNameResolver,
                GlobalEventExecutor.INSTANCE, DEV_NULL_LOGGER, useInitialRouter );
    }

    @SuppressWarnings( "unchecked" )
    private static ClusterCompositionProvider compositionProviderMock(
            Map<BoltServerAddress,Object> responsesByAddress )
    {
        ClusterCompositionProvider provider = mock( ClusterCompositionProvider.class );
        when( provider.getClusterComposition( any( CompletionStage.class ) ) ).then( invocation ->
        {
            CompletionStage<Connection> connectionStage = invocation.getArgumentAt( 0, CompletionStage.class );
            BoltServerAddress address = await( connectionStage ).serverAddress();
            Object response = responsesByAddress.get( address );
            assertNotNull( response );
            if ( response instanceof Throwable )
            {
                return failedFuture( (Throwable) response );
            }
            else
            {
                return completedFuture( response );
            }
        } );
        return provider;
    }

    private static HostNameResolver hostNameResolverMock( BoltServerAddress address, BoltServerAddress... resolved )
    {
        HostNameResolver resolver = mock( HostNameResolver.class );
        when( resolver.resolve( address ) ).thenReturn( asOrderedSet( resolved ) );
        return resolver;
    }

    private static ConnectionPool asyncConnectionPoolMock()
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        when( pool.acquire( any() ) ).then( invocation ->
        {
            BoltServerAddress address = invocation.getArgumentAt( 0, BoltServerAddress.class );
            return completedFuture( asyncConnectionMock( address ) );
        } );
        return pool;
    }

    private static Connection asyncConnectionMock( BoltServerAddress address )
    {
        Connection connection = mock( Connection.class );
        when( connection.serverAddress() ).thenReturn( address );
        return connection;
    }

    private static RoutingTable routingTableMock( BoltServerAddress... routers )
    {
        RoutingTable routingTable = mock( RoutingTable.class );
        AddressSet addressSet = new AddressSet();
        addressSet.update( asOrderedSet( routers ) );
        when( routingTable.routers() ).thenReturn( addressSet );
        return routingTable;
    }
}
