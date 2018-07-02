/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.internal.cluster;

import io.netty.util.concurrent.GlobalEventExecutor;
import org.junit.jupiter.api.Test;

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
import org.neo4j.driver.v1.net.ServerAddressResolver;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
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

class RediscoveryTest
{
    private final ConnectionPool pool = asyncConnectionPoolMock();

    @Test
    void shouldUseFirstRouterInTable()
    {
        ClusterComposition expectedComposition = new ClusterComposition( 42,
                asOrderedSet( B, C ), asOrderedSet( C, D ), asOrderedSet( B ) );

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( B, new Success( expectedComposition ) ); // first -> valid cluster composition

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        Rediscovery rediscovery = newRediscovery( A, compositionProvider, mock( ServerAddressResolver.class ) );
        RoutingTable table = routingTableMock( B );

        ClusterComposition actualComposition = await( rediscovery.lookupClusterComposition( table, pool ) );

        assertEquals( expectedComposition, actualComposition );
        verify( table, never() ).forget( B );
    }

    @Test
    void shouldSkipFailingRouters()
    {
        ClusterComposition expectedComposition = new ClusterComposition( 42,
                asOrderedSet( A, B, C ), asOrderedSet( B, C, D ), asOrderedSet( A, B ) );

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( A, new RuntimeException( "Hi!" ) ); // first -> non-fatal failure
        responsesByAddress.put( B, new ServiceUnavailableException( "Hi!" ) ); // second -> non-fatal failure
        responsesByAddress.put( C, new Success( expectedComposition ) ); // third -> valid cluster composition

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        Rediscovery rediscovery = newRediscovery( A, compositionProvider, mock( ServerAddressResolver.class ) );
        RoutingTable table = routingTableMock( A, B, C );

        ClusterComposition actualComposition = await( rediscovery.lookupClusterComposition( table, pool ) );

        assertEquals( expectedComposition, actualComposition );
        verify( table ).forget( A );
        verify( table ).forget( B );
        verify( table, never() ).forget( C );
    }

    @Test
    void shouldFailImmediatelyOnAuthError()
    {
        AuthenticationException authError = new AuthenticationException( "Neo.ClientError.Security.Unauthorized",
                "Wrong password" );

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( A, new RuntimeException( "Hi!" ) ); // first router -> non-fatal failure
        responsesByAddress.put( B, authError ); // second router -> fatal auth error

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        Rediscovery rediscovery = newRediscovery( A, compositionProvider, mock( ServerAddressResolver.class ) );
        RoutingTable table = routingTableMock( A, B, C );

        AuthenticationException error = assertThrows( AuthenticationException.class, () -> await( rediscovery.lookupClusterComposition( table, pool ) ) );
        assertEquals( authError, error );
        verify( table ).forget( A );
    }

    @Test
    void shouldFallbackToInitialRouterWhenKnownRoutersFail()
    {
        BoltServerAddress initialRouter = A;
        ClusterComposition expectedComposition = new ClusterComposition( 42,
                asOrderedSet( C, B, A ), asOrderedSet( A, B ), asOrderedSet( D, E ) );

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( B, new ServiceUnavailableException( "Hi!" ) ); // first -> non-fatal failure
        responsesByAddress.put( C, new ServiceUnavailableException( "Hi!" ) ); // second -> non-fatal failure
        responsesByAddress.put( initialRouter, new Success( expectedComposition ) ); // initial -> valid response

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        ServerAddressResolver resolver = resolverMock( initialRouter, initialRouter );
        Rediscovery rediscovery = newRediscovery( initialRouter, compositionProvider, resolver );
        RoutingTable table = routingTableMock( B, C );

        ClusterComposition actualComposition = await( rediscovery.lookupClusterComposition( table, pool ) );

        assertEquals( expectedComposition, actualComposition );
        verify( table ).forget( B );
        verify( table ).forget( C );
    }

    @Test
    void shouldFailImmediatelyWhenClusterCompositionProviderReturnsFailure()
    {
        ClusterComposition validComposition = new ClusterComposition( 42,
                asOrderedSet( A ), asOrderedSet( B ), asOrderedSet( C ) );
        ProtocolException protocolError = new ProtocolException( "Wrong record!" );

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( B, new Failure( protocolError ) ); // first -> fatal failure
        responsesByAddress.put( C, new Success( validComposition ) ); // second -> valid cluster composition

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        Rediscovery rediscovery = newRediscovery( A, compositionProvider, mock( ServerAddressResolver.class ) );
        RoutingTable table = routingTableMock( B, C );

        ProtocolException error = assertThrows( ProtocolException.class, () -> await( rediscovery.lookupClusterComposition( table, pool ) ) );
        assertEquals( protocolError, error );
    }

    @Test
    void shouldResolveInitialRouterAddress()
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
        ServerAddressResolver resolver = resolverMock( initialRouter, D, E );
        Rediscovery rediscovery = newRediscovery( initialRouter, compositionProvider, resolver );
        RoutingTable table = routingTableMock( B, C );

        ClusterComposition actualComposition = await( rediscovery.lookupClusterComposition( table, pool ) );

        assertEquals( expectedComposition, actualComposition );
        verify( table ).forget( B );
        verify( table ).forget( C );
        verify( table ).forget( D );
    }

    @Test
    void shouldResolveInitialRouterAddressUsingCustomResolver()
    {
        ClusterComposition expectedComposition = new ClusterComposition( 42,
                asOrderedSet( A, B, C ), asOrderedSet( A, B, C ), asOrderedSet( B, E ) );

        ServerAddressResolver resolver = address ->
        {
            assertEquals( A, address );
            return asOrderedSet( B, C, E );
        };

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( B, new ServiceUnavailableException( "Hi!" ) ); // first -> non-fatal failure
        responsesByAddress.put( C, new ServiceUnavailableException( "Hi!" ) ); // second -> non-fatal failure
        responsesByAddress.put( E, new Success( expectedComposition ) ); // resolved second -> valid response

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        Rediscovery rediscovery = newRediscovery( A, compositionProvider, resolver );
        RoutingTable table = routingTableMock( B, C );

        ClusterComposition actualComposition = await( rediscovery.lookupClusterComposition( table, pool ) );

        assertEquals( expectedComposition, actualComposition );
        verify( table ).forget( B );
        verify( table ).forget( C );
    }

    @Test
    void shouldUseInitialRouterAddressAsIsWhenResolverFails()
    {
        ClusterComposition expectedComposition = new ClusterComposition( 42,
                asOrderedSet( A, B ), asOrderedSet( A, B ), asOrderedSet( A, B ) );

        Map<BoltServerAddress,Object> responsesByAddress = singletonMap( A, new Success( expectedComposition ) );
        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );

        // failing server address resolver
        ServerAddressResolver resolver = mock( ServerAddressResolver.class );
        when( resolver.resolve( A ) ).thenThrow( new RuntimeException( "Resolver fails!" ) );

        Rediscovery rediscovery = newRediscovery( A, compositionProvider, resolver );
        RoutingTable table = routingTableMock();

        ClusterComposition actualComposition = await( rediscovery.lookupClusterComposition( table, pool ) );

        assertEquals( expectedComposition, actualComposition );
        verify( resolver ).resolve( A );
        verify( table, never() ).forget( any() );
    }

    @Test
    void shouldFailWhenNoRoutersRespond()
    {
        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( A, new ServiceUnavailableException( "Hi!" ) ); // first -> non-fatal failure
        responsesByAddress.put( B, new SessionExpiredException( "Hi!" ) ); // second -> non-fatal failure
        responsesByAddress.put( C, new IOException( "Hi!" ) ); // third -> non-fatal failure

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        Rediscovery rediscovery = newRediscovery( A, compositionProvider, mock( ServerAddressResolver.class ) );
        RoutingTable table = routingTableMock( A, B, C );

        ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, () -> await( rediscovery.lookupClusterComposition( table, pool ) ) );
        assertEquals( "Could not perform discovery. No routing servers available.", e.getMessage() );
    }

    @Test
    void shouldUseInitialRouterAfterDiscoveryReturnsNoWriters()
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
        ServerAddressResolver resolver = resolverMock( initialRouter, initialRouter );
        Rediscovery rediscovery = newRediscovery( initialRouter, compositionProvider, resolver );
        RoutingTable table = routingTableMock( B );

        ClusterComposition composition1 = await( rediscovery.lookupClusterComposition( table, pool ) );
        assertEquals( noWritersComposition, composition1 );

        ClusterComposition composition2 = await( rediscovery.lookupClusterComposition( table, pool ) );
        assertEquals( validComposition, composition2 );
    }

    @Test
    void shouldUseInitialRouterToStartWith()
    {
        BoltServerAddress initialRouter = A;
        ClusterComposition validComposition = new ClusterComposition( 42,
                asOrderedSet( A ), asOrderedSet( A ), asOrderedSet( A ) );

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( initialRouter, new Success( validComposition ) ); // initial -> valid composition

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        ServerAddressResolver resolver = resolverMock( initialRouter, initialRouter );
        Rediscovery rediscovery = newRediscovery( initialRouter, compositionProvider, resolver, true );
        RoutingTable table = routingTableMock( B, C, D );

        ClusterComposition composition = await( rediscovery.lookupClusterComposition( table, pool ) );
        assertEquals( validComposition, composition );
    }

    @Test
    void shouldUseKnownRoutersWhenInitialRouterFails()
    {
        BoltServerAddress initialRouter = A;
        ClusterComposition validComposition = new ClusterComposition( 42,
                asOrderedSet( D, E ), asOrderedSet( E, D ), asOrderedSet( A, B ) );

        Map<BoltServerAddress,Object> responsesByAddress = new HashMap<>();
        responsesByAddress.put( initialRouter, new ServiceUnavailableException( "Hi" ) ); // initial -> non-fatal error
        responsesByAddress.put( D, new IOException( "Hi" ) ); // first known -> non-fatal failure
        responsesByAddress.put( E, new Success( validComposition ) ); // second known -> valid composition

        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        ServerAddressResolver resolver = resolverMock( initialRouter, initialRouter );
        Rediscovery rediscovery = newRediscovery( initialRouter, compositionProvider, resolver, true );
        RoutingTable table = routingTableMock( D, E );

        ClusterComposition composition = await( rediscovery.lookupClusterComposition( table, pool ) );
        assertEquals( validComposition, composition );
        verify( table ).forget( initialRouter );
        verify( table ).forget( D );
    }

    @Test
    void shouldRetryConfiguredNumberOfTimesWithDelay()
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
        ServerAddressResolver resolver = mock( ServerAddressResolver.class );
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
    void shouldNotLogWhenSingleRetryAttemptFails()
    {
        int maxRoutingFailures = 1;
        long retryTimeoutDelay = 10;

        Map<BoltServerAddress,Object> responsesByAddress = singletonMap( A, new ServiceUnavailableException( "Hi!" ) );
        ClusterCompositionProvider compositionProvider = compositionProviderMock( responsesByAddress );
        ServerAddressResolver resolver = resolverMock( A, A );

        ImmediateSchedulingEventExecutor eventExecutor = new ImmediateSchedulingEventExecutor();
        RoutingSettings settings = new RoutingSettings( maxRoutingFailures, retryTimeoutDelay );
        Logger logger = mock( Logger.class );
        Rediscovery rediscovery = new Rediscovery( A, settings, compositionProvider, resolver, eventExecutor,
                logger, false );
        RoutingTable table = routingTableMock( A );

        ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, () -> await( rediscovery.lookupClusterComposition( table, pool ) ) );
        assertEquals( "Could not perform discovery. No routing servers available.", e.getMessage() );

        // rediscovery should not log about retries and should not schedule any retries
        verify( logger, never() ).info( startsWith( "Unable to fetch new routing table, will try again in " ) );
        assertEquals( 0, eventExecutor.scheduleDelays().size() );
    }

    private Rediscovery newRediscovery( BoltServerAddress initialRouter, ClusterCompositionProvider compositionProvider,
            ServerAddressResolver resolver )
    {
        return newRediscovery( initialRouter, compositionProvider, resolver, false );
    }

    private Rediscovery newRediscovery( BoltServerAddress initialRouter, ClusterCompositionProvider compositionProvider,
            ServerAddressResolver resolver, boolean useInitialRouter )
    {
        RoutingSettings settings = new RoutingSettings( 1, 0 );
        return new Rediscovery( initialRouter, settings, compositionProvider, resolver,
                GlobalEventExecutor.INSTANCE, DEV_NULL_LOGGER, useInitialRouter );
    }

    @SuppressWarnings( "unchecked" )
    private static ClusterCompositionProvider compositionProviderMock(
            Map<BoltServerAddress,Object> responsesByAddress )
    {
        ClusterCompositionProvider provider = mock( ClusterCompositionProvider.class );
        when( provider.getClusterComposition( any( CompletionStage.class ) ) ).then( invocation ->
        {
            CompletionStage<Connection> connectionStage = invocation.getArgument( 0 );
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

    private static ServerAddressResolver resolverMock( BoltServerAddress address, BoltServerAddress... resolved )
    {
        ServerAddressResolver resolver = mock( ServerAddressResolver.class );
        when( resolver.resolve( address ) ).thenReturn( asOrderedSet( resolved ) );
        return resolver;
    }

    private static ConnectionPool asyncConnectionPoolMock()
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        when( pool.acquire( any() ) ).then( invocation ->
        {
            BoltServerAddress address = invocation.getArgument( 0 );
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
