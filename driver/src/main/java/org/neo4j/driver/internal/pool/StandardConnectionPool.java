package org.neo4j.driver.internal.pool;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.connector.socket.SocketConnector;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.internal.spi.Logging;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Consumer;

import static java.lang.Integer.getInteger;

/**
 * A basic connection pool that optimizes for threads being long-lived, acquiring/releasing many connections.
 * It uses a global queue as a fallback pool, but tries to avoid coordination by storing connections in a ThreadLocal.
 * <p>
 * Safety is achieved by tracking thread locals getting garbage collected, returning connections to the global pool
 * when this happens.
 * <p>
 * If threads are long-lived, this pool will achieve linearly scalable performance with overhead equivalent to a
 * hash-map lookup per acquire.
 * <p>
 * If threads are short-lived, this pool is not ideal.
 */
public class StandardConnectionPool implements ConnectionPool
{
    // TODO: This should be dealt with a general config mechanism for the driver, rather than java properties
    private static int defaultConnectionsPerDatabase = getInteger( "neo4j.connectionsPerDatabase", 10 );
    private static long defaultMaxConnectionIdleTime = getInteger( "neo4j.maxConnectionIdleMillis", 60 * 5 * 1000 );

    /**
     * Map of scheme -> connector, this is what we use to establish new connections.
     */
    private final ConcurrentHashMap<String,Connector> connectors = new ConcurrentHashMap<>();

    /**
     * Pools, organized by URL.
     */
    private final ConcurrentHashMap<URI,ThreadCachingPool<PooledConnection>> pools = new ConcurrentHashMap<>();

    /**
     * Connections that fail this criteria will be disposed of.
     */
    private final ValidationStrategy<PooledConnection> connectionValidation;

    private final int connectionsPerDatabase;
    private final Clock clock;

    public StandardConnectionPool( Logging logging )
    {
        this( loadConnectors( logging ) );
    }

    public StandardConnectionPool( Collection<Connector> conns )
    {
        this( defaultConnectionsPerDatabase, defaultMaxConnectionIdleTime, conns, Clock.SYSTEM );
    }

    public StandardConnectionPool( int connectionsPerDatabase, long maxConnIdleTimeMillis,
            Collection<Connector> conns, Clock clock )
    {
        this.connectionsPerDatabase = connectionsPerDatabase;
        this.clock = clock;
        this.connectionValidation = new PooledConnectionValidator( maxConnIdleTimeMillis );
        for ( Connector connector : conns )
        {
            for ( String s : connector.supportedSchemes() )
            {
                this.connectors.put( s, connector );
            }
        }
    }

    @Override
    public Connection acquire( URI sessionURI )
    {
        try
        {
            return pool( sessionURI ).acquire( 30, TimeUnit.SECONDS );
        }
        catch ( InterruptedException e )
        {
            throw new ClientException( "Interrupted while waiting for a connection to Neo4j." );
        }
    }

    private ThreadCachingPool<PooledConnection> pool( URI sessionURI )
    {
        ThreadCachingPool<PooledConnection> pool = pools.get( sessionURI );
        if ( pool == null )
        {
            pool = newPool( sessionURI );
            if ( pools.putIfAbsent( sessionURI, pool ) != null )
            {
                // We lost a race to create the pool, dispose of the one we created, and recurse
                pool.close();
                return pool( sessionURI );
            }
        }
        return pool;
    }

    @SuppressWarnings( "SameReturnValue" )
    private static Collection<Connector> loadConnectors( Logging logging )
    {
        List<Connector> connectors = new LinkedList<>();

        // Hard code socket connector
        Connector conn = new SocketConnector();
        conn.setLogging( logging );
        connectors.add( conn );

        // Load custom loadConnectors via JSL
        ServiceLoader<Connector> load = ServiceLoader.load( Connector.class );
        for ( Connector connector : load )
        {
            connector.setLogging( logging );
            connectors.add( connector );
        }
        return connectors;
    }

    @Override
    public void close() throws Exception
    {
        for ( ThreadCachingPool<PooledConnection> pool : pools.values() )
        {
            pool.close();
        }
        pools.clear();
    }

    private String connectorSchemes()
    {
        return Arrays.toString( connectors.keySet().toArray( new String[connectors.keySet().size()] ) );
    }

    private ThreadCachingPool<PooledConnection> newPool( final URI uri )
    {

        return new ThreadCachingPool<>( connectionsPerDatabase, new Allocator<PooledConnection>()
        {
            @Override
            public PooledConnection create( Consumer<PooledConnection> release )
            {
                Connector connector = connectors.get( uri.getScheme() );
                if ( connector == null )
                {
                    throw new ClientException(
                            "'" + uri.getScheme() + "' is not a supported transport (in '" +
                            uri + "', available transports are: " + connectorSchemes() + "." );
                }
                Connection conn = connector.connect( uri );
                return new PooledConnection( conn, release );
            }

            @Override
            public void onDispose( PooledConnection pooledConnection )
            {
                pooledConnection.dispose();
            }

            @Override
            public void onAcquire( PooledConnection pooledConnection )
            {

            }
        }, connectionValidation, clock );
    }
}
