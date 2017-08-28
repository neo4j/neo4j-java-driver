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
package org.neo4j.driver.internal.netty;

import io.netty.util.concurrent.Promise;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.SocketClient;
import org.neo4j.driver.internal.net.SocketConnection;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.summary.InternalServerInfo;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Value;

import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;
import static org.neo4j.driver.v1.Values.value;

public class Main
{
    private static final int ITERATIONS = 100;

    private static final String QUERY1 = "RETURN 1";

    private static final String QUERY =
            "MATCH (s:Sku{sku_no: {skuNo}})-[:HAS_ITEM_SOURCE]->(i:ItemSource{itemsource: {itemSource}})\n" +
            "//Get master sku for auxiliary item\n" +
            "OPTIONAL MATCH (s)-[:AUXILIARY_FOR]->(master_sku:Sku) WHERE NOT s.display_auxiliary_content\n" +
            "//Get New item for Used item\n" +
            "OPTIONAL MATCH (s)-[:USED_VERSION_OF]->(new_sku:Sku)\n" +
            "//Get other items like kit details and bundle includes\n" +
            "OPTIONAL MATCH (s)-[r:RELATED_ITEM]->(ri:Sku)\n" +
            "WITH i, r, coalesce(ri, master_sku, new_sku, s) as sku, coalesce(master_sku, new_sku, s) as final_sku\n" +
            "OPTIONAL MATCH (sku)-[:DESCRIBED_AS]->(d:Desc)\n" +
            "OPTIONAL MATCH (sku)-[:FEATURED_WITH]->(f:Feature)\n" +
            "//Get itemsource of related item\n" +
            "OPTIONAL MATCH (sku)-[:HAS_ITEM_SOURCE]->(relatedItemSource:ItemSource)" +
            "<-[:KIT_CONTAINS|INCLUDES_ITEMSOURCE*1..2]-(i)\n" +
            "WITH i, final_sku, sku, d, f, relatedItemSource, r\n" +
            "\tORDER BY f.seqnum\n" +
            "WITH final_sku, sku, r, d, i, relatedItemSource, CASE WHEN f IS NOT null THEN collect({\n" +
            "\ttitle: f.title,\n" +
            "\tbody: f.body,\n" +
            "\tisHeader: f.is_header,\n" +
            "\tnote: f.note\n" +
            "}) END as featureList ORDER BY coalesce(r.seqnum,0)\n" +
            "//Get description of kit header or bundle heder\n" +
            "OPTIONAL MATCH (final_sku)-[:DESCRIBED_AS]->(mainDescription:Desc) WHERE r is not null\n" +
            "RETURN\n" +
            "collect(DISTINCT CASE WHEN mainDescription is not null THEN\n" +
            "{\n" +
            "\titemName: null,\n" +
            "\tdescription: {\n" +
            "\t\ttext: mainDescription.description,\n" +
            "\t\tnote: mainDescription.description_note\n" +
            "\t},\n" +
            "\tfeatures: {\n" +
            "\t  \tnote: null,\n" +
            "\t\tfeatureList: null\n" +
            "\t},\n" +
            "\tupc: i.upc,\n" +
            "\thasThirdPartyContent: final_sku.has_third_party_content\n" +
            "} END)\n" +
            "+\n" +
            "collect({\n" +
            "\titemName: r.item_name,\n" +
            "\tdescription: {\n" +
            "\t\ttext: d.description,\n" +
            "\t\tnote: d.description_note\n" +
            "\t},\n" +
            "\tfeatures: {\n" +
            "\t\tnote: d.feature_note,\n" +
            "\t\tfeatureList: featureList\n" +
            "\t},\n" +
            "\tupc: coalesce(relatedItemSource, i).upc,\n" +
            "\thasThirdPartyContent: sku.has_third_party_content\n" +
            "}) AS overview;\n";

    private static final Map<String,Value> PARAMS = new HashMap<>();

    private static final String HOST = "localhost";
    private static final int PORT = 7687;
    private static final SecurityPlan SECURITY_PLAN;

    static
    {
        PARAMS.put( "skuNo", value( 366421 ) );
        PARAMS.put( "itemSource", value( "REG" ) );
        PARAMS.put( "catalogId", value( 2 ) );
        PARAMS.put( "locale", value( "en" ) );
        Map<String,Object> tmp = new HashMap<>();
        tmp.put( "skuNo", 366421 );
        tmp.put( "itemSource", "REG" );
        PARAMS.put( "itemList", value( Collections.singletonList( tmp ) ) );

        try
        {
            SECURITY_PLAN = SecurityPlan.forAllCertificates();
        }
        catch ( Throwable t )
        {
            throw new RuntimeException( t );
        }
    }

    public static void main( String[] args ) throws Throwable
    {
        testDriverFetchList();
//        testDriverFetchOneByOne();

//        testNetty();

//        testSocket();
    }

    private static void testDriverFetchList() throws Throwable
    {
        final AtomicReference<Object> status = new AtomicReference<>();

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "test" ),
                Config.build().withoutEncryption().toConfig() ) )
        {
            try ( Session session = driver.session() )
            {
                final StatementResultCursor cursor = session.runAsync( "unwind ['a', 'b', 'c', 'd', 'e'] as c return c",
                        Collections.<String,Object>emptyMap() );

                final ListenableFuture<List<Record>> all = ((InternalStatementResultCursor) cursor).allAsync();
                all.addListener( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            List<Record> records = all.get();
                            System.out.println( "-->> " + records );
                            status.set( new Object() );
                        }
                        catch ( Throwable t )
                        {
                            status.set( t );
                        }
                    }
                } );
            }

            while ( status.get() == null )
            {
                Thread.sleep( 1_000 );
            }

            Object res = status.get();
            if ( res instanceof Throwable )
            {
                throw ((Throwable) res);
            }

            System.out.println( "-->> done!" );
        }
    }

    private static void testDriverFetchOneByOne() throws Throwable
    {
        final AtomicReference<Object> status = new AtomicReference<>();

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "test" ),
                Config.build().withoutEncryption().toConfig() ) )
        {
            try ( Session session = driver.session() )
            {
                final StatementResultCursor cursor = session.runAsync( "unwind ['a', 'b', 'c', 'd', 'e'] as c return c",
                        Collections.<String,Object>emptyMap() );

                final ListenableFuture<Boolean> fetchAsync = cursor.fetchAsync();
                fetchAsync.addListener( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            Boolean available = fetchAsync.get();
                            System.out.println( "-->> record available: " + available );
                            Record record = cursor.current();
                            System.out.println( "-->> " + record );

                            final ListenableFuture<Boolean> fetchAsync1 = cursor.fetchAsync();
                            fetchAsync1.addListener( new Runnable()
                            {
                                @Override
                                public void run()
                                {
                                    try
                                    {
                                        Boolean available1 = fetchAsync1.get();
                                        System.out.println( "-->> record available: " + available1 );
                                        status.set( new Object() );
                                    }
                                    catch ( Throwable t )
                                    {
                                        status.set( t );
                                    }
                                }
                            } );
                        }
                        catch ( Throwable t )
                        {
                            status.set( t );
                        }
                    }
                } );
            }

            while ( status.get() == null )
            {
                Thread.sleep( 1_000 );
            }

            Object res = status.get();
            if ( res instanceof Throwable )
            {
                throw ((Throwable) res);
            }

            System.out.println( "-->> done!" );
        }
    }

    private static void testNetty() throws Throwable
    {
        BoltServerAddress address = new BoltServerAddress( HOST, PORT );
        AuthToken authToken = AuthTokens.basic( "neo4j", "test" );
        Map<String,Value> authTokenMap = ((InternalAuthToken) authToken).toMap();

        List<Long> timings = new ArrayList<>();
        try ( AsyncConnector bootstrap = new AsyncConnector( "Tester", authTokenMap, SECURITY_PLAN ) )
        {
            AsyncConnection connection = bootstrap.connect( address );

            for ( int i = 0; i < ITERATIONS; i++ )
            {
                long start = System.nanoTime();

                final Promise<Void> queryPromise = connection.newPromise();

                connection.run( QUERY, PARAMS, NoOpResponseHandler.INSTANCE );
                connection.pullAll( new ResponseHandler()
                {
                    @Override
                    public void onSuccess( Map<String,Value> metadata )
                    {
                        queryPromise.setSuccess( null );
                    }

                    @Override
                    public void onRecord( Value[] fields )
                    {
//                        System.out.println( "Received records: " + Arrays.toString( fields ) );
                    }

                    @Override
                    public void onFailure( Throwable error )
                    {
                        queryPromise.setFailure( error );
                    }
                } );
                connection.flush();

                queryPromise.await();
                if ( !queryPromise.isSuccess() )
                {
                    throw queryPromise.cause();
                }

                long end = System.nanoTime();

                timings.add( TimeUnit.NANOSECONDS.toMillis( end - start ) );
            }
        }

        timings = clean( timings );

        System.out.println( "Netty: mean --> " + mean( timings ) + "ms, stdDev --> " + stdDev( timings ) );
        System.out.println( "Netty: " + timings );
    }

    private static void testSocket()
    {
        BoltServerAddress address = new BoltServerAddress( HOST, PORT );
        SocketClient socket = new SocketClient( address, SECURITY_PLAN, 10_000, DEV_NULL_LOGGER );
        InternalServerInfo serverInfo = new InternalServerInfo( address, "" );
        SocketConnection connection = new SocketConnection( socket, serverInfo, DEV_NULL_LOGGER );

        AuthToken token = AuthTokens.basic( "neo4j", "test" );
        Map<String,Value> map = ((InternalAuthToken) token).toMap();

        connection.init( "Tester", map );

        List<Long> timings = new ArrayList<>();
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            long start = System.nanoTime();
            connection.run( QUERY, PARAMS, NoOpResponseHandler.INSTANCE );
            connection.pullAll( new ResponseHandler()
            {
                @Override
                public void onSuccess( Map<String,Value> metadata )
                {

                }

                @Override
                public void onFailure( Throwable error )
                {

                }

                @Override
                public void onRecord( Value[] fields )
                {
//                    System.out.println( "Received records: " + Arrays.toString( fields ) );
                }
            } );
            connection.sync();
            long end = System.nanoTime();

            timings.add( TimeUnit.NANOSECONDS.toMillis( end - start ) );
        }

        timings = clean( timings );

        System.out.println( "Socket: mean --> " + mean( timings ) + "ms, stdDev --> " + stdDev( timings ) );
        System.out.println( "Socket: " + timings );
    }

    private static List<Long> clean( List<Long> timings )
    {
        int warmup = timings.size() / 10; // remove first 10% of measurements, they are just a warmup :)
        return timings.subList( warmup, timings.size() );
    }

    private static long mean( List<Long> timings )
    {
        long sum = 0;
        for ( Long timing : timings )
        {
            sum += timing;
        }
        return sum / timings.size();
    }

    private static double stdDev( List<Long> timings )
    {
        long mean = mean( timings );
        long sum = 0;
        for ( Long timing : timings )
        {
            sum += ((timing - mean) * (timing - mean));
        }

        double squaredDiffMean = sum / timings.size();
        return (Math.sqrt( squaredDiffMean ));
    }
}
