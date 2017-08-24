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

import io.netty.channel.ChannelPromise;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.internal.messaging.InitMessage;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.SocketClient;
import org.neo4j.driver.internal.net.SocketConnection;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.summary.InternalServerInfo;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;
import static org.neo4j.driver.internal.messaging.PullAllMessage.PULL_ALL;
import static org.neo4j.driver.v1.Values.value;

public class Main
{
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
    }

    private static final String HOST = "localhost";
    private static final int PORT = 7687;

    public static void main( String[] args ) throws Exception
    {
        testNetty();
        System.out.println( "----------------------" );
        testSocket();
    }

    private static void testNetty() throws Exception
    {
        try ( ChannelBootstrap bootstrap = new ChannelBootstrap() )
        {
            NettyConnection connection = bootstrap.connect( "localhost", PORT );

            AuthToken token = AuthTokens.basic( "neo4j", "test" );
            Map<String,Value> map = ((InternalAuthToken) token).toMap();
            InitMessage initMessage = new InitMessage( "Tester", map );

            final ChannelPromise connectionInitializedPromise = connection.newPromise();

            connection.sendAndFlush( initMessage, new ResponseHandler()
            {
                @Override
                public void onSuccess( Map<String,Value> meta )
                {
                    connectionInitializedPromise.setSuccess();
                }

                @Override
                public void onFailure( String code, String message )
                {
                    // todo: detect proper exception type here based on error code
                    connectionInitializedPromise.setFailure( new ClientException( code, message ) );
                }

                @Override
                public void onRecord( Value[] fields )
                {
                    throw new UnsupportedOperationException();
                }
            } );

            connectionInitializedPromise.await();

            for ( int i = 0; i < 100; i++ )
            {
                long start = System.nanoTime();
                final ChannelPromise queryPromise = connection.newPromise();

                connection.send( new RunMessage( QUERY, PARAMS ), new VoidResponseHandler() );
                connection.sendAndFlush( PULL_ALL, new ResponseHandler()
                {
                    @Override
                    public void onSuccess( Map<String,Value> meta )
                    {
                        queryPromise.setSuccess();
                    }

                    @Override
                    public void onRecord( Value[] fields )
                    {
                        System.out.println( "Received records: " + Arrays.toString( fields ) );
                    }

                    @Override
                    public void onFailure( String code, String message )
                    {
                        queryPromise.setFailure( new ClientException( code, message ) );
                    }
                } );

                queryPromise.await();
                long end = System.nanoTime();
                System.out.println( "Query took: " + TimeUnit.NANOSECONDS.toMillis( end - start ) + "ms" );
            }
        }
    }

    private static void testSocket()
    {
        BoltServerAddress address = new BoltServerAddress( HOST, PORT );
        SocketClient socket = new SocketClient( address, SecurityPlan.insecure(), 10_000, DEV_NULL_LOGGER );
        InternalServerInfo serverInfo = new InternalServerInfo( address, "" );
        SocketConnection connection = new SocketConnection( socket, serverInfo, DEV_NULL_LOGGER );

        AuthToken token = AuthTokens.basic( "neo4j", "test" );
        Map<String,Value> map = ((InternalAuthToken) token).toMap();

        connection.init( "Tester", map );

        for ( int i = 0; i < 100; i++ )
        {
            long start = System.nanoTime();
            connection.run( QUERY, PARAMS, new Collector.NoOperationCollector() );
            connection.pullAll( new Collector.NoOperationCollector()
            {
                @Override
                public void record( Value[] fields )
                {
                    System.out.println( "Received records: " + Arrays.toString( fields ) );
                }
            } );
            connection.sync();
            long end = System.nanoTime();
            System.out.println( "Query took: " + TimeUnit.NANOSECONDS.toMillis( end - start ) + "ms" );
        }
    }
}
