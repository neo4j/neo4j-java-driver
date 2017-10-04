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
package org.neo4j.driver.internal.async;

import io.netty.util.internal.ConcurrentSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;

// todo: remove this class
public class Main
{
    private static final int ITERATIONS = 200;

    private static final String QUERY1 = "RETURN 1";
    private static final String QUERY2 = "MATCH (n:ActiveItem) RETURN n LIMIT 50000";

    private static final String QUERY3 =
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

    private static final String QUERY = QUERY2;

    private static final Map<String,Object> PARAMS1 = new HashMap<>();
    private static final Map<String,Object> PARAMS2 = new HashMap<>();

    private static final Map<String,Object> PARAMS = PARAMS1;

    private static final String SCHEME = "bolt+routing";
    private static final String USER = "neo4j";
    private static final String PASSWORD = "test";
    private static final String HOST = "ec2-34-249-23-195.eu-west-1.compute.amazonaws.com";
    private static final int PORT = 26000;
    private static final String URI = SCHEME + "://" + HOST + ":" + PORT;

    static
    {
        PARAMS1.put( "skuNo", 366421 );
        PARAMS1.put( "itemSource", "REG" );
        PARAMS1.put( "catalogId", 2 );
        PARAMS1.put( "locale", "en" );
        Map<String,Object> tmpObj = new HashMap<>();
        tmpObj.put( "skuNo", 366421 );
        tmpObj.put( "itemSource", "REG" );
        PARAMS1.put( "itemList", Collections.singletonList( tmpObj ) );
    }

    public static void main( String[] args ) throws Throwable
    {
        testSessionRun();
        testSessionRunAsync();

        testTxRun();
        testTxRunAsync();
    }

    private static void testSessionRun() throws Throwable
    {
        test( "Session#run()", new Action()
        {
            @Override
            public void apply( Driver driver, MutableInt recordsRead, Set<String> serversUsed )
            {
                try ( Session session = driver.session( AccessMode.READ ) )
                {
                    StatementResult result = session.run( QUERY, PARAMS );
                    while ( result.hasNext() )
                    {
                        Record record = result.next();
                        useRecord( record );
                        recordsRead.increment();
                    }
                    serversUsed.add( result.summary().server().address() );
                }
            }
        } );
    }

    private static void testSessionRunAsync() throws Throwable
    {
        test( "Session#runAsync()", new Action()
        {
            @Override
            public void apply( Driver driver, MutableInt recordsRead, Set<String> serversUsed )
            {
                Session session = driver.session( AccessMode.READ );
                CompletionStage<StatementResultCursor> cursorFuture = session.runAsync( QUERY, PARAMS );
                StatementResultCursor cursor = await( cursorFuture );
                Record record;
                while ( (record = await( cursor.nextAsync() )) != null )
                {
                    useRecord( record );
                    recordsRead.increment();
                }
                serversUsed.add( await( cursor.summaryAsync() ).server().address() );
                await( session.closeAsync() );
            }
        } );
    }

    private static void testTxRun() throws Throwable
    {
        test( "Transaction#run()", new Action()
        {
            @Override
            public void apply( Driver driver, MutableInt recordsRead, Set<String> serversUsed )
            {
                try ( Session session = driver.session( AccessMode.READ );
                      Transaction tx = session.beginTransaction() )
                {
                    StatementResult result = tx.run( QUERY, PARAMS );
                    while ( result.hasNext() )
                    {
                        Record record = result.next();
                        useRecord( record );
                        recordsRead.increment();
                    }
                    tx.success();
                    serversUsed.add( result.summary().server().address() );
                }
            }
        } );
    }

    private static void testTxRunAsync() throws Throwable
    {
        test( "Transaction#runAsync()", new Action()
        {
            @Override
            public void apply( Driver driver, MutableInt recordsRead, Set<String> serversUsed )
            {
                Session session = driver.session( AccessMode.READ );
                Transaction tx = await( session.beginTransactionAsync() );
                StatementResultCursor cursor = await( tx.runAsync( QUERY, PARAMS ) );
                Record record;
                while ( (record = await( cursor.nextAsync() )) != null )
                {
                    useRecord( record );
                    recordsRead.increment();
                }
                serversUsed.add( await( cursor.summaryAsync() ).server().address() );
                await( tx.commitAsync() );
                await( session.closeAsync() );
            }
        } );
    }

    private static void test( String actionName, Action action ) throws Throwable
    {
        AuthToken authToken = AuthTokens.basic( USER, PASSWORD );
        Config config = Config.build().withoutEncryption().toConfig();

        List<Long> timings = new ArrayList<>();
        MutableInt recordsRead = new MutableInt();
        ConcurrentSet<String> serversUsed = new ConcurrentSet<>();

        try ( Driver driver = GraphDatabase.driver( URI, authToken, config ) )
        {
            for ( int i = 0; i < ITERATIONS; i++ )
            {
                long start = System.nanoTime();

                action.apply( driver, recordsRead, serversUsed );

                long end = System.nanoTime();
                timings.add( TimeUnit.NANOSECONDS.toMillis( end - start ) );
            }
        }

        timings = clean( timings );

        System.out.println( "============================================================" );
        System.out.println( actionName + ": mean --> " + mean( timings ) + "ms, stdDev --> " + stdDev( timings ) );
        System.out.println( actionName + ": timings --> " + timings );
        System.out.println( actionName + ": recordsRead --> " + recordsRead );
        System.out.println( actionName + ": serversUsed --> " + serversUsed );
        System.out.println( "============================================================" );
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

    private static <T> T await( CompletionStage<T> stage )
    {
        try
        {
            return stage.toCompletableFuture().get();
        }
        catch ( Throwable t )
        {
            throw new RuntimeException( t );
        }
    }

    private static void useRecord( Record record )
    {
        if ( record.keys().size() > 5 )
        {
            System.out.println( "Hello" );
        }

        if ( record.get( 0 ).isNull() )
        {
            System.out.println( " " );
        }

        if ( record.get( "A" ) == null )
        {
            System.out.println( "World" );
        }

//        System.out.println( record );
    }

    private interface Action
    {
        void apply( Driver driver, MutableInt recordsRead, Set<String> serversUsed );
    }

    private static class MutableInt
    {
        int value;

        void increment()
        {
            value++;
        }

        @Override
        public String toString()
        {
            return String.valueOf( value );
        }
    }
}
