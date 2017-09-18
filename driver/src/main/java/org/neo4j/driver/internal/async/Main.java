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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Response;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;

public class Main
{
    private static final int ITERATIONS = 100;

    private static final String QUERY1 = "MATCH (n:ActiveItem) RETURN n LIMIT 10000";

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

    private static final Map<String,Object> PARAMS_OBJ = new HashMap<>();

    private static final String USER = "neo4j";
    private static final String PASSWORD = "test";
    private static final String HOST = "ec2-54-73-57-164.eu-west-1.compute.amazonaws.com";
    private static final int PORT = 7687;
    private static final String URI = "bolt://" + HOST + ":" + PORT;

    static
    {
        PARAMS_OBJ.put( "skuNo", 366421 );
        PARAMS_OBJ.put( "itemSource", "REG" );
        PARAMS_OBJ.put( "catalogId", 2 );
        PARAMS_OBJ.put( "locale", "en" );
        Map<String,Object> tmpObj = new HashMap<>();
        tmpObj.put( "skuNo", 366421 );
        tmpObj.put( "itemSource", "REG" );
        PARAMS_OBJ.put( "itemList", Collections.singletonList( tmpObj ) );
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
            public void apply( Driver driver, MutableInt recordsRead )
            {
                try ( Session session = driver.session() )
                {
                    StatementResult result = session.run( QUERY, PARAMS_OBJ );
                    while ( result.hasNext() )
                    {
                        Record record = result.next();
                        useRecord( record );
                        recordsRead.increment();
                    }
                }
            }
        } );
    }

    private static void testSessionRunAsync() throws Throwable
    {
        test( "Session#runAsync()", new Action()
        {
            @Override
            public void apply( Driver driver, MutableInt recordsRead )
            {
                Session session = driver.session();
                Response<StatementResultCursor> cursorResponse = session.runAsync( QUERY, PARAMS_OBJ );
                StatementResultCursor cursor = await( cursorResponse );
                while ( await( cursor.fetchAsync() ) )
                {
                    Record record = cursor.current();
                    useRecord( record );
                    recordsRead.increment();
                }
                await( session.closeAsync() );
            }
        } );
    }

    private static void testTxRun() throws Throwable
    {
        test( "Transaction#run()", new Action()
        {
            @Override
            public void apply( Driver driver, MutableInt recordsRead )
            {
                try ( Session session = driver.session();
                      Transaction tx = session.beginTransaction() )
                {
                    StatementResult result = tx.run( QUERY, PARAMS_OBJ );
                    while ( result.hasNext() )
                    {
                        Record record = result.next();
                        useRecord( record );
                        recordsRead.increment();
                    }
                    tx.success();
                }
            }
        } );
    }

    private static void testTxRunAsync() throws Throwable
    {
        test( "Transaction#runAsync()", new Action()
        {
            @Override
            public void apply( Driver driver, MutableInt recordsRead )
            {
                Session session = driver.session();
                Transaction tx = await( session.beginTransactionAsync() );
                StatementResultCursor cursor = await( tx.runAsync( QUERY, PARAMS_OBJ ) );
                while ( await( cursor.fetchAsync() ) )
                {
                    Record record = cursor.current();
                    useRecord( record );
                    recordsRead.increment();
                }
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

        try ( Driver driver = GraphDatabase.driver( URI, authToken, config ) )
        {
            for ( int i = 0; i < ITERATIONS; i++ )
            {
                long start = System.nanoTime();

                action.apply( driver, recordsRead );

                long end = System.nanoTime();
                timings.add( TimeUnit.NANOSECONDS.toMillis( end - start ) );
            }
        }

        timings = clean( timings );

        System.out.println( "============================================================" );
        System.out.println( actionName + ": mean --> " + mean( timings ) + "ms, stdDev --> " + stdDev( timings ) );
        System.out.println( actionName + ": timings --> " + timings );
        System.out.println( actionName + ": recordsRead --> " + recordsRead );
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

    private static <T, U extends Future<T>> T await( U future )
    {
        try
        {
            return future.get();
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
        void apply( Driver driver, MutableInt recordsRead );
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
