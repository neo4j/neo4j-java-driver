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
package org.neo4j.driver.v1.integration;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Point;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.driver.internal.util.ServerVersion.v3_4_0;
import static org.neo4j.driver.v1.Values.ofPoint;
import static org.neo4j.driver.v1.Values.point;

public class SpatialTypesIT
{
    private static final int WGS_84_CRS_CODE = 4326;
    private static final int CARTESIAN_CRS_CODE = 7203;
    private static final double DELTA = 0.00001;

    @Rule
    public final TestNeo4jSession session = new TestNeo4jSession();

    @Before
    public void setUp()
    {
        assumeTrue( session.version().greaterThanOrEqual( v3_4_0 ) );
    }

    @Test
    public void shouldReceivePoint()
    {
        Record record = session.run( "RETURN point({x: 39.111748, y:-76.775635})" ).single();

        Point point = record.get( 0 ).asPoint();

        assertEquals( CARTESIAN_CRS_CODE, point.srid() );
        assertEquals( 39.111748, point.x(), DELTA );
        assertEquals( -76.775635, point.y(), DELTA );
    }

    @Test
    public void shouldSendPoint()
    {
        Value pointValue = point( WGS_84_CRS_CODE, 38.8719, 77.0563 );
        Record record1 = session.run( "CREATE (n:Node {location: $point}) RETURN 42", singletonMap( "point", pointValue ) ).single();

        assertEquals( 42, record1.get( 0 ).asInt() );

        Record record2 = session.run( "MATCH (n:Node) RETURN n.location" ).single();
        Point point = record2.get( 0 ).asPoint();

        assertEquals( WGS_84_CRS_CODE, point.srid() );
        assertEquals( 38.8719, point.x(), DELTA );
        assertEquals( 77.0563, point.y(), DELTA );
    }

    @Test
    public void shouldSendAndReceivePoint()
    {
        testPointSendAndReceive( point( CARTESIAN_CRS_CODE, 40.7624, 73.9738 ) );
    }

    @Test
    public void shouldSendAndReceiveRandom2DPoints()
    {
        Stream<Value> randomPoints = ThreadLocalRandom.current()
                .ints( 1_000, 0, 2 )
                .mapToObj( idx -> idx % 2 == 0
                                  ? point( WGS_84_CRS_CODE, randomDouble(), randomDouble() )
                                  : point( CARTESIAN_CRS_CODE, randomDouble(), randomDouble() ) );

        randomPoints.forEach( this::testPointSendAndReceive );
    }

    @Test
    public void shouldSendAndReceiveRandom2DPointArrays()
    {
        Stream<List<Value>> randomPointLists = ThreadLocalRandom.current()
                .ints( 1_000, 0, 2 )
                .mapToObj( SpatialTypesIT::randomPointList );

        randomPointLists.forEach( this::testPointListSendAndReceive );
    }

    private void testPointSendAndReceive( Value pointValue )
    {
        Point originalPoint = pointValue.asPoint();

        Record record = session.run( "CREATE (n {point: $point}) return n.point", singletonMap( "point", pointValue ) ).single();
        Point receivedPoint = record.get( 0 ).asPoint();

        assertPoints2DEqual( originalPoint, receivedPoint );
    }

    private void testPointListSendAndReceive( List<Value> points )
    {
        Record record = session.run( "CREATE (n {points: $points}) return n.points", singletonMap( "points", points ) ).single();
        List<Point> receivedPoints = record.get( 0 ).asList( ofPoint() );

        assertEquals( points.size(), receivedPoints.size() );
        for ( int i = 0; i < points.size(); i++ )
        {
            assertPoints2DEqual( points.get( i ).asPoint(), receivedPoints.get( i ) );
        }
    }

    private static List<Value> randomPointList( int index )
    {
        int size = ThreadLocalRandom.current().nextInt( 1, 100 );
        int srid = index % 2 == 0 ? CARTESIAN_CRS_CODE : WGS_84_CRS_CODE;
        return IntStream.range( 0, size )
                .mapToObj( i -> point( srid, randomDouble(), randomDouble() ) )
                .collect( toList() );
    }

    private static double randomDouble()
    {
        return ThreadLocalRandom.current().nextDouble( -180.0, 180 );
    }

    private static void assertPoints2DEqual( Point expected, Point actual )
    {
        String message = "Expected: " + expected + " but was: " + actual;
        assertEquals( message, expected.srid(), actual.srid() );
        assertEquals( message, expected.x(), actual.x(), DELTA );
        assertEquals( message, expected.y(), actual.y(), DELTA );
    }
}
