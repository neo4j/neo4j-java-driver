/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.driver.v1.integration;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Point2D;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.driver.internal.util.ServerVersion.v3_4_0;
import static org.neo4j.driver.v1.Values.point2D;

public class PointsIT
{
    private static final long WGS_84_CRS_CODE = 4326;
    private static final long CARTESIAN_CRS_CODE = 7203;
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

        Point2D point = record.get( 0 ).asPoint2D();

        assertEquals( CARTESIAN_CRS_CODE, point.srid() );
        assertEquals( 39.111748, point.x(), DELTA );
        assertEquals( -76.775635, point.y(), DELTA );
    }

    @Test
    public void shouldSendPoint()
    {
        Value pointValue = point2D( WGS_84_CRS_CODE, 38.8719, 77.0563 );
        Record record1 = session.run( "CREATE (n:Node {location: $point}) RETURN 42", singletonMap( "point", pointValue ) ).single();

        assertEquals( 42, record1.get( 0 ).asInt() );

        Record record2 = session.run( "MATCH (n:Node) RETURN n.location" ).single();
        Point2D point = record2.get( 0 ).asPoint2D();

        assertEquals( WGS_84_CRS_CODE, point.srid() );
        assertEquals( 38.8719, point.x(), DELTA );
        assertEquals( 77.0563, point.y(), DELTA );
    }

    @Test
    public void shouldSendAndReceivePoint()
    {
        testPointSendAndReceive( point2D( CARTESIAN_CRS_CODE, 40.7624, 73.9738 ) );
    }

    @Test
    public void shouldSendAndReceiveRandomPoints()
    {
        Stream<Value> randomPoints = ThreadLocalRandom.current()
                .ints( 1_000, 0, 2 )
                .mapToObj( idx -> idx % 2 == 0
                                  ? point2D( WGS_84_CRS_CODE, randomDouble(), randomDouble() )
                                  : point2D( CARTESIAN_CRS_CODE, randomDouble(), randomDouble() ) );

        randomPoints.forEach( this::testPointSendAndReceive );
    }

    private void testPointSendAndReceive( Value pointValue )
    {
        Point2D originalPoint = pointValue.asPoint2D();

        Record record = session.run( "CREATE (n {p:$point}) return n.p", singletonMap( "point", pointValue ) ).single();
        Point2D receivedPoint = record.get( 0 ).asPoint2D();

        String message = "Failed for " + originalPoint;
        assertEquals( message, originalPoint.srid(), receivedPoint.srid() );
        assertEquals( message, originalPoint.x(), receivedPoint.x(), DELTA );
        assertEquals( message, originalPoint.y(), receivedPoint.y(), DELTA );
    }

    private static double randomDouble()
    {
        return ThreadLocalRandom.current().nextDouble( -180.0, 180 );
    }
}
