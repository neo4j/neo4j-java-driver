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

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Point;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.neo4j.driver.v1.Values.point;

public class PointTypeIT
{
    private static final int EPSG_TABLE_ID = 1;
    private static final int WGS_84_CRS_CODE = 4326;

    private static final int SR_ORG_TABLE_ID = 2;
    private static final int CARTESIAN_CRS_CODE = 7203;

    @Rule
    public final TestNeo4jSession session = new TestNeo4jSession();

    @Test
    public void shouldReceivePoint()
    {
        Record record = session.run( "RETURN point({x: 39.111748, y:-76.775635})" ).single();

        Point point = record.get( 0 ).asPoint();

        assertEquals( SR_ORG_TABLE_ID, point.crsTableId() );
        assertEquals( CARTESIAN_CRS_CODE, point.crsCode() );
        assertEquals( asList( 39.111748, -76.775635 ), point.coordinate().values() );
    }

    @Test
    public void shouldSendPoint()
    {
        Value pointValue = point( EPSG_TABLE_ID, WGS_84_CRS_CODE, 38.8719, 77.0563 );
        Record record1 = session.run( "CREATE (n:Node {location: $point}) RETURN 42", singletonMap( "point", pointValue ) ).single();

        assertEquals( 42, record1.get( 0 ).asInt() );

        Record record2 = session.run( "MATCH (n:Node) RETURN n.location" ).single();
        Point point = record2.get( 0 ).asPoint();

        assertEquals( EPSG_TABLE_ID, point.crsTableId() );
        assertEquals( WGS_84_CRS_CODE, point.crsCode() );
        assertEquals( asList( 38.8719, 77.0563 ), point.coordinate().values() );
    }

    @Test
    public void shouldSendAndReceivePoint()
    {
        testPointSendAndReceive( SR_ORG_TABLE_ID, CARTESIAN_CRS_CODE, 40.7624, 73.9738 );
    }

    @Test
    public void shouldSendAndReceiveRandomPoints()
    {
        Stream<Value> randomPoints = ThreadLocalRandom.current()
                .ints( 1_000, 0, 2 )
                .mapToObj( idx -> idx % 2 == 0
                                  ? point( EPSG_TABLE_ID, WGS_84_CRS_CODE, randomCoordinate() )
                                  : point( SR_ORG_TABLE_ID, CARTESIAN_CRS_CODE, randomCoordinate() ) );

        randomPoints.forEach( this::testPointSendAndReceive );
    }

    private void testPointSendAndReceive( long crsTableId, long crsCode, double... coordinate )
    {
        testPointSendAndReceive( point( crsTableId, crsCode, coordinate ) );
    }

    private void testPointSendAndReceive( Value pointValue )
    {
        Point originalPoint = pointValue.asPoint();

        Record record = session.run( "CREATE (n {p:$point}) return n.p", singletonMap( "point", pointValue ) ).single();
        Point receivedPoint = record.get( 0 ).asPoint();

        String message = "Failed for " + originalPoint;
        assertEquals( message, originalPoint.crsTableId(), receivedPoint.crsTableId() );
        assertEquals( message, originalPoint.crsCode(), receivedPoint.crsCode() );
        assertEquals( message, originalPoint.coordinate().values(), receivedPoint.coordinate().values() );
    }

    private static double[] randomCoordinate()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int count = random.nextInt( 2, 4 ); // either 2D or 3D point
        return random.doubles( count, -180.0, 180 ).toArray();
    }
}
