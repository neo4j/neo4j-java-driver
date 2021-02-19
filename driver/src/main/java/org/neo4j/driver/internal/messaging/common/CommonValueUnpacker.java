/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.driver.internal.messaging.common;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.messaging.ValueUnpacker;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.packstream.PackStream;
import org.neo4j.driver.internal.packstream.PackType;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;

import static java.time.ZoneOffset.UTC;
import static org.neo4j.driver.Values.isoDuration;
import static org.neo4j.driver.Values.point;
import static org.neo4j.driver.Values.value;

public class CommonValueUnpacker implements ValueUnpacker
{

    public static final byte DATE = 'D';
    public static final int DATE_STRUCT_SIZE = 1;

    public static final byte TIME = 'T';
    public static final int TIME_STRUCT_SIZE = 2;

    public static final byte LOCAL_TIME = 't';
    public static final int LOCAL_TIME_STRUCT_SIZE = 1;

    public static final byte LOCAL_DATE_TIME = 'd';
    public static final int LOCAL_DATE_TIME_STRUCT_SIZE = 2;

    public static final byte DATE_TIME_WITH_ZONE_OFFSET = 'F';
    public static final byte DATE_TIME_WITH_ZONE_ID = 'f';
    public static final int DATE_TIME_STRUCT_SIZE = 3;

    public static final byte DURATION = 'E';
    public static final int DURATION_TIME_STRUCT_SIZE = 4;

    public static final byte POINT_2D_STRUCT_TYPE = 'X';
    public static final int POINT_2D_STRUCT_SIZE = 3;

    public static final byte POINT_3D_STRUCT_TYPE = 'Y';
    public static final int POINT_3D_STRUCT_SIZE = 4;

    public static final byte NODE = 'N';
    public static final byte RELATIONSHIP = 'R';
    public static final byte UNBOUND_RELATIONSHIP = 'r';
    public static final byte PATH = 'P';

    public static final int NODE_FIELDS = 3;

    protected final PackStream.Unpacker unpacker;

    public CommonValueUnpacker( PackInput input )
    {
        this.unpacker = new PackStream.Unpacker( input );
    }

    @Override
    public long unpackStructHeader() throws IOException
    {
        return unpacker.unpackStructHeader();
    }

    @Override
    public int unpackStructSignature() throws IOException
    {
        return unpacker.unpackStructSignature();
    }

    @Override
    public Map<String,Value> unpackMap() throws IOException
    {
        int size = (int) unpacker.unpackMapHeader();
        if ( size == 0 )
        {
            return Collections.emptyMap();
        }
        Map<String,Value> map = Iterables.newHashMapWithSize( size );
        for ( int i = 0; i < size; i++ )
        {
            String key = unpacker.unpackString();
            map.put( key, unpack() );
        }
        return map;
    }

    @Override
    public Value[] unpackArray() throws IOException
    {
        int size = (int) unpacker.unpackListHeader();
        Value[] values = new Value[size];
        for ( int i = 0; i < size; i++ )
        {
            values[i] = unpack();
        }
        return values;
    }

    private Value unpack() throws IOException
    {
        PackType type = unpacker.peekNextType();
        switch ( type )
        {
        case NULL:
            return value( unpacker.unpackNull() );
        case BOOLEAN:
            return value( unpacker.unpackBoolean() );
        case INTEGER:
            return value( unpacker.unpackLong() );
        case FLOAT:
            return value( unpacker.unpackDouble() );
        case BYTES:
            return value( unpacker.unpackBytes() );
        case STRING:
            return value( unpacker.unpackString() );
        case MAP:
        {
            return new MapValue( unpackMap() );
        }
        case LIST:
        {
            int size = (int) unpacker.unpackListHeader();
            Value[] vals = new Value[size];
            for ( int j = 0; j < size; j++ )
            {
                vals[j] = unpack();
            }
            return new ListValue( vals );
        }
        case STRUCT:
        {
            long size = unpacker.unpackStructHeader();
            byte structType = unpacker.unpackStructSignature();
            return unpackStruct( size, structType );
        }
        }
        throw new IOException( "Unknown value type: " + type );
    }

    protected Value unpackStruct( long size, byte type ) throws IOException
    {
        switch ( type )
        {
        case DATE:
            ensureCorrectStructSize( TypeConstructor.DATE, DATE_STRUCT_SIZE, size );
            return unpackDate();
        case TIME:
            ensureCorrectStructSize( TypeConstructor.TIME, TIME_STRUCT_SIZE, size );
            return unpackTime();
        case LOCAL_TIME:
            ensureCorrectStructSize( TypeConstructor.LOCAL_TIME, LOCAL_TIME_STRUCT_SIZE, size );
            return unpackLocalTime();
        case LOCAL_DATE_TIME:
            ensureCorrectStructSize( TypeConstructor.LOCAL_DATE_TIME, LOCAL_DATE_TIME_STRUCT_SIZE, size );
            return unpackLocalDateTime();
        case DATE_TIME_WITH_ZONE_OFFSET:
            ensureCorrectStructSize( TypeConstructor.DATE_TIME, DATE_TIME_STRUCT_SIZE, size );
            return unpackDateTimeWithZoneOffset();
        case DATE_TIME_WITH_ZONE_ID:
            ensureCorrectStructSize( TypeConstructor.DATE_TIME, DATE_TIME_STRUCT_SIZE, size );
            return unpackDateTimeWithZoneId();
        case DURATION:
            ensureCorrectStructSize( TypeConstructor.DURATION, DURATION_TIME_STRUCT_SIZE, size );
            return unpackDuration();
        case POINT_2D_STRUCT_TYPE:
            ensureCorrectStructSize( TypeConstructor.POINT, POINT_2D_STRUCT_SIZE, size );
            return unpackPoint2D();
        case POINT_3D_STRUCT_TYPE:
            ensureCorrectStructSize( TypeConstructor.POINT, POINT_3D_STRUCT_SIZE, size );
            return unpackPoint3D();
        case NODE:
            ensureCorrectStructSize( TypeConstructor.NODE, NODE_FIELDS, size );
            InternalNode adapted = unpackNode();
            return new NodeValue( adapted );
        case RELATIONSHIP:
            ensureCorrectStructSize( TypeConstructor.RELATIONSHIP, 5, size );
            return unpackRelationship();
        case PATH:
            ensureCorrectStructSize( TypeConstructor.PATH, 3, size );
            return unpackPath();
        default:
            throw new IOException( "Unknown struct type: " + type );
        }
    }

    private Value unpackRelationship() throws IOException
    {
        long urn = unpacker.unpackLong();
        long startUrn = unpacker.unpackLong();
        long endUrn = unpacker.unpackLong();
        String relType = unpacker.unpackString();
        Map<String,Value> props = unpackMap();

        InternalRelationship adapted = new InternalRelationship( urn, startUrn, endUrn, relType, props );
        return new RelationshipValue( adapted );
    }

    private InternalNode unpackNode() throws IOException
    {
        long urn = unpacker.unpackLong();

        int numLabels = (int) unpacker.unpackListHeader();
        List<String> labels = new ArrayList<>( numLabels );
        for ( int i = 0; i < numLabels; i++ )
        {
            labels.add( unpacker.unpackString() );
        }
        int numProps = (int) unpacker.unpackMapHeader();
        Map<String,Value> props = Iterables.newHashMapWithSize( numProps );
        for ( int j = 0; j < numProps; j++ )
        {
            String key = unpacker.unpackString();
            props.put( key, unpack() );
        }

        return new InternalNode( urn, labels, props );
    }

    private Value unpackPath() throws IOException
    {
        // List of unique nodes
        Node[] uniqNodes = new Node[(int) unpacker.unpackListHeader()];
        for ( int i = 0; i < uniqNodes.length; i++ )
        {
            ensureCorrectStructSize( TypeConstructor.NODE, NODE_FIELDS, unpacker.unpackStructHeader() );
            ensureCorrectStructSignature( "NODE", NODE, unpacker.unpackStructSignature() );
            uniqNodes[i] = unpackNode();
        }

        // List of unique relationships, without start/end information
        InternalRelationship[] uniqRels = new InternalRelationship[(int) unpacker.unpackListHeader()];
        for ( int i = 0; i < uniqRels.length; i++ )
        {
            ensureCorrectStructSize( TypeConstructor.RELATIONSHIP, 3, unpacker.unpackStructHeader() );
            ensureCorrectStructSignature( "UNBOUND_RELATIONSHIP", UNBOUND_RELATIONSHIP, unpacker.unpackStructSignature() );
            long id = unpacker.unpackLong();
            String relType = unpacker.unpackString();
            Map<String,Value> props = unpackMap();
            uniqRels[i] = new InternalRelationship( id, -1, -1, relType, props );
        }

        // Path sequence
        int length = (int) unpacker.unpackListHeader();

        // Knowing the sequence length, we can create the arrays that will represent the nodes, rels and segments in their "path order"
        Path.Segment[] segments = new Path.Segment[length / 2];
        Node[] nodes = new Node[segments.length + 1];
        Relationship[] rels = new Relationship[segments.length];

        Node prevNode = uniqNodes[0], nextNode; // Start node is always 0, and isn't encoded in the sequence
        nodes[0] = prevNode;
        InternalRelationship rel;
        for ( int i = 0; i < segments.length; i++ )
        {
            int relIdx = (int) unpacker.unpackLong();
            nextNode = uniqNodes[(int) unpacker.unpackLong()];
            // Negative rel index means this rel was traversed "inversed" from its direction
            if ( relIdx < 0 )
            {
                rel = uniqRels[(-relIdx) - 1]; // -1 because rel idx are 1-indexed
                rel.setStartAndEnd( nextNode.id(), prevNode.id() );
            }
            else
            {
                rel = uniqRels[relIdx - 1];
                rel.setStartAndEnd( prevNode.id(), nextNode.id() );
            }

            nodes[i + 1] = nextNode;
            rels[i] = rel;
            segments[i] = new InternalPath.SelfContainedSegment( prevNode, rel, nextNode );
            prevNode = nextNode;
        }
        return new PathValue( new InternalPath( Arrays.asList( segments ), Arrays.asList( nodes ), Arrays.asList( rels ) ) );
    }

    protected final void ensureCorrectStructSize( TypeConstructor typeConstructor, int expected, long actual )
    {
        if ( expected != actual )
        {
            String structName = typeConstructor.toString();
            throw new ClientException( String.format(
                    "Invalid message received, serialized %s structures should have %d fields, "
                    + "received %s structure has %d fields.", structName, expected, structName, actual ) );
        }
    }

    private void ensureCorrectStructSignature( String structName, byte expected, byte actual )
    {
        if ( expected != actual )
        {
            throw new ClientException( String.format(
                    "Invalid message received, expected a `%s`, signature 0x%s. Received signature was 0x%s.",
                    structName, Integer.toHexString( expected ), Integer.toHexString( actual ) ) );
        }
    }

    private Value unpackDate() throws IOException
    {
        long epochDay = unpacker.unpackLong();
        return value( LocalDate.ofEpochDay( epochDay ) );
    }

    private Value unpackTime() throws IOException
    {
        long nanoOfDayLocal = unpacker.unpackLong();
        int offsetSeconds = Math.toIntExact( unpacker.unpackLong() );

        LocalTime localTime = LocalTime.ofNanoOfDay( nanoOfDayLocal );
        ZoneOffset offset = ZoneOffset.ofTotalSeconds( offsetSeconds );
        return value( OffsetTime.of( localTime, offset ) );
    }

    private Value unpackLocalTime() throws IOException
    {
        long nanoOfDayLocal = unpacker.unpackLong();
        return value( LocalTime.ofNanoOfDay( nanoOfDayLocal ) );
    }

    private Value unpackLocalDateTime() throws IOException
    {
        long epochSecondUtc = unpacker.unpackLong();
        int nano = Math.toIntExact( unpacker.unpackLong() );
        return value( LocalDateTime.ofEpochSecond( epochSecondUtc, nano, UTC ) );
    }

    private Value unpackDateTimeWithZoneOffset() throws IOException
    {
        long epochSecondLocal = unpacker.unpackLong();
        int nano = Math.toIntExact( unpacker.unpackLong() );
        int offsetSeconds = Math.toIntExact( unpacker.unpackLong() );
        return value( newZonedDateTime( epochSecondLocal, nano, ZoneOffset.ofTotalSeconds( offsetSeconds ) ) );
    }

    private Value unpackDateTimeWithZoneId() throws IOException
    {
        long epochSecondLocal = unpacker.unpackLong();
        int nano = Math.toIntExact( unpacker.unpackLong() );
        String zoneIdString = unpacker.unpackString();
        return value( newZonedDateTime( epochSecondLocal, nano, ZoneId.of( zoneIdString ) ) );
    }

    private Value unpackDuration() throws IOException
    {
        long months = unpacker.unpackLong();
        long days = unpacker.unpackLong();
        long seconds = unpacker.unpackLong();
        int nanoseconds = Math.toIntExact( unpacker.unpackLong() );
        return isoDuration( months, days, seconds, nanoseconds );
    }

    private Value unpackPoint2D() throws IOException
    {
        int srid = Math.toIntExact( unpacker.unpackLong() );
        double x = unpacker.unpackDouble();
        double y = unpacker.unpackDouble();
        return point( srid, x, y );
    }

    private Value unpackPoint3D() throws IOException
    {
        int srid = Math.toIntExact( unpacker.unpackLong() );
        double x = unpacker.unpackDouble();
        double y = unpacker.unpackDouble();
        double z = unpacker.unpackDouble();
        return point( srid, x, y, z );
    }

    private static ZonedDateTime newZonedDateTime( long epochSecondLocal, long nano, ZoneId zoneId )
    {
        Instant instant = Instant.ofEpochSecond( epochSecondLocal, nano );
        LocalDateTime localDateTime = LocalDateTime.ofInstant( instant, UTC );
        return ZonedDateTime.of( localDateTime, zoneId );
    }
}


