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
package org.neo4j.driver.internal.messaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.packstream.ByteArrayIncompatiblePacker;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.internal.packstream.PackStream;
import org.neo4j.driver.internal.packstream.PackType;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.internal.value.InternalValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;

import static org.neo4j.driver.v1.Values.value;

public class PackStreamMessageFormatV1 implements MessageFormat
{
    public final static byte MSG_INIT = 0x01;
    public final static byte MSG_ACK_FAILURE = 0x0E;
    public final static byte MSG_RESET = 0x0F;
    public final static byte MSG_RUN = 0x10;
    public final static byte MSG_DISCARD_ALL = 0x2F;
    public final static byte MSG_PULL_ALL = 0x3F;

    public final static byte MSG_RECORD = 0x71;
    public final static byte MSG_SUCCESS = 0x70;
    public final static byte MSG_IGNORED = 0x7E;
    public final static byte MSG_FAILURE = 0x7F;

    public static final byte NODE = 'N';
    public static final byte RELATIONSHIP = 'R';
    public static final byte UNBOUND_RELATIONSHIP = 'r';
    public static final byte PATH = 'P';

    public static final int NODE_FIELDS = 3;

    @Override
    public MessageFormat.Writer newWriter( PackOutput output, boolean byteArraySupportEnabled )
    {
        return new WriterV1( output, byteArraySupportEnabled );
    }

    @Override
    public MessageFormat.Reader newReader( PackInput input )
    {
        return new ReaderV1( input );
    }

    static class WriterV1 implements MessageFormat.Writer, MessageHandler
    {
        final PackStream.Packer packer;

        /**
         * @param output interface to write messages to
         * @param byteArraySupportEnabled specify if support to pack/write byte array to server
         */
        WriterV1( PackOutput output, boolean byteArraySupportEnabled )
        {
            if( byteArraySupportEnabled )
            {
                packer = new PackStream.Packer( output );
            }
            else
            {
                packer = new ByteArrayIncompatiblePacker( output );
            }
        }

        @Override
        public void handleInitMessage( String clientNameAndVersion, Map<String,Value> authToken ) throws IOException
        {
            packer.packStructHeader( 2, MSG_INIT );
            packer.pack( clientNameAndVersion );
            packRawMap( authToken );
        }

        @Override
        public void handleRunMessage( String statement, Map<String,Value> parameters ) throws IOException
        {
            packer.packStructHeader( 2, MSG_RUN );
            packer.pack( statement );
            packRawMap( parameters );
        }

        @Override
        public void handlePullAllMessage() throws IOException
        {
            packer.packStructHeader( 0, MSG_PULL_ALL );
        }

        @Override
        public void handleDiscardAllMessage() throws IOException
        {
            packer.packStructHeader( 0, MSG_DISCARD_ALL );
        }

        @Override
        public void handleResetMessage() throws IOException
        {
            packer.packStructHeader( 0, MSG_RESET );
        }

        @Override
        public void handleAckFailureMessage() throws IOException
        {
            packer.packStructHeader( 0, MSG_ACK_FAILURE );
        }

        @Override
        public void handleSuccessMessage( Map<String,Value> meta ) throws IOException
        {
            packer.packStructHeader( 1, MSG_SUCCESS );
            packRawMap( meta );
        }

        @Override
        public void handleRecordMessage( Value[] fields ) throws IOException
        {
            packer.packStructHeader( 1, MSG_RECORD );
            packer.packListHeader( fields.length );
            for ( Value field : fields )
            {
                packValue( field );
            }
        }

        @Override
        public void handleFailureMessage( String code, String message ) throws IOException
        {
            packer.packStructHeader( 1, MSG_FAILURE );
            packer.packMapHeader( 2 );

            packer.pack( "code" );
            packValue( value( code ) );

            packer.pack( "message" );
            packValue( value( message ) );
        }

        @Override
        public void handleIgnoredMessage() throws IOException
        {
            packer.packStructHeader( 0, MSG_IGNORED );
        }

        private void packRawMap( Map<String,Value> map ) throws IOException
        {
            if ( map == null || map.size() == 0 )
            {
                packer.packMapHeader( 0 );
                return;
            }
            packer.packMapHeader( map.size() );
            for ( Map.Entry<String,Value> entry : map.entrySet() )
            {
                packer.pack( entry.getKey() );
                packValue( entry.getValue() );
            }
        }

        private void packValue( Value value ) throws IOException
        {
            if ( value instanceof InternalValue )
            {
                packInternalValue( ((InternalValue) value) );
            }
            else
            {
                throw new IllegalArgumentException( "Unable to pack: " + value );
            }
        }

        void packInternalValue( InternalValue value ) throws IOException
        {
            switch ( value.typeConstructor() )
            {
            case NULL:
                packer.packNull();
                break;

            case BYTES:
                packer.pack( value.asByteArray() );
                break;

            case STRING:
                packer.pack( value.asString() );
                break;

            case BOOLEAN:
                packer.pack( value.asBoolean() );
                break;

            case INTEGER:
                packer.pack( value.asLong() );
                break;

            case FLOAT:
                packer.pack( value.asDouble() );
                break;

            case MAP:
                packer.packMapHeader( value.size() );
                for ( String s : value.keys() )
                {
                    packer.pack( s );
                    packValue( value.get( s ) );
                }
                break;

            case LIST:
                packer.packListHeader( value.size() );
                for ( Value item : value.values() )
                {
                    packValue( item );
                }
                break;

            default:
                throw new IOException( "Unknown type: " + value.type().name() );
            }
        }

        @Override
        public void write( Message msg ) throws IOException
        {
            msg.dispatch( this );
        }
    }

    static class ReaderV1 implements MessageFormat.Reader
    {
        final PackStream.Unpacker unpacker;

        ReaderV1( PackInput input )
        {
            unpacker = new PackStream.Unpacker( input );
        }

        /**
         * Parse a single message into the given consumer.
         */
        @Override
        public void read( MessageHandler handler ) throws IOException
        {
            unpacker.unpackStructHeader();
            int type = unpacker.unpackStructSignature();
            switch ( type )
            {
            case MSG_RUN:
                unpackRunMessage( handler );
                break;
            case MSG_DISCARD_ALL:
                unpackDiscardAllMessage( handler );
                break;
            case MSG_PULL_ALL:
                unpackPullAllMessage( handler );
                break;
            case MSG_RECORD:
                unpackRecordMessage(handler);
                break;
            case MSG_SUCCESS:
                unpackSuccessMessage( handler );
                break;
            case MSG_FAILURE:
                unpackFailureMessage( handler );
                break;
            case MSG_IGNORED:
                unpackIgnoredMessage( handler );
                break;
            case MSG_INIT:
                unpackInitMessage( handler );
                break;
            case MSG_RESET:
                unpackResetMessage( handler );
                break;
            default:
                throw new IOException( "Unknown message type: " + type );
            }
        }

        private void unpackResetMessage( MessageHandler handler ) throws IOException
        {
            handler.handleResetMessage();
        }

        private void unpackInitMessage( MessageHandler handler ) throws IOException
        {
            handler.handleInitMessage( unpacker.unpackString(), unpackMap() );
        }

        private void unpackIgnoredMessage( MessageHandler output ) throws IOException
        {
            output.handleIgnoredMessage();
        }

        private void unpackFailureMessage( MessageHandler output ) throws IOException
        {
            Map<String,Value> params = unpackMap();
            String code = params.get( "code" ).asString();
            String message = params.get( "message" ).asString();
            output.handleFailureMessage( code, message );
        }

        private void unpackRunMessage( MessageHandler output ) throws IOException
        {
            String statement = unpacker.unpackString();
            Map<String,Value> params = unpackMap();
            output.handleRunMessage( statement, params );
        }

        private void unpackDiscardAllMessage( MessageHandler output ) throws IOException
        {
            output.handleDiscardAllMessage();
        }

        private void unpackPullAllMessage( MessageHandler output ) throws IOException
        {
            output.handlePullAllMessage();
        }

        private void unpackSuccessMessage( MessageHandler output ) throws IOException
        {
            Map<String,Value> map = unpackMap();
            output.handleSuccessMessage( map );
        }

        private void unpackRecordMessage(MessageHandler output) throws IOException
        {
            int fieldCount = (int) unpacker.unpackListHeader();
            Value[] fields = new Value[fieldCount];
            for ( int i = 0; i < fieldCount; i++ )
            {
                fields[i] = unpackValue();
            }
            output.handleRecordMessage( fields );
        }

        private Value unpackValue() throws IOException
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
                    vals[j] = unpackValue();
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

        Value unpackStruct( long size, byte type ) throws IOException
        {
            switch ( type )
            {
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
                props.put( key, unpackValue() );
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
                if( relIdx < 0 )
                {
                    rel = uniqRels[(-relIdx) - 1]; // -1 because rel idx are 1-indexed
                    rel.setStartAndEnd( nextNode.id(), prevNode.id() );
                }
                else
                {
                    rel = uniqRels[relIdx - 1];
                    rel.setStartAndEnd( prevNode.id(), nextNode.id() );
                }

                nodes[i+1] = nextNode;
                rels[i] = rel;
                segments[i] = new InternalPath.SelfContainedSegment( prevNode, rel, nextNode );
                prevNode = nextNode;
            }
            return new PathValue( new InternalPath( Arrays.asList( segments ), Arrays.asList( nodes ), Arrays.asList( rels ) ) );
        }

        void ensureCorrectStructSize( TypeConstructor typeConstructor, int expected, long actual )
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

        private Map<String,Value> unpackMap() throws IOException
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
                map.put( key, unpackValue() );
            }
            return map;
        }
    }
}
