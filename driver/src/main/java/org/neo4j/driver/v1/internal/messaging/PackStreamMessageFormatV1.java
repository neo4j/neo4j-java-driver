/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.v1.internal.messaging;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Entity;
import org.neo4j.driver.v1.Identity;
import org.neo4j.driver.v1.Node;
import org.neo4j.driver.v1.Path;
import org.neo4j.driver.v1.Relationship;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.internal.Identities;
import org.neo4j.driver.v1.internal.SimpleNode;
import org.neo4j.driver.v1.internal.SimplePath;
import org.neo4j.driver.v1.internal.SimpleRelationship;
import org.neo4j.driver.v1.internal.connector.socket.ChunkedInput;
import org.neo4j.driver.v1.internal.connector.socket.ChunkedOutput;
import org.neo4j.driver.v1.internal.packstream.BufferedChannelOutput;
import org.neo4j.driver.v1.internal.packstream.PackInput;
import org.neo4j.driver.v1.internal.packstream.PackOutput;
import org.neo4j.driver.v1.internal.packstream.PackStream;
import org.neo4j.driver.v1.internal.packstream.PackType;
import org.neo4j.driver.v1.internal.util.Iterables;
import org.neo4j.driver.v1.internal.value.ListValue;
import org.neo4j.driver.v1.internal.value.MapValue;
import org.neo4j.driver.v1.internal.value.NodeValue;
import org.neo4j.driver.v1.internal.value.PathValue;
import org.neo4j.driver.v1.internal.value.RelationshipValue;

import static org.neo4j.driver.v1.Values.value;

public class PackStreamMessageFormatV1 implements MessageFormat
{
    public final static byte MSG_INIT = 0x01;
    public final static byte MSG_ACK_FAILURE = 0x0F;
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

    public static final int VERSION = 1;

    public static final int NODE_FIELDS = 3;

    private static final Map<String,Value> EMPTY_STRING_VALUE_MAP = new HashMap<>( 0 );

    @Override
    public MessageFormat.Writer newWriter( WritableByteChannel ch )
    {
        ChunkedOutput output = new ChunkedOutput( ch );
        return new Writer( output, output.messageBoundaryHook() );
    }

    @Override
    public MessageFormat.Reader newReader( ReadableByteChannel ch )
    {
        ChunkedInput input = new ChunkedInput( ch );
        return new Reader( input, input.messageBoundaryHook() );
    }

    @Override
    public int version()
    {
        return VERSION;
    }

    public static class Writer implements MessageFormat.Writer, MessageHandler
    {
        private final PackStream.Packer packer;
        private final Runnable onMessageComplete;

        public Writer()
        {
            this( new BufferedChannelOutput( 8192 ), new NoOpRunnable() );
        }

        /**
         * @param output interface to write messages to
         * @param onMessageComplete invoked for each message, after it's done writing to the output
         */
        public Writer( PackOutput output, Runnable onMessageComplete )
        {
            this.onMessageComplete = onMessageComplete;
            packer = new PackStream.Packer( output );
        }

        @Override
        public void handleInitMessage( String clientNameAndVersion ) throws IOException
        {
            packer.packStructHeader( 1, MSG_INIT );
            packer.pack( clientNameAndVersion );
            onMessageComplete.run();
        }

        @Override
        public void handleRunMessage( String statement, Map<String,Value> parameters ) throws IOException
        {
            packer.packStructHeader( 2, MSG_RUN );
            packer.pack( statement );
            packRawMap( parameters );
            onMessageComplete.run();
        }

        @Override
        public void handlePullAllMessage() throws IOException
        {
            packer.packStructHeader( 0, MSG_PULL_ALL );
            onMessageComplete.run();
        }

        @Override
        public void handleDiscardAllMessage() throws IOException
        {
            packer.packStructHeader( 0, MSG_DISCARD_ALL );
            onMessageComplete.run();
        }

        @Override
        public void handleAckFailureMessage() throws IOException
        {
            packer.packStructHeader( 0, MSG_ACK_FAILURE );
            onMessageComplete.run();
        }

        @Override
        public void handleSuccessMessage( Map<String,Value> meta ) throws IOException
        {
            packer.packStructHeader( 1, MSG_SUCCESS );
            packRawMap( meta );
            onMessageComplete.run();
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
            onMessageComplete.run();
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
            onMessageComplete.run();
        }

        @Override
        public void handleIgnoredMessage() throws IOException
        {
            packer.packStructHeader( 0, MSG_IGNORED );
            onMessageComplete.run();
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
            if ( value == null )
            {
                packer.packNull();
            }
            else if ( value.isBoolean() )
            {
                packer.pack( value.javaBoolean() );
            }
            else if ( value.isInteger() )
            {
                packer.pack( value.javaLong() );
            }
            else if ( value.isFloat() )
            {
                packer.pack( value.javaDouble() );
            }
            else if ( value.isString() )
            {
                packer.pack( value.javaString() );
            }
            else if ( value.isMap() )
            {
                packer.packMapHeader( (int) value.size() );
                for ( String s : value.keys() )
                {
                    packer.pack( s );
                    packValue( value.get( s ) );
                }
            }
            else if ( value.isList() )
            {
                packer.packListHeader( (int) value.size() );
                for ( Value item : value )
                {
                    packValue( item );
                }
            }
            else if ( value.isNode() )
            {
                Node node = value.asNode();
                packNode( node );
            }
            else if ( value.isRelationship() )
            {
                Relationship rel = value.asRelationship();
                packer.packStructHeader( 5, RELATIONSHIP );
                packer.pack( rel.identity().asLong() );
                packer.pack( rel.start().asLong() );
                packer.pack( rel.end().asLong() );

                packer.pack( rel.type() );

                packProperties( rel );
            }
            else if ( value.isPath() )
            {
                Path path = value.asPath();
                packer.packStructHeader( 3, PATH );

                // Uniq nodes
                Map<Node, Integer> nodeIdx = new LinkedHashMap<>();
                for ( Node node : path.nodes() )
                {
                    if( !nodeIdx.containsKey( node ) )
                    {
                        nodeIdx.put( node, nodeIdx.size() );
                    }
                }
                packer.packListHeader( nodeIdx.size() );
                for ( Node node : nodeIdx.keySet() )
                {
                    packNode( node );
                }

                // Uniq rels
                Map<Relationship, Integer> relIdx = new LinkedHashMap<>();
                for ( Relationship rel : path.relationships() )
                {
                    if( !relIdx.containsKey( rel ) )
                    {
                        relIdx.put( rel, relIdx.size() + 1 );
                    }
                }
                packer.packListHeader( relIdx.size() );
                for ( Relationship rel : relIdx.keySet() )
                {
                    packer.packStructHeader( 3, UNBOUND_RELATIONSHIP );
                    packer.pack( rel.identity().asLong() );
                    packer.pack( rel.type() );
                    packProperties( rel );
                }

                // Sequence
                packer.packListHeader( (int) path.length() * 2 );
                for ( Path.Segment seg : path )
                {
                    Relationship rel = seg.relationship();
                    packer.pack( rel.end().equals( seg.end() ) ? relIdx.get( rel ) : -relIdx.get( rel ) );
                    packer.pack( nodeIdx.get( seg.end() ) );
                }
            }
            else
            {
                throw new UnsupportedOperationException( "Unknown type: " + value );
            }
        }

        @Override
        public Writer flush() throws IOException
        {
            packer.flush();
            return this;
        }

        @Override
        public Writer write( Message msg ) throws IOException
        {
            msg.dispatch( this );
            return this;
        }

        @Override
        public Writer reset( WritableByteChannel channel )
        {
            packer.reset( channel );
            return this;
        }

        private void packNode( Node node ) throws IOException
        {
            packer.packStructHeader( NODE_FIELDS, NODE );
            packer.pack( node.identity().asLong() );

            Iterable<String> labels = node.labels();
            packer.packListHeader( Iterables.count( labels ) );
            for ( String label : labels )
            {
                packer.pack( label );
            }

            packProperties( node );
        }

        private void packProperties( Entity entity ) throws IOException
        {
            Iterable<String> keys = entity.propertyKeys();
            packer.packMapHeader( entity.propertyCount() );
            for ( String propKey : keys )
            {
                packer.pack( propKey );
                packValue( entity.property( propKey ) );
            }
        }
    }

    public static class Reader implements MessageFormat.Reader
    {
        private final PackStream.Unpacker unpacker;
        private final Runnable onMessageComplete;

        public Reader( PackInput input, Runnable onMessageComplete )
        {
            unpacker = new PackStream.Unpacker( input );
            this.onMessageComplete = onMessageComplete;
        }

        @Override
        public boolean hasNext() throws IOException
        {
            return unpacker.hasNext();
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
            default:
                throw new IOException( "Unknown message type: " + type );
            }
        }

        private void unpackInitMessage( MessageHandler handler ) throws IOException
        {
            handler.handleInitMessage( unpacker.unpackString() );
            onMessageComplete.run();
        }

        private void unpackIgnoredMessage( MessageHandler output ) throws IOException
        {
            output.handleIgnoredMessage();
            onMessageComplete.run();
        }

        private void unpackFailureMessage( MessageHandler output ) throws IOException
        {
            Map<String,Value> params = unpackMap();
            String code = params.get( "code" ).javaString();
            String message = params.get( "message" ).javaString();
            output.handleFailureMessage( code, message );
            onMessageComplete.run();
        }

        private void unpackRunMessage( MessageHandler output ) throws IOException
        {
            String statement = unpacker.unpackString();
            Map<String,Value> params = unpackMap();
            output.handleRunMessage( statement, params );
            onMessageComplete.run();
        }

        private void unpackDiscardAllMessage( MessageHandler output ) throws IOException
        {
            output.handleDiscardAllMessage();
            onMessageComplete.run();
        }

        private void unpackPullAllMessage( MessageHandler output ) throws IOException
        {
            output.handlePullAllMessage();
            onMessageComplete.run();
        }

        private void unpackSuccessMessage( MessageHandler output ) throws IOException
        {
            Map<String,Value> map = unpackMap();
            output.handleSuccessMessage( map );
            onMessageComplete.run();
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
            onMessageComplete.run();
        }

        private Value unpackValue() throws IOException
        {
            PackType type = unpacker.peekNextType();
            switch ( type )
            {
            case BYTES:
                break;
            case NULL:
                return value( unpacker.unpackNull() );
            case BOOLEAN:
                return value( unpacker.unpackBoolean() );
            case INTEGER:
                return value( unpacker.unpackLong() );
            case FLOAT:
                return value( unpacker.unpackDouble() );
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
                switch ( unpacker.unpackStructSignature() )
                {
                case NODE:
                    ensureCorrectStructSize( "NODE", NODE_FIELDS, size );
                    return new NodeValue( unpackNode() );
                case RELATIONSHIP:
                    ensureCorrectStructSize( "RELATIONSHIP", 5, size );
                    return unpackRelationship();
                case PATH:
                    ensureCorrectStructSize( "PATH", 3, size );
                    return unpackPath();
                }
            }
            }
            throw new IOException( "Unknown value type: " + type );
        }

        private Value unpackRelationship() throws IOException
        {
            long urn = unpacker.unpackLong();
            long startUrn = unpacker.unpackLong();
            long endUrn = unpacker.unpackLong();
            String relType = unpacker.unpackString();
            Map<String,Value> props = unpackMap();

            return new RelationshipValue( new SimpleRelationship( urn, startUrn, endUrn, relType, props ) );
        }

        private SimpleNode unpackNode() throws IOException
        {
            long urn = unpacker.unpackLong();

            int numLabels = (int) unpacker.unpackListHeader();
            List<String> labels = new ArrayList<>( numLabels );
            for ( int i = 0; i < numLabels; i++ )
            {
                labels.add( unpacker.unpackString() );
            }
            int numProps = (int) unpacker.unpackMapHeader();
            Map<String,Value> props = new HashMap<>();
            for ( int j = 0; j < numProps; j++ )
            {
                String key = unpacker.unpackString();
                props.put( key, unpackValue() );
            }

            return new SimpleNode( urn, labels, props );
        }

        private Value unpackPath() throws IOException
        {
            // List of unique nodes
            Node[] uniqNodes = new Node[(int) unpacker.unpackListHeader()];
            for ( int i = 0; i < uniqNodes.length; i++ )
            {
                ensureCorrectStructSize( "NODE", NODE_FIELDS, unpacker.unpackStructHeader() );
                ensureCorrectStructSignature( "NODE", NODE, unpacker.unpackStructSignature() );
                uniqNodes[i] = unpackNode();
            }

            // List of unique relationships, without start/end information
            SimpleRelationship[] uniqRels = new SimpleRelationship[(int) unpacker.unpackListHeader()];
            for ( int i = 0; i < uniqRels.length; i++ )
            {
                ensureCorrectStructSize( "RELATIONSHIP", 3, unpacker.unpackStructHeader() );
                ensureCorrectStructSignature( "UNBOUND_RELATIONSHIP", UNBOUND_RELATIONSHIP, unpacker.unpackStructSignature() );
                Identity urn = Identities.identity( unpacker.unpackLong() );
                String relType = unpacker.unpackString();
                Map<String,Value> props = unpackMap();
                uniqRels[i] = new SimpleRelationship( urn, null, null, relType, props );
            }

            // Path sequence
            int length = (int) unpacker.unpackListHeader();

            // Knowing the sequence length, we can create the arrays that will represent the nodes, rels and segments in their "path order"
            Path.Segment[] segments = new Path.Segment[length / 2];
            Node[] nodes = new Node[segments.length + 1];
            Relationship[] rels = new Relationship[segments.length];

            Node prevNode = uniqNodes[0], nextNode; // Start node is always 0, and isn't encoded in the sequence
            nodes[0] = prevNode;
            SimpleRelationship rel;
            for ( int i = 0; i < segments.length; i++ )
            {
                int relIdx = (int) unpacker.unpackLong();
                nextNode = uniqNodes[(int) unpacker.unpackLong()];
                // Negative rel index means this rel was traversed "inversed" from its direction
                if( relIdx < 0 )
                {
                    rel = uniqRels[(-relIdx) - 1]; // -1 because rel idx are 1-indexed
                    rel.setStartAndEnd( nextNode.identity(), prevNode.identity() );
                }
                else
                {
                    rel = uniqRels[relIdx - 1];
                    rel.setStartAndEnd( prevNode.identity(), nextNode.identity() );
                }

                nodes[i+1] = nextNode;
                rels[i] = rel;
                segments[i] = new SimplePath.SelfContainedSegment( prevNode, rel, nextNode );
                prevNode = nextNode;
            }
            return new PathValue( new SimplePath( Arrays.asList( segments ), Arrays.asList( nodes ), Arrays.asList( rels ) ) );
        }

        private void ensureCorrectStructSize( String structName, int expected, long actual )
        {
            if ( expected != actual )
            {
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
                        "Invalid message received, expected a `%s`, signature 0x%s. Recieved signature was 0x%s.",
                        structName, Integer.toHexString( expected ), Integer.toHexString( actual ) ) );
            }
        }

        private Map<String,Value> unpackMap() throws IOException
        {
            int size = (int) unpacker.unpackMapHeader();
            if ( size == 0 )
            {
                return EMPTY_STRING_VALUE_MAP;
            }
            Map<String,Value> map = new HashMap<>( size );
            for ( int i = 0; i < size; i++ )
            {
                String key = unpacker.unpackString();
                map.put( key, unpackValue() );
            }
            return map;
        }
    }

    public static class NoOpRunnable implements Runnable
    {
        @Override
        public void run()
        {
            // no-op
        }
    }

}
