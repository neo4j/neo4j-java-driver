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
package org.neo4j.driver.internal.messaging;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Entity;
import org.neo4j.driver.Node;
import org.neo4j.driver.Path;
import org.neo4j.driver.Relationship;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.SimpleNode;
import org.neo4j.driver.internal.SimplePath;
import org.neo4j.driver.internal.SimpleRelationship;
import org.neo4j.driver.internal.connector.socket.ChunkedInput;
import org.neo4j.driver.internal.connector.socket.ChunkedOutput;
import org.neo4j.driver.internal.packstream.BufferedChannelOutput;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.internal.packstream.PackStream;
import org.neo4j.driver.internal.packstream.PackType;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.RelationshipValue;

import static org.neo4j.driver.Values.value;

public class PackStreamMessageFormatV1 implements MessageFormat
{
    public final static byte MSG_INITIALIZE = 0x01;
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
    public static final byte PATH = 'P';

    public static final int VERSION = 1;

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
        public void handleInitializeMessage( String clientNameAndVersion ) throws IOException
        {
            packer.packStructHeader( 1, MSG_INITIALIZE );
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
            else if ( value.isText() )
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
                packer.packStructHeader( 3, NODE );
                packer.pack( node.identity().toString() );

                Iterable<String> labels = node.labels();
                packer.packListHeader( Iterables.count( labels ) );
                for ( String label : labels )
                {
                    packer.pack( label );
                }

                Iterable<String> keys = node.propertyKeys();
                packer.packMapHeader( (int) value.size() );
                for ( String propKey : keys )
                {
                    packer.pack( propKey );
                    packValue( node.property( propKey ) );
                }
            }
            else if ( value.isRelationship() )
            {
                Relationship rel = value.asRelationship();
                packer.packStructHeader( 5, RELATIONSHIP );
                packer.pack( rel.identity().toString() );
                packer.pack( rel.start().toString() );
                packer.pack( rel.end().toString() );

                packer.pack( rel.type() );

                Iterable<String> keys = rel.propertyKeys();
                packer.packMapHeader( (int) value.size() );
                for ( String propKey : keys )
                {
                    packer.pack( propKey );
                    packValue( rel.property( propKey ) );
                }
            }
            else if ( value.isPath() )
            {
                Path path = value.asPath();
                packer.packStructHeader( 1, PATH );
                packer.packListHeader( (int) path.length() * 2 + 1 );
                packValue( value( path.start() ) );
                for ( Path.Segment seg : path )
                {
                    packValue( value( seg.relationship() ) );
                    packValue( value( seg.end() ) );
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
            case MSG_INITIALIZE:
                unpackInitializeMessage( handler );
                break;
            default:
                throw new IOException( "Unknown message type: " + type );
            }
        }

        private void unpackInitializeMessage( MessageHandler handler ) throws IOException
        {
            handler.handleInitializeMessage( unpacker.unpackString() );
            onMessageComplete.run();
        }

        private void unpackIgnoredMessage( MessageHandler output ) throws IOException
        {
            output.handleIgnoredMessage();
            onMessageComplete.run();
        }

        private void unpackFailureMessage( MessageHandler output ) throws IOException
        {
            Map<String,Value> params = unpackRawMap();
            String code = params.get( "code" ).javaString();
            String message = params.get( "message" ).javaString();
            output.handleFailureMessage( code, message );
            onMessageComplete.run();
        }

        private void unpackRunMessage( MessageHandler output ) throws IOException
        {
            String statement = unpacker.unpackString();
            Map<String,Value> params = unpackRawMap();
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
            Map<String,Value> map = unpackRawMap();
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
            case TEXT:
                return value( unpacker.unpackString() );
            case MAP:
            {
                int size = (int) unpacker.unpackMapHeader();
                Map<String,Value> map = new HashMap<>();
                for ( int j = 0; j < size; j++ )
                {
                    String key = unpacker.unpackString();
                    Value value = unpackValue();
                    map.put( key, value );
                }
                return new MapValue( map );
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
                {
                    ensureCorrectStructSize( "NODE", 3, size );
                    String urn = unpacker.unpackString();

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

                    return new NodeValue( new SimpleNode( urn, labels, props ) );
                }
                case RELATIONSHIP:
                {
                    ensureCorrectStructSize( "RELATIONSHIP", 5, size );
                    String urn = unpacker.unpackString();
                    String startUrn = unpacker.unpackString();
                    String endUrn = unpacker.unpackString();
                    String relType = unpacker.unpackString();

                    int numProps = (int) unpacker.unpackMapHeader();
                    Map<String,Value> props = new HashMap<>();
                    for ( int j = 0; j < numProps; j++ )
                    {
                        String key = unpacker.unpackString();
                        Value val = unpackValue();
                        props.put( key, val );
                    }

                    return new RelationshipValue( new SimpleRelationship( urn, startUrn, endUrn, relType, props ) );
                }
                case PATH:
                {
                    ensureCorrectStructSize( "PATH", 1, size );
                    int length = (int) unpacker.unpackListHeader();
                    Entity[] entities = new Entity[length];
                    for ( int i = 0; i < length; i++ )
                    {
                        Value entity = unpackValue();
                        if ( entity.isNode() )
                        {
                            entities[i] = entity.asNode();
                        }
                        else if ( entity.isRelationship() )
                        {
                            entities[i] = entity.asRelationship();
                        }
                        else
                        {
                            throw new RuntimeException( "Entity is neither a node nor a relationship - what gives??" );
                        }
                    }
                    return new PathValue( new SimplePath( entities ) );
                }
                }
            }
            }
            throw new IOException( "Unknown value type: " + type );
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

        private Map<String,Value> unpackRawMap() throws IOException
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
