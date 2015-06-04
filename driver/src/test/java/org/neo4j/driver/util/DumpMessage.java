package org.neo4j.driver.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.messaging.AckFailureMessage;
import org.neo4j.driver.internal.messaging.DiscardAllMessage;
import org.neo4j.driver.internal.messaging.FailureMessage;
import org.neo4j.driver.internal.messaging.IgnoredMessage;
import org.neo4j.driver.internal.messaging.InitializeMessage;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.MessageHandler;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;
import org.neo4j.driver.internal.messaging.PullAllMessage;
import org.neo4j.driver.internal.messaging.RecordMessage;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.messaging.SuccessMessage;
import org.neo4j.driver.internal.util.BytePrinter;

import static org.neo4j.driver.internal.util.BytePrinter.hexStringToBytes;

public class DumpMessage
{
    public static void main( String[] args ) throws IOException
    {
        if( args.length <= 1)
        {
            System.out.println( "Please specify the PackStreamV1 message (without chunk size and 00 00 ending) " +
                                "that you want to unpack in hex strings");
            return;
        }
        StringBuilder hexStr = new StringBuilder();
        for( int i = 0; i < args.length; i ++ )
        {
            hexStr.append( args[i] );
        }
        // for now we only handle PackStreamV1
        unpackPackStreamV1Message( hexStr.toString() );
    }

    public static void unpackPackStreamV1Message( String hexString ) throws IOException
    {
        byte[] bytes = hexStringToBytes( hexString );
        ArrayList<Message> messages = unpack( new PackStreamMessageFormatV1(), bytes );
        for ( Message message : messages )
        {
            System.out.println( message );
        }
    }

    public static ArrayList<Message> unpack( MessageFormat format, byte[] data )
    {
        final ArrayList<Message> outcome = new ArrayList<>();
        try
        {
            MessageFormat.Reader reader = format.newReader();
            ByteArrayInputStream input = new ByteArrayInputStream( data );
            reader.reset( Channels.newChannel( input ) );
            reader.read( new MessageHandler()
            {
                @Override
                public void handlePullAllMessage()
                {
                    outcome.add( new PullAllMessage() );
                }

                @Override
                public void handleInitializeMessage( String clientNameAndVersion ) throws IOException
                {
                    outcome.add( new InitializeMessage( clientNameAndVersion ) );
                }

                @Override
                public void handleRunMessage( String statement, Map<String,Value> parameters )
                {
                    outcome.add( new RunMessage( statement, parameters ) );
                }

                @Override
                public void handleDiscardAllMessage()
                {
                    outcome.add( new DiscardAllMessage() );
                }

                @Override
                public void handleAckFailureMessage()
                {
                    outcome.add( new AckFailureMessage() );
                }

                @Override
                public void handleSuccessMessage( Map<String,Value> meta )
                {
                    outcome.add( new SuccessMessage( meta ) );
                }

                @Override
                public void handleRecordMessage( Value[] fields )
                {
                    outcome.add( new RecordMessage( fields ) );
                }

                @Override
                public void handleFailureMessage( String code, String message )
                {
                    outcome.add( new FailureMessage( code, message ) );
                }

                @Override
                public void handleIgnoredMessage()
                {
                    outcome.add( new IgnoredMessage() );
                }
            } );
        }
        catch ( Neo4jException e )
        {
            throw e;
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( "Failed to deserialize message. Raw data was:\n" + BytePrinter.hex( data ), e );
        }
        return outcome;
    }
}
