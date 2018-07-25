package org.neo4j.driver.internal.messaging.v3;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.stream.Stream;

import org.neo4j.driver.internal.Bookmark;
import org.neo4j.driver.internal.messaging.AbstractMessageWriterTestBase;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.BeginMessage;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.request.InitMessage;
import org.neo4j.driver.internal.messaging.request.RunMessage;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.internal.security.InternalAuthToken;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.neo4j.driver.internal.messaging.request.CommitMessage.COMMIT;
import static org.neo4j.driver.internal.messaging.request.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.driver.internal.messaging.request.GoodbyeMessage.GOODBYE;
import static org.neo4j.driver.internal.messaging.request.PullAllMessage.PULL_ALL;
import static org.neo4j.driver.internal.messaging.request.ResetMessage.RESET;
import static org.neo4j.driver.internal.messaging.request.RollbackMessage.ROLLBACK;
import static org.neo4j.driver.v1.AuthTokens.basic;
import static org.neo4j.driver.v1.Values.point;
import static org.neo4j.driver.v1.Values.value;

class MessageWriterV3Test extends AbstractMessageWriterTestBase
{
    @Override
    protected MessageFormat.Writer newWriter( PackOutput output )
    {
        return new MessageWriterV3( output );
    }

    @Override
    protected Stream<Message> supportedMessages()
    {
        return Stream.of(
                // Bolt V3 messages
                new HelloMessage( "MyDriver/1.2.3", ((InternalAuthToken) basic( "neo4j", "neo4j" )).toMap() ),
                GOODBYE,
                new BeginMessage( Bookmark.from( "neo4j:bookmark:v1:tx123" ), Duration.ofSeconds( 5 ), singletonMap( "key", value( 42 ) ) ),
                COMMIT,
                ROLLBACK,
                new RunWithMetadataMessage( "RETURN 1", emptyMap(), Bookmark.from( "neo4j:bookmark:v1:tx1" ), Duration.ofSeconds( 5 ),
                        singletonMap( "key", value( 42 ) ) ),
                PULL_ALL,
                DISCARD_ALL,
                RESET,

                // Bolt V3 messages with struct values
                new RunWithMetadataMessage( "RETURN $x", singletonMap( "x", value( ZonedDateTime.now() ) ), Bookmark.empty(),
                        Duration.ofSeconds( 1 ), emptyMap() ),
                new RunWithMetadataMessage( "RETURN $x", singletonMap( "x", point( 42, 1, 2, 3 ) ), Bookmark.empty(),
                        Duration.ofSeconds( 1 ), emptyMap() )
        );
    }

    @Override
    protected Stream<Message> unsupportedMessages()
    {
        return Stream.of(
                // Bolt V1 and V2 messages
                new InitMessage( "Apa", emptyMap() ),
                new RunMessage( "RETURN 1" )
        );
    }
}
