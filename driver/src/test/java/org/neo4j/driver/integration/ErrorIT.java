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
package org.neo4j.driver.integration;

import io.netty.channel.Channel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.messaging.response.FailureMessage;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.internal.util.FailingMessageFormat;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.util.io.ChannelTrackingDriverFactory;
import org.neo4j.driver.internal.util.io.ChannelTrackingDriverFactoryWithFailingMessageFormat;
import org.neo4j.driver.util.ParallelizableIT;
import org.neo4j.driver.util.SessionExtension;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Iterables.single;

@ParallelizableIT
class ErrorIT
{
    @RegisterExtension
    static final SessionExtension session = new SessionExtension();

    @Test
    void shouldThrowHelpfulSyntaxError()
    {
        ClientException e = assertThrows( ClientException.class, () ->
        {
            Result result = session.run( "invalid query" );
            result.consume();
        } );

        assertThat( e.getMessage(), startsWith( "Invalid input" ) );
    }

    @Test
    void shouldNotAllowMoreTxAfterClientException()
    {
        // Given
        Transaction tx = session.beginTransaction();

        // And Given an error has occurred
        try { tx.run( "invalid" ).consume(); } catch ( ClientException e ) {/*empty*/}

        // Expect
        ClientException e = assertThrows( ClientException.class, () ->
        {
            Result cursor = tx.run( "RETURN 1" );
            cursor.single().get( "1" ).asInt();
        } );
        assertThat( e.getMessage(), startsWith( "Cannot run more queries in this transaction" ) );
    }

    @Test
    void shouldAllowNewQueryAfterRecoverableError()
    {
        // Given an error has occurred
        try { session.run( "invalid" ).consume(); } catch ( ClientException e ) {/*empty*/}

        // When
        Result cursor = session.run( "RETURN 1" );
        int val = cursor.single().get( "1" ).asInt();

        // Then
        assertThat( val, equalTo( 1 ) );
    }

    @Test
    void shouldAllowNewTransactionAfterRecoverableError()
    {
        // Given an error has occurred in a prior transaction
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "invalid" ).consume();
        }
        catch ( ClientException e ) {/*empty*/}

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            Result cursor = tx.run( "RETURN 1" );
            int val = cursor.single().get( "1" ).asInt();

            // Then
            assertThat( val, equalTo( 1 ) );
        }
    }

    @Test
    void shouldExplainConnectionError()
    {
        final Driver driver = GraphDatabase.driver( "bolt://localhost:7777" );
        ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, driver::verifyConnectivity );

        assertEquals( "Unable to connect to localhost:7777, ensure the database is running " +
                      "and that there is a working network connection to it.", e.getMessage() );
    }

    @Test
    void shouldHandleFailureAtCommitTime()
    {
        String label = UUID.randomUUID().toString();  // avoid clashes with other tests

        // given
        Transaction tx = session.beginTransaction();
        tx.run( "CREATE CONSTRAINT ON (a:`" + label + "`) ASSERT a.name IS UNIQUE" );
        tx.commit();

        // and
        tx = session.beginTransaction();
        tx.run( "CREATE INDEX ON :`" + label + "`(name)" );

        // then expect
        ClientException e = assertThrows( ClientException.class, tx::commit );
        assertThat( e.getMessage(), containsString( label ) );
        assertThat( e.getMessage(), containsString( "name" ) );
    }

    @Test
    void shouldGetHelpfulErrorWhenTryingToConnectToHttpPort() throws Throwable
    {
        //the http server needs some time to start up
        Thread.sleep( 2000 );

        Config config = Config.builder().withoutEncryption().build();

        final Driver driver = GraphDatabase.driver( "bolt://localhost:" + session.httpPort(), config );
        ClientException e = assertThrows( ClientException.class, driver::verifyConnectivity );
        assertEquals( "Server responded HTTP. Make sure you are not trying to connect to the http endpoint " +
                      "(HTTP defaults to port 7474 whereas BOLT defaults to port 7687)", e.getMessage() );
    }

    @Test
    void shouldCloseChannelOnRuntimeExceptionInOutboundMessage() throws InterruptedException
    {
        RuntimeException error = new RuntimeException( "Unable to encode message" );
        Throwable queryError = testChannelErrorHandling( messageFormat -> messageFormat.makeWriterThrow( error ) );

        assertEquals( error, queryError );
    }

    @Test
    void shouldCloseChannelOnIOExceptionInOutboundMessage() throws InterruptedException
    {
        IOException error = new IOException( "Unable to write" );
        Throwable queryError = testChannelErrorHandling( messageFormat -> messageFormat.makeWriterThrow( error ) );

        assertThat( queryError, instanceOf( ServiceUnavailableException.class ) );
        assertEquals( "Connection to the database failed", queryError.getMessage() );
        assertEquals( error, queryError.getCause() );
    }

    @Test
    void shouldCloseChannelOnRuntimeExceptionInInboundMessage() throws InterruptedException
    {
        RuntimeException error = new RuntimeException( "Unable to decode message" );
        Throwable queryError = testChannelErrorHandling( messageFormat -> messageFormat.makeReaderThrow( error ) );

        assertEquals( error, queryError );
    }

    @Test
    void shouldCloseChannelOnIOExceptionInInboundMessage() throws InterruptedException
    {
        IOException error = new IOException( "Unable to read" );
        Throwable queryError = testChannelErrorHandling( messageFormat -> messageFormat.makeReaderThrow( error ) );

        assertThat( queryError, instanceOf( ServiceUnavailableException.class ) );
        assertEquals( "Connection to the database failed", queryError.getMessage() );
        assertEquals( error, queryError.getCause() );
    }

    @Test
    void shouldCloseChannelOnInboundFatalFailureMessage() throws InterruptedException
    {
        String errorCode = "Neo.ClientError.Request.Invalid";
        String errorMessage = "Very wrong request";
        FailureMessage failureMsg = new FailureMessage( errorCode, errorMessage );

        Throwable queryError = testChannelErrorHandling( messageFormat -> messageFormat.makeReaderFail( failureMsg ) );

        assertThat( queryError, instanceOf( ClientException.class ) );
        assertEquals( ((ClientException) queryError).code(), errorCode );
        assertEquals( queryError.getMessage(), errorMessage );
    }

    @Test
    void shouldThrowErrorWithNiceStackTrace( TestInfo testInfo )
    {
        ClientException error = assertThrows( ClientException.class, () -> session.run( "RETURN 10 / 0" ).consume() );

        // thrown error should have this class & method in the stacktrace
        StackTraceElement[] stackTrace = error.getStackTrace();
        assertTrue( Stream.of( stackTrace ).anyMatch( element -> testClassAndMethodMatch( testInfo, element ) ),
                () -> "Expected stacktrace element is absent:\n" + Arrays.toString( stackTrace ) );

        // thrown error should have a suppressed error with an async stacktrace
        assertThat( asList( error.getSuppressed() ), hasSize( greaterThanOrEqualTo( 1 ) ) );
    }

    private Throwable testChannelErrorHandling( Consumer<FailingMessageFormat> messageFormatSetup )
            throws InterruptedException
    {
        ChannelTrackingDriverFactoryWithFailingMessageFormat driverFactory = new ChannelTrackingDriverFactoryWithFailingMessageFormat( new FakeClock() );

        URI uri = session.uri();
        AuthToken authToken = session.authToken();
        RoutingSettings routingSettings = RoutingSettings.DEFAULT;
        RetrySettings retrySettings = RetrySettings.DEFAULT;
        Config config = Config.builder().withLogging( DEV_NULL_LOGGING ).build();
        Throwable queryError = null;

        try ( Driver driver = driverFactory.newInstance( uri, authToken, routingSettings, retrySettings, config, SecurityPlanImpl.insecure() ) )
        {
            driver.verifyConnectivity();
            try(Session session = driver.session() )
            {
                messageFormatSetup.accept( driverFactory.getFailingMessageFormat() );

                try
                {
                    session.run( "RETURN 1" ).consume();
                    fail( "Exception expected" );
                }
                catch ( Throwable error )
                {
                    queryError = error;
                }

                assertSingleChannelIsClosed( driverFactory );
                assertNewQueryCanBeExecuted( session, driverFactory );
            }
        }


        return queryError;
    }

    private void assertSingleChannelIsClosed( ChannelTrackingDriverFactory driverFactory ) throws InterruptedException
    {
        Channel channel = single( driverFactory.channels() );
        assertTrue( channel.closeFuture().await( 10, SECONDS ) );
        assertFalse( channel.isActive() );
    }

    private void assertNewQueryCanBeExecuted( Session session, ChannelTrackingDriverFactory driverFactory )
    {
        assertEquals( 42, session.run( "RETURN 42" ).single().get( 0 ).asInt() );
        List<Channel> channels = driverFactory.channels();
        Channel lastChannel = channels.get( channels.size() - 1 );
        assertTrue( lastChannel.isActive() );
    }

    private static boolean testClassAndMethodMatch( TestInfo testInfo, StackTraceElement element )
    {
        return testClassMatches( testInfo, element ) && testMethodMatches( testInfo, element );
    }

    private static boolean testClassMatches( TestInfo testInfo, StackTraceElement element )
    {
        String expectedName = testInfo.getTestClass().map( Class::getName ).orElse( "" );
        String actualName = element.getClassName();
        return Objects.equals( expectedName, actualName );
    }

    private static boolean testMethodMatches( TestInfo testInfo, StackTraceElement element )
    {
        String expectedName = testInfo.getTestMethod().map( Method::getName ).orElse( "" );
        String actualName = element.getMethodName();
        return Objects.equals( expectedName, actualName );
    }
}
