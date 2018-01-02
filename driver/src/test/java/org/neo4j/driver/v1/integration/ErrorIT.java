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

import io.netty.channel.Channel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.messaging.FailureMessage;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.util.ChannelTrackingDriverFactory;
import org.neo4j.driver.internal.util.ChannelTrackingDriverFactoryWithMessageFormat;
import org.neo4j.driver.internal.util.FailingMessageFormat;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Iterables.single;

public class ErrorIT
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Test
    public void shouldThrowHelpfulSyntaxError() throws Throwable
    {
        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( startsWith( "Invalid input") );

        // When
        StatementResult result = session.run( "invalid statement" );
        result.consume();
    }

    @Test
    public void shouldNotAllowMoreTxAfterClientException() throws Throwable
    {
        // Given
        Transaction tx = session.beginTransaction();

        // And Given an error has occurred
        try { tx.run( "invalid" ).consume(); } catch ( ClientException e ) {/*empty*/}

        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Cannot run more statements in this transaction, " +
                                 "because previous statements in the" );

        // When
        StatementResult cursor = tx.run( "RETURN 1" );
        cursor.single().get( "1" ).asInt();
    }

    @Test
    public void shouldAllowNewStatementAfterRecoverableError() throws Throwable
    {
        // Given an error has occurred
        try { session.run( "invalid" ).consume(); } catch ( ClientException e ) {/*empty*/}

        // When
        StatementResult cursor = session.run( "RETURN 1" );
        int val = cursor.single().get( "1" ).asInt();

        // Then
        assertThat( val, equalTo( 1 ) );
    }

    @Test
    public void shouldAllowNewTransactionAfterRecoverableError() throws Throwable
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
            StatementResult cursor = tx.run( "RETURN 1" );
            int val = cursor.single().get( "1" ).asInt();

            // Then
            assertThat( val, equalTo( 1 ) );
        }
    }

    @Test
    public void shouldExplainConnectionError() throws Throwable
    {
        exception.expect( ServiceUnavailableException.class );
        exception.expectMessage( "Unable to connect to localhost:7777, ensure the database is running " +
                                 "and that there is a working network connection to it." );

        GraphDatabase.driver( "bolt://localhost:7777" );
    }

    @Test
    public void shouldHandleFailureAtCommitTime() throws Throwable
    {
        String label = UUID.randomUUID().toString();  // avoid clashes with other tests

        // given
        Transaction tx = session.beginTransaction();
        tx.run( "CREATE CONSTRAINT ON (a:`" + label + "`) ASSERT a.name IS UNIQUE" );
        tx.success();
        tx.close();

        // and
        tx = session.beginTransaction();
        tx.run( "CREATE INDEX ON :`" + label + "`(name)" );
        tx.success();

        // then expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Label '" + label + "' and property 'name' have a unique " +
                "constraint defined on them, so an index is already created that matches this." );

        // when
        tx.close();

    }

    @Test
    public void shouldGetHelpfulErrorWhenTryingToConnectToHttpPort() throws Throwable
    {
        //the http server needs some time to start up
        Thread.sleep( 2000 );

        Config config = Config.build().withoutEncryption().toConfig();

        exception.expect( ClientException.class );
        exception.expectMessage(
                "Server responded HTTP. Make sure you are not trying to connect to the http endpoint " +
                "(HTTP defaults to port 7474 whereas BOLT defaults to port 7687)" );

        GraphDatabase.driver( "bolt://localhost:7474", config );
    }

    @Test
    public void shouldCloseChannelOnRuntimeExceptionInOutboundMessage() throws InterruptedException
    {
        RuntimeException error = new RuntimeException( "Unable to encode message" );
        Throwable queryError = testChannelErrorHandling( messageFormat -> messageFormat.makeWriterThrow( error ) );

        assertEquals( error, queryError );
    }

    @Test
    public void shouldCloseChannelOnIOExceptionInOutboundMessage() throws InterruptedException
    {
        IOException error = new IOException( "Unable to write" );
        Throwable queryError = testChannelErrorHandling( messageFormat -> messageFormat.makeWriterThrow( error ) );

        assertThat( queryError, instanceOf( ServiceUnavailableException.class ) );
        assertEquals( "Connection to the database failed", queryError.getMessage() );
        assertEquals( error, queryError.getCause() );
    }

    @Test
    public void shouldCloseChannelOnRuntimeExceptionInInboundMessage() throws InterruptedException
    {
        RuntimeException error = new RuntimeException( "Unable to decode message" );
        Throwable queryError = testChannelErrorHandling( messageFormat -> messageFormat.makeReaderThrow( error ) );

        assertEquals( error, queryError );
    }

    @Test
    public void shouldCloseChannelOnIOExceptionInInboundMessage() throws InterruptedException
    {
        IOException error = new IOException( "Unable to read" );
        Throwable queryError = testChannelErrorHandling( messageFormat -> messageFormat.makeReaderThrow( error ) );

        assertThat( queryError, instanceOf( ServiceUnavailableException.class ) );
        assertEquals( "Connection to the database failed", queryError.getMessage() );
        assertEquals( error, queryError.getCause() );
    }

    @Test
    public void shouldCloseChannelOnInboundFatalFailureMessage() throws InterruptedException
    {
        String errorCode = "Neo.ClientError.Request.Invalid";
        String errorMessage = "Very wrong request";
        FailureMessage failureMsg = new FailureMessage( errorCode, errorMessage );

        Throwable queryError = testChannelErrorHandling( messageFormat -> messageFormat.makeReaderFail( failureMsg ) );

        assertThat( queryError, instanceOf( ClientException.class ) );
        assertEquals( ((ClientException) queryError).code(), errorCode );
        assertEquals( queryError.getMessage(), errorMessage );
    }

    private Throwable testChannelErrorHandling( Consumer<FailingMessageFormat> messageFormatSetup )
            throws InterruptedException
    {
        FailingMessageFormat messageFormat = new FailingMessageFormat();

        ChannelTrackingDriverFactoryWithMessageFormat driverFactory = new ChannelTrackingDriverFactoryWithMessageFormat(
                messageFormat, new FakeClock() );

        URI uri = session.uri();
        AuthToken authToken = session.authToken();
        RoutingSettings routingSettings = new RoutingSettings( 1, 1 );
        RetrySettings retrySettings = RetrySettings.DEFAULT;
        Config config = Config.build().withLogging( DEV_NULL_LOGGING ).toConfig();
        Throwable queryError = null;

        try ( Driver driver = driverFactory.newInstance( uri, authToken, routingSettings, retrySettings, config );
              Session session = driver.session() )
        {
            messageFormatSetup.accept( messageFormat );

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
}
