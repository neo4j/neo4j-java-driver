/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V51;
import static org.neo4j.driver.testutil.TestUtil.await;

import io.netty.bootstrap.Bootstrap;
import io.netty.handler.ssl.SslHandler;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.security.GeneralSecurityException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.RevocationCheckingStrategy;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.BoltAgentUtil;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.DefaultDomainNameResolver;
import org.neo4j.driver.internal.async.connection.BootstrapFactory;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.async.connection.ChannelConnectorImpl;
import org.neo4j.driver.internal.async.inbound.ConnectTimeoutHandler;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.internal.security.StaticAuthTokenManager;
import org.neo4j.driver.internal.util.DisabledOnNeo4jWith;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@ParallelizableIT
class ChannelConnectorImplIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private Bootstrap bootstrap;

    @BeforeEach
    void setUp() {
        bootstrap = BootstrapFactory.newBootstrap(1);
    }

    @AfterEach
    void tearDown() {
        if (bootstrap != null) {
            bootstrap.config().group().shutdownGracefully().syncUninterruptibly();
        }
    }

    @Test
    void shouldConnect() throws Exception {
        ChannelConnector connector = newConnector(neo4j.authTokenManager());

        var channelFuture = connector.connect(neo4j.address(), bootstrap);
        assertTrue(channelFuture.await(10, TimeUnit.SECONDS));
        var channel = channelFuture.channel();

        assertNull(channelFuture.get());
        assertTrue(channel.isActive());
    }

    @Test
    void shouldSetupHandlers() throws Exception {
        ChannelConnector connector = newConnector(neo4j.authTokenManager(), trustAllCertificates(), 10_000);

        var channelFuture = connector.connect(neo4j.address(), bootstrap);
        assertTrue(channelFuture.await(10, TimeUnit.SECONDS));

        var channel = channelFuture.channel();
        var pipeline = channel.pipeline();
        assertTrue(channel.isActive());

        assertNotNull(pipeline.get(SslHandler.class));
        assertNull(pipeline.get(ConnectTimeoutHandler.class));
    }

    @Test
    void shouldFailToConnectToWrongAddress() throws Exception {
        ChannelConnector connector = newConnector(neo4j.authTokenManager());

        var channelFuture = connector.connect(new BoltServerAddress("wrong-localhost"), bootstrap);
        assertTrue(channelFuture.await(10, TimeUnit.SECONDS));
        var channel = channelFuture.channel();

        var e = assertThrows(ExecutionException.class, channelFuture::get);

        assertThat(e.getCause(), instanceOf(ServiceUnavailableException.class));
        assertThat(e.getCause().getMessage(), startsWith("Unable to connect"));
        assertFalse(channel.isActive());
    }

    // Beginning with Bolt 5.1 auth is not sent on HELLO message.
    @DisabledOnNeo4jWith(BOLT_V51)
    @Test
    void shouldFailToConnectWithWrongCredentials() throws Exception {
        var authToken = AuthTokens.basic("neo4j", "wrong-password");
        ChannelConnector connector = newConnector(new StaticAuthTokenManager(authToken));

        var channelFuture = connector.connect(neo4j.address(), bootstrap);
        assertTrue(channelFuture.await(10, TimeUnit.SECONDS));
        var channel = channelFuture.channel();

        var e = assertThrows(ExecutionException.class, channelFuture::get);
        assertThat(e.getCause(), instanceOf(AuthenticationException.class));
        assertFalse(channel.isActive());
    }

    @Test
    void shouldEnforceConnectTimeout() throws Exception {
        ChannelConnector connector = newConnector(neo4j.authTokenManager(), 1000);

        // try connect to a non-routable ip address 10.0.0.0, it will never respond
        var channelFuture = connector.connect(new BoltServerAddress("10.0.0.0"), bootstrap);

        assertThrows(ServiceUnavailableException.class, () -> await(channelFuture));
    }

    @Test
    void shouldFailWhenProtocolNegotiationTakesTooLong() throws Exception {
        // run without TLS so that Bolt handshake is the very first operation after connection is established
        testReadTimeoutOnConnect(SecurityPlanImpl.insecure());
    }

    @Test
    @Disabled("TLS actually fails, the test setup is not valid")
    void shouldFailWhenTLSHandshakeTakesTooLong() throws Exception {
        // run with TLS so that TLS handshake is the very first operation after connection is established
        testReadTimeoutOnConnect(trustAllCertificates());
    }

    @Test
    @SuppressWarnings("resource")
    void shouldThrowServiceUnavailableExceptionOnFailureDuringConnect() throws Exception {
        var server = new ServerSocket(0);
        var address = new BoltServerAddress("localhost", server.getLocalPort());

        runAsync(() -> {
            try {
                // wait for a connection
                var socket = server.accept();
                // and terminate it immediately so that client gets a "reset by peer" IOException
                socket.close();
                server.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        ChannelConnector connector = newConnector(neo4j.authTokenManager());
        var channelFuture = connector.connect(address, bootstrap);

        // connect operation should fail with ServiceUnavailableException
        assertThrows(ServiceUnavailableException.class, () -> await(channelFuture));
    }

    private void testReadTimeoutOnConnect(SecurityPlan securityPlan) throws IOException {
        try (var server = new ServerSocket(0)) // server that accepts connections but does not reply
        {
            var timeoutMillis = 1_000;
            var address = new BoltServerAddress("localhost", server.getLocalPort());
            ChannelConnector connector = newConnector(neo4j.authTokenManager(), securityPlan, timeoutMillis);

            var channelFuture = connector.connect(address, bootstrap);

            var e = assertThrows(ServiceUnavailableException.class, () -> await(channelFuture));
            assertEquals(e.getMessage(), "Unable to establish connection in " + timeoutMillis + "ms");
        }
    }

    private ChannelConnectorImpl newConnector(AuthTokenManager authTokenManager) throws Exception {
        return newConnector(authTokenManager, Integer.MAX_VALUE);
    }

    private ChannelConnectorImpl newConnector(AuthTokenManager authTokenManager, int connectTimeoutMillis)
            throws Exception {
        return newConnector(authTokenManager, trustAllCertificates(), connectTimeoutMillis);
    }

    private ChannelConnectorImpl newConnector(
            AuthTokenManager authTokenManager, SecurityPlan securityPlan, int connectTimeoutMillis) {
        var settings = new ConnectionSettings(authTokenManager, "test", connectTimeoutMillis);
        return new ChannelConnectorImpl(
                settings,
                securityPlan,
                DEV_NULL_LOGGING,
                new FakeClock(),
                RoutingContext.EMPTY,
                DefaultDomainNameResolver.getInstance(),
                null,
                BoltAgentUtil.VALUE);
    }

    private static SecurityPlan trustAllCertificates() throws GeneralSecurityException {
        return SecurityPlanImpl.forAllCertificates(false, RevocationCheckingStrategy.NO_CHECKS);
    }
}
