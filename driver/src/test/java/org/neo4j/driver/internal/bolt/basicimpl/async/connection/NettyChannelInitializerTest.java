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
package org.neo4j.driver.internal.bolt.basicimpl.async.connection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.bolt.api.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.creationTimestamp;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.serverAddress;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandler;
import java.security.GeneralSecurityException;
import java.time.Clock;
import javax.net.ssl.SNIHostName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.RevocationCheckingStrategy;
import org.neo4j.driver.internal.bolt.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.logging.DevNullLogging;
import org.neo4j.driver.internal.security.BoltSecurityPlanManager;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.internal.util.FakeClock;

class NettyChannelInitializerTest {
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldAddSslHandlerWhenRequiresEncryption() throws Exception {
        var security = trustAllCertificates();
        var initializer = newInitializer(security);

        initializer.initChannel(channel);

        assertNotNull(channel.pipeline().get(SslHandler.class));
    }

    @Test
    void shouldNotAddSslHandlerWhenDoesNotRequireEncryption() {
        var security = SecurityPlan.INSECURE;
        var initializer = newInitializer(security);

        initializer.initChannel(channel);

        assertNull(channel.pipeline().get(SslHandler.class));
    }

    @Test
    void shouldAddSslHandlerWithHandshakeTimeout() throws Exception {
        var timeoutMillis = 424242;
        var security = trustAllCertificates();
        var initializer = newInitializer(security, timeoutMillis);

        initializer.initChannel(channel);

        var sslHandler = channel.pipeline().get(SslHandler.class);
        assertNotNull(sslHandler);
        assertEquals(timeoutMillis, sslHandler.getHandshakeTimeoutMillis());
    }

    @Test
    void shouldUpdateChannelAttributes() {
        var clock = mock(Clock.class);
        when(clock.millis()).thenReturn(42L);
        var security = SecurityPlan.INSECURE;
        var initializer = newInitializer(security, Integer.MAX_VALUE, clock);

        initializer.initChannel(channel);

        assertEquals(LOCAL_DEFAULT, serverAddress(channel));
        assertEquals(42L, creationTimestamp(channel));
        assertNotNull(messageDispatcher(channel));
    }

    @Test
    void shouldIncludeSniHostName() throws Exception {
        var address = new BoltServerAddress("database.neo4j.com", 8989);
        var initializer = new NettyChannelInitializer(
                address, trustAllCertificates(), 10000, Clock.systemUTC(), NoopLoggingProvider.INSTANCE);

        initializer.initChannel(channel);

        var sslHandler = channel.pipeline().get(SslHandler.class);
        var sslEngine = sslHandler.engine();
        var sslParameters = sslEngine.getSSLParameters();
        var sniServerNames = sslParameters.getServerNames();
        assertThat(sniServerNames, hasSize(1));
        assertThat(sniServerNames.get(0), instanceOf(SNIHostName.class));
        assertThat(((SNIHostName) sniServerNames.get(0)).getAsciiName(), equalTo(address.host()));
    }

    @Test
    void shouldEnableHostnameVerificationWhenConfigured() throws Exception {
        testHostnameVerificationSetting(true, "HTTPS");
    }

    @Test
    void shouldNotEnableHostnameVerificationWhenNotConfigured() throws Exception {
        testHostnameVerificationSetting(false, null);
    }

    private void testHostnameVerificationSetting(boolean enabled, String expectedValue) throws Exception {
        var initializer = newInitializer(BoltSecurityPlanManager.from(SecurityPlanImpl.forAllCertificates(
                        enabled, RevocationCheckingStrategy.NO_CHECKS, null, DevNullLogging.DEV_NULL_LOGGING))
                .plan()
                .toCompletableFuture()
                .join());

        initializer.initChannel(channel);

        var sslHandler = channel.pipeline().get(SslHandler.class);
        var sslEngine = sslHandler.engine();
        var sslParameters = sslEngine.getSSLParameters();
        assertEquals(expectedValue, sslParameters.getEndpointIdentificationAlgorithm());
    }

    private static NettyChannelInitializer newInitializer(SecurityPlan securityPlan) {
        return newInitializer(securityPlan, Integer.MAX_VALUE);
    }

    private static NettyChannelInitializer newInitializer(SecurityPlan securityPlan, int connectTimeoutMillis) {
        return newInitializer(securityPlan, connectTimeoutMillis, new FakeClock());
    }

    private static NettyChannelInitializer newInitializer(
            SecurityPlan securityPlan, int connectTimeoutMillis, Clock clock) {
        return new NettyChannelInitializer(
                LOCAL_DEFAULT, securityPlan, connectTimeoutMillis, clock, NoopLoggingProvider.INSTANCE);
    }

    private static SecurityPlan trustAllCertificates() throws GeneralSecurityException {
        return BoltSecurityPlanManager.from(SecurityPlanImpl.forAllCertificates(
                        false, RevocationCheckingStrategy.NO_CHECKS, null, DevNullLogging.DEV_NULL_LOGGING))
                .plan()
                .toCompletableFuture()
                .join();
    }
}
