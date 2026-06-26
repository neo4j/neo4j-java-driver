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
package org.neo4j.driver.internal.svm;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import com.oracle.svm.core.jdk.JDK11OrLater;
import com.oracle.svm.core.jdk.JDK8OrEarlier;
import io.netty.bootstrap.AbstractBootstrapConfig;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultChannelPromise;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import javax.net.ssl.SSLEngine;

/**
 * This substitution avoid having loggers added to the build
 */
@TargetClass(className = "io.netty.util.internal.logging.InternalLoggerFactory")
final class Target_io_netty_util_internal_logging_InternalLoggerFactory {

    @Substitute
    private static InternalLoggerFactory newDefaultFactory(String name) {
        return JdkLoggerFactory.INSTANCE;
    }
}

// SSL
// This whole section is mostly about removing static analysis references to openssl/tcnative
@TargetClass(className = "io.netty.handler.ssl.SslHandler$SslEngineType")
final class Target_io_netty_handler_ssl_SslHandler$SslEngineType {

    @Alias
    public static Target_io_netty_handler_ssl_SslHandler$SslEngineType JDK;

    @Substitute
    static Target_io_netty_handler_ssl_SslHandler$SslEngineType forEngine(SSLEngine engine) {
        return JDK;
    }
}

@TargetClass(
        className = "io.netty.handler.ssl.JdkAlpnApplicationProtocolNegotiator$AlpnWrapper",
        onlyWith = JDK11OrLater.class)
final class Target_io_netty_handler_ssl_JdkAlpnApplicationProtocolNegotiator_AlpnWrapper {
    @Substitute
    public SSLEngine wrapSslEngine(
            SSLEngine engine,
            ByteBufAllocator alloc,
            @SuppressWarnings("deprecation")
                    io.netty.handler.ssl.JdkApplicationProtocolNegotiator applicationNegotiator,
            boolean isServer) {
        return (SSLEngine)
                (Object) new Target_io_netty_handler_ssl_JdkAlpnSslEngine(engine, applicationNegotiator, isServer);
    }
}

@TargetClass(
        className = "io.netty.handler.ssl.JdkAlpnApplicationProtocolNegotiator$AlpnWrapper",
        onlyWith = JDK8OrEarlier.class)
final class Target_io_netty_handler_ssl_JdkAlpnApplicationProtocolNegotiator_AlpnWrapperJava8 {
    @Substitute
    public SSLEngine wrapSslEngine(
            SSLEngine engine,
            ByteBufAllocator alloc,
            @SuppressWarnings("deprecation")
                    io.netty.handler.ssl.JdkApplicationProtocolNegotiator applicationNegotiator,
            boolean isServer) {
        if (Target_io_netty_handler_ssl_JettyAlpnSslEngine.isAvailable()) {
            return isServer
                    ? (SSLEngine) (Object) Target_io_netty_handler_ssl_JettyAlpnSslEngine.newServerEngine(
                            engine, applicationNegotiator)
                    : (SSLEngine) (Object) Target_io_netty_handler_ssl_JettyAlpnSslEngine.newClientEngine(
                            engine, applicationNegotiator);
        }
        throw new RuntimeException(
                "Unable to wrap SSLEngine of type " + engine.getClass().getName());
    }
}

@TargetClass(className = "io.netty.handler.ssl.JettyAlpnSslEngine", onlyWith = JDK8OrEarlier.class)
final class Target_io_netty_handler_ssl_JettyAlpnSslEngine {
    @Substitute
    static boolean isAvailable() {
        return false;
    }

    @Substitute
    @SuppressWarnings("deprecation")
    static Target_io_netty_handler_ssl_JettyAlpnSslEngine newClientEngine(
            SSLEngine engine, io.netty.handler.ssl.JdkApplicationProtocolNegotiator applicationNegotiator) {
        return null;
    }

    @Substitute
    @SuppressWarnings("deprecation")
    static Target_io_netty_handler_ssl_JettyAlpnSslEngine newServerEngine(
            SSLEngine engine, io.netty.handler.ssl.JdkApplicationProtocolNegotiator applicationNegotiator) {
        return null;
    }
}

@TargetClass(className = "io.netty.handler.ssl.JdkAlpnSslEngine", onlyWith = JDK11OrLater.class)
final class Target_io_netty_handler_ssl_JdkAlpnSslEngine {

    @Alias
    @SuppressWarnings("deprecation")
    Target_io_netty_handler_ssl_JdkAlpnSslEngine(
            final SSLEngine engine,
            final io.netty.handler.ssl.JdkApplicationProtocolNegotiator applicationNegotiator,
            final boolean isServer) {}
}

@TargetClass(className = "io.netty.handler.ssl.JdkDefaultApplicationProtocolNegotiator")
final class Target_io_netty_handler_ssl_JdkDefaultApplicationProtocolNegotiator {

    @Alias
    public static Target_io_netty_handler_ssl_JdkDefaultApplicationProtocolNegotiator INSTANCE;
}

@TargetClass(className = "io.netty.handler.ssl.JdkSslContext")
final class Target_io_netty_handler_ssl_JdkSslContext {

    @SuppressWarnings("deprecation")
    @Substitute
    static io.netty.handler.ssl.JdkApplicationProtocolNegotiator toNegotiator(
            ApplicationProtocolConfig config, boolean isServer) {
        if (config == null) {
            return (io.netty.handler.ssl.JdkApplicationProtocolNegotiator)
                    (Object) Target_io_netty_handler_ssl_JdkDefaultApplicationProtocolNegotiator.INSTANCE;
        }

        switch (config.protocol()) {
            case NONE:
                return (io.netty.handler.ssl.JdkApplicationProtocolNegotiator)
                        (Object) Target_io_netty_handler_ssl_JdkDefaultApplicationProtocolNegotiator.INSTANCE;
            case ALPN:
                if (isServer) {
                    // GRAAL RC9 bug: https://github.com/oracle/graal/issues/813
                    //                switch(config.selectorFailureBehavior()) {
                    //                case FATAL_ALERT:
                    //                    return new JdkAlpnApplicationProtocolNegotiator(true,
                    // config.supportedProtocols());
                    //                case NO_ADVERTISE:
                    //                    return new JdkAlpnApplicationProtocolNegotiator(false,
                    // config.supportedProtocols());
                    //                default:
                    //                    throw new UnsupportedOperationException(new StringBuilder("JDK provider does
                    // not support ")
                    //                    .append(config.selectorFailureBehavior()).append(" failure
                    // behavior").toString());
                    //                }
                    SelectorFailureBehavior behavior = config.selectorFailureBehavior();
                    if (behavior == SelectorFailureBehavior.FATAL_ALERT)
                        return new io.netty.handler.ssl.JdkAlpnApplicationProtocolNegotiator(
                                true, config.supportedProtocols());
                    else if (behavior == SelectorFailureBehavior.NO_ADVERTISE)
                        return new io.netty.handler.ssl.JdkAlpnApplicationProtocolNegotiator(
                                false, config.supportedProtocols());
                    else {
                        throw new UnsupportedOperationException(new StringBuilder("JDK provider does not support ")
                                .append(config.selectorFailureBehavior())
                                .append(" failure behavior")
                                .toString());
                    }
                } else {
                    switch (config.selectedListenerFailureBehavior()) {
                        case ACCEPT:
                            return new io.netty.handler.ssl.JdkAlpnApplicationProtocolNegotiator(
                                    false, config.supportedProtocols());
                        case FATAL_ALERT:
                            return new io.netty.handler.ssl.JdkAlpnApplicationProtocolNegotiator(
                                    true, config.supportedProtocols());
                        default:
                            throw new UnsupportedOperationException(new StringBuilder("JDK provider does not support ")
                                    .append(config.selectedListenerFailureBehavior())
                                    .append(" failure behavior")
                                    .toString());
                    }
                }
            default:
                throw new UnsupportedOperationException(new StringBuilder("JDK provider does not support ")
                        .append(config.protocol())
                        .append(" protocol")
                        .toString());
        }
    }
}

/*
 * This one only prints exceptions otherwise we get a useless bogus
 * exception message: https://github.com/eclipse-vertx/vert.x/issues/1657
 */
@TargetClass(className = "io.netty.bootstrap.AbstractBootstrap")
final class Target_io_netty_bootstrap_AbstractBootstrap {

    @SuppressWarnings("deprecation")
    @Alias
    private io.netty.bootstrap.ChannelFactory channelFactory;

    @Alias
    void init(Channel channel) throws Exception {}

    @Alias
    public AbstractBootstrapConfig config() {
        return null;
    }

    @Substitute
    final ChannelFuture initAndRegister() {
        Channel channel = null;
        try {
            channel = channelFactory.newChannel();
            init(channel);
        } catch (Throwable t) {
            // THE FIX IS HERE:
            t.printStackTrace();
            if (channel != null) {
                // channel can be null if newChannel crashed (eg SocketException("too many open files"))
                channel.unsafe().closeForcibly();
            }
            // as the Channel is not registered yet we need to force the usage of the GlobalEventExecutor
            return new DefaultChannelPromise(channel, GlobalEventExecutor.INSTANCE).setFailure(t);
        }

        ChannelFuture regFuture = config().group().register(channel);
        if (regFuture.cause() != null) {
            if (channel.isRegistered()) {
                channel.close();
            } else {
                channel.unsafe().closeForcibly();
            }
        }

        // If we are here and the promise is not failed, it's one of the following cases:
        // 1) If we attempted registration from the event loop, the registration has been completed at this point.
        //    i.e. It's safe to attempt bind() or connect() now because the channel has been registered.
        // 2) If we attempted registration from the other thread, the registration request has been successfully
        //    added to the event loop's task queue for later execution.
        //    i.e. It's safe to attempt bind() or connect() now:
        //         because bind() or connect() will be executed *after* the scheduled registration task is executed
        //         because register(), bind(), and connect() are all bound to the same thread.

        return regFuture;
    }
}

class NettySubstitutions {}
