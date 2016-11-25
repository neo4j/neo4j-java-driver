/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.v1;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.logging.JULogging;
import org.neo4j.driver.internal.net.pooling.PoolSettings;
import org.neo4j.driver.v1.util.Immutable;

import static org.neo4j.driver.v1.Config.TrustStrategy.trustAllCertificates;

/**
 * A configuration class to config driver properties.
 * <p>
 * To create a config:
 * <pre>
 * {@code
 * Config config = Config
 *                  .build()
 *                  .withLogging(new MyLogging())
 *                  .toConfig();
 * }
 * </pre>
 * @since 1.0
 */
@Immutable
public class Config
{
    /** User defined logging */
    private final Logging logging;

    private final int maxIdleConnectionPoolSize;

    /** Level of encryption we need to adhere to */
    private final EncryptionLevel encryptionLevel;

    /** Strategy for how to trust encryption certificate */
    private final TrustStrategy trustStrategy;

    private final int routingFailureLimit;
    private final long routingRetryDelayMillis;

    private Config( ConfigBuilder builder)
    {
        this.logging = builder.logging;

        this.maxIdleConnectionPoolSize = builder.maxIdleConnectionPoolSize;

        this.encryptionLevel = builder.encryptionLevel;
        this.trustStrategy = builder.trustStrategy;
        this.routingFailureLimit = builder.routingFailureLimit;
        this.routingRetryDelayMillis = builder.routingRetryDelayMillis;
    }

    /**
     * Logging provider
     * @return the logging provider to use
     */
    public Logging logging()
    {
        return logging;
    }

    /**
     * Max number of connections per URL for this driver.
     * @return the max number of connections
     */
    @Deprecated
    public int connectionPoolSize()
    {
        return maxIdleConnectionPoolSize;
    }

    /**
     * Max number of idle connections per URL for this driver.
     * @return the max number of connections
     */
    public int maxIdleConnectionPoolSize()
    {
        return maxIdleConnectionPoolSize;
    }

    /**
     * Pooled connections that have been unused for longer than this timeout will be tested before they are
     * used again, to ensure they are still live.
     *
     * @return idle time in milliseconds
     * @deprecated pooled sessions are automatically checked for validity before being returned to the pool. This
     * method will always return <code>-1</code> and will be possibly removed in future.
     */
    @Deprecated
    public long idleTimeBeforeConnectionTest()
    {
        return -1;
    }

    /**
     * @return the level of encryption required for all connections.
     */
    public EncryptionLevel encryptionLevel()
    {
        return encryptionLevel;
    }

    /**
     * @return the strategy to use to determine the authenticity of an encryption certificate provided by the Neo4j instance we are connecting to.
     */
    public TrustStrategy trustStrategy()
    {
        return trustStrategy;
    }

    /**
     * Return a {@link ConfigBuilder} instance
     * @return a {@link ConfigBuilder} instance
     */
    public static ConfigBuilder build()
    {
        return new ConfigBuilder();
    }

    /**
     * @return A config with all default settings
     */
    public static Config defaultConfig()
    {
        return Config.build().toConfig();
    }

    RoutingSettings routingSettings()
    {
        return new RoutingSettings( routingFailureLimit, routingRetryDelayMillis );
    }

    /**
     * Used to build new config instances
     */
    public static class ConfigBuilder
    {
        private Logging logging = new JULogging( Level.INFO );
        private int maxIdleConnectionPoolSize = PoolSettings.DEFAULT_MAX_IDLE_CONNECTION_POOL_SIZE;
        private EncryptionLevel encryptionLevel = EncryptionLevel.REQUIRED;
        private TrustStrategy trustStrategy = trustAllCertificates();
        private int routingFailureLimit = 1;
        private long routingRetryDelayMillis = 5_000;

        private ConfigBuilder() {}

        /**
         * Provide an alternative logging implementation for the driver to use. By default we use
         * java util logging.
         * @param logging the logging instance to use
         * @return this builder
         */
        public ConfigBuilder withLogging( Logging logging )
        {
            this.logging = logging;
            return this;
        }

        /**
         * The max number of sessions to keep open at once. Configure this
         * higher if you want more concurrent sessions, or lower if you want
         * to lower the pressure on the database instance.
         *
         * If the driver is asked to provide more sessions than this, it will
         * block waiting for another session to be closed, with a timeout.
         *
         * @param size the max number of sessions to keep open
         * @return this builder
         */
        @Deprecated
        public ConfigBuilder withMaxSessions( int size )
        {
            return this;
        }

        /**
         * The max number of idle sessions to keep open at once. Configure this
         * higher if you want more concurrent sessions, or lower if you want
         * to lower the pressure on the database instance.
         *
         * @param size the max number of idle sessions to keep open
         * @return this builder
         */
        public ConfigBuilder withMaxIdleSessions( int size )
        {
            this.maxIdleConnectionPoolSize = size;
            return this;
        }

        /**
         * Pooled sessions that have been unused for longer than this timeout
         * will be tested before they are used again, to ensure they are still live.
         *
         * If this option is set too low, an additional network call will be
         * incurred when acquiring a session, which causes a performance hit.
         *
         * If this is set high, you may receive sessions that are no longer live,
         * which will lead to exceptions in your application. Assuming the
         * database is running, these exceptions will go away if you retry acquiring
         * sessions.
         *
         * Hence, this parameter tunes a balance between the likelihood of your
         * application seeing connection problems, and performance.
         *
         * You normally should not need to tune this parameter.
         *
         * @param timeout minimum idle time in milliseconds
         * @return this builder
         * @deprecated pooled sessions are automatically checked for validity before being returned to the pool.
         * This setting will be ignored and possibly removed in future.
         */
        @Deprecated
        public ConfigBuilder withSessionLivenessCheckTimeout( long timeout )
        {
            return this;
        }

        /**
         * Configure the {@link EncryptionLevel} to use, use this to control wether the driver uses TLS encryption or not.
         * @param level the TLS level to use
         * @return this builder
         */
        public ConfigBuilder withEncryptionLevel( EncryptionLevel level )
        {
            this.encryptionLevel = level;
            return this;
        }

        /**
         * Specify how to determine the authenticity of an encryption certificate provided by the Neo4j instance we are connecting to.
         * This defaults to {@link TrustStrategy#trustOnFirstUse(File)}.
         * See {@link TrustStrategy#trustCustomCertificateSignedBy(File)} for using certificate signatures instead to verify
         * trust.
         * <p>
         * This is an important setting to understand, because unless we know that the remote server we have an encrypted connection to
         * is really Neo4j, there is no point to encrypt at all, since anyone could pretend to be the remote Neo4j instance.
         * <p>
         * For this reason, there is no option to disable trust verification, if you find this cumbersome you should disable encryption using
         *
         * @param trustStrategy TLS authentication strategy
         * @return this builder
         */
        public ConfigBuilder withTrustStrategy( TrustStrategy trustStrategy )
        {
            this.trustStrategy = trustStrategy;
            return this;
        }

        /**
         * Specify how many times the client should attempt to reconnect to the routing servers before declaring the
         * cluster unavailable.
         * <p>
         * The routing servers are tried in order. If connecting any of them fails, they are all retried after
         * {@linkplain #withRoutingRetryDelay a delay}. This process of retrying all servers is then repeated for the
         * number of times specified here before considering the cluster unavailable.
         * <p>
         * The default value of this parameter is {@code 1}, which means that the the driver will not re-attempt to
         * connect to the cluster when connecting has failed to each individual server in the list of routers. This
         * default value is sensible under this assumption that if the attempt to connect fails for all servers, then
         * the entire cluster is down, or the client is disconnected from the network, and retrying to connect will
         * not bring it back up, in which case it is better to report the failure sooner.
         *
         * @param routingFailureLimit
         *         the number of times to retry each server in the list of routing servers
         * @return this builder
         */
        public ConfigBuilder withRoutingFailureLimit( int routingFailureLimit )
        {
            if ( routingFailureLimit < 1 )
            {
                throw new IllegalArgumentException(
                        "The failure limit may not be smaller than 1, but was: " + routingFailureLimit );
            }
            this.routingFailureLimit = routingFailureLimit;
            return this;
        }

        /**
         * Specify how long to wait before retrying to connect to a routing server.
         * <p>
         * When connecting to all routing servers fail, connecting will be retried after the delay specified here.
         * The delay is measured from when the first attempt to connect was made, so that the delay time specifies a
         * retry interval.
         * <p>
         * For each {@linkplain #withRoutingFailureLimit retry attempt} the delay time will be doubled. The time
         * specified here is the base time, i.e. the time to wait before the first retry. If that attempt (on all
         * servers) also fails, the delay before the next retry will be double the time specified here, and the next
         * attempt after that will be double that, et.c. So if, for example, the delay specified here is
         * {@code 5 SECONDS}, then after attempting to connect to each server fails reconnecting will be attempted
         * 5 seconds after the first connection attempt to the first server. If that attempt also fails to connect to
         * all servers, the next attempt will start 10 seconds after the second attempt started.
         * <p>
         * The default value of this parameter is {@code 5 SECONDS}.
         *
         * @param delay
         *         the amount of time between attempts to reconnect to the same server
         * @param unit
         *         the unit in which the duration is given
         * @return this builder
         */
        public ConfigBuilder withRoutingRetryDelay( long delay, TimeUnit unit )
        {
            long routingRetryDelayMillis = unit.toMillis( delay );
            if ( routingRetryDelayMillis < 0 )
            {
                throw new IllegalArgumentException( String.format(
                        "The retry delay may not be smaller than 0, but was %d %s.", delay, unit ) );
            }
            this.routingRetryDelayMillis = routingRetryDelayMillis;
            return this;
        }

        /**
         * Create a config instance from this builder.
         * @return a {@link Config} instance
         */
        public Config toConfig()
        {
            return new Config( this );
        }
    }

    /**
     * Control the level of encryption to require
     */
    public enum EncryptionLevel
    {
        /** With this level, the driver will only connect to the server if it can do it without encryption. */
        NONE,

        /** With this level, the driver will only connect to the server it if can do it with encryption. */
        REQUIRED
    }

    /**
     * Control how the driver determines if it can trust the encryption certificates provided by the Neo4j instance it is connected to.
     */
    public static class TrustStrategy
    {
        public enum Strategy
        {
            @Deprecated
            TRUST_ON_FIRST_USE,

            @Deprecated
            TRUST_SIGNED_CERTIFICATES,

            TRUST_ALL_CERTIFICATES,

            TRUST_CUSTOM_CA_SIGNED_CERTIFICATES,

            TRUST_SYSTEM_CA_SIGNED_CERTIFICATES
        }

        private final Strategy strategy;
        private final File certFile;

        private TrustStrategy( Strategy strategy )
        {
            this( strategy, null );
        }

        private TrustStrategy( Strategy strategy, File certFile )
        {
            this.strategy = strategy;
            this.certFile = certFile;
        }

        /**
         * Return the strategy type desired.
         * @return the strategy we should use
         */
        public Strategy strategy()
        {
            return strategy;
        }

        public File certFile()
        {
            return certFile;
        }

        /**
         * Use {@link #trustCustomCertificateSignedBy(File)} instead.
         */
        @Deprecated
        public static TrustStrategy trustSignedBy( File certFile )
        {
            return new TrustStrategy( Strategy.TRUST_SIGNED_CERTIFICATES, certFile );
        }

        /**
         * Only encrypted connections to Neo4j instances with certificates signed by a trusted certificate will be accepted.
         * The file specified should contain one or more trusted X.509 certificates.
         * <p>
         * The certificate(s) in the file must be encoded using PEM encoding, meaning the certificates in the file should be encoded using Base64,
         * and each certificate is bounded at the beginning by "-----BEGIN CERTIFICATE-----", and bounded at the end by "-----END CERTIFICATE-----".
         *
         * @param certFile the trusted certificate file
         * @return an authentication config
         */
        public static TrustStrategy trustCustomCertificateSignedBy( File certFile )
        {
            return new TrustStrategy( Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES, certFile );
        }

        /**
         *
         * @return
         */
        public static TrustStrategy trustSystemCertificates()
        {
            return new TrustStrategy( Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES );
        }

        /**
         *
         * @return
         *
         * @since 1.1
         */
        public static TrustStrategy trustAllCertificates()
        {
            return new TrustStrategy( Strategy.TRUST_ALL_CERTIFICATES );
        }

        /**
         * Automatically trust a Neo4j instance the first time we see it - but fail to connect if its encryption certificate ever changes.
         * This is similar to the mechanism used in SSH, and protects against man-in-the-middle attacks that occur after the initial setup of your application.
         * <p>
         * Known Neo4j hosts are recorded in a file, {@code certFile}.
         * Each time we reconnect to a known host, we verify that its certificate remains the same, guarding against attackers intercepting our communication.
         * <p>
         * Note that this approach is vulnerable to man-in-the-middle attacks the very first time you connect to a new Neo4j instance.
         * If you do not trust the network you are connecting over, consider using {@link #trustCustomCertificateSignedBy(File)}  signed certificates} instead, or manually adding the
         * trusted host line into the specified file.
         *
         * @param knownHostsFile a file where known certificates are stored.
         * @return an authentication config
         *
         * @deprecated in 1.1 in favour of {@link #trustAllCertificates()}
         */
        @Deprecated
        public static TrustStrategy trustOnFirstUse( File knownHostsFile )
        {
            return new TrustStrategy( Strategy.TRUST_ON_FIRST_USE, knownHostsFile );
        }
    }
}
