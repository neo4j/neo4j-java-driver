/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.driver.v1.util.Experimental;
import org.neo4j.driver.v1.util.Immutable;
import org.neo4j.driver.v1.util.Resource;

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
    private final boolean logLeakedSessions;

    private final int maxIdleConnectionPoolSize;
    private final int maxConnectionPoolSize;

    private final long idleTimeBeforeConnectionTest;
    private final long maxConnectionLifetimeMillis;
    private final long connectionAcquisitionTimeoutMillis;

    /** Indicator for encrypted traffic */
    private final boolean encrypted;

    /** Strategy for how to trust encryption certificate */
    private final TrustStrategy trustStrategy;

    private final int routingFailureLimit;
    private final long routingRetryDelayMillis;
    private final int connectionTimeoutMillis;
    private final RetrySettings retrySettings;

    private final LoadBalancingStrategy loadBalancingStrategy;

    private Config( ConfigBuilder builder)
    {
        this.logging = builder.logging;
        this.logLeakedSessions = builder.logLeakedSessions;

        this.idleTimeBeforeConnectionTest = builder.idleTimeBeforeConnectionTest;
        this.maxConnectionLifetimeMillis = builder.maxConnectionLifetimeMillis;
        this.maxIdleConnectionPoolSize = builder.maxIdleConnectionPoolSize;
        this.maxConnectionPoolSize = builder.maxConnectionPoolSize;
        this.connectionAcquisitionTimeoutMillis = builder.connectionAcquisitionTimeoutMillis;

        this.encrypted = builder.encrypted;
        this.trustStrategy = builder.trustStrategy;
        this.routingFailureLimit = builder.routingFailureLimit;
        this.routingRetryDelayMillis = builder.routingRetryDelayMillis;
        this.connectionTimeoutMillis = builder.connectionTimeoutMillis;
        this.retrySettings = builder.retrySettings;
        this.loadBalancingStrategy = builder.loadBalancingStrategy;
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
     * Check if leaked sessions logging is enabled.
     *
     * @return {@code true} if enabled, {@code false} otherwise.
     */
    public boolean logLeakedSessions()
    {
        return logLeakedSessions;
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
     * Pooled connections that have been idle in the pool for longer than this timeout
     * will be tested before they are used again, to ensure they are still live.
     *
     * @return idle time in milliseconds
     */
    public long idleTimeBeforeConnectionTest()
    {
        return idleTimeBeforeConnectionTest;
    }

    /**
     * Pooled connections older than this threshold will be closed and removed from the pool.
     *
     * @return maximum lifetime in milliseconds
     */
    public long maxConnectionLifetimeMillis()
    {
        return maxConnectionLifetimeMillis;
    }

    /**
     * @return the configured connection timeout value in milliseconds.
     */
    public int connectionTimeoutMillis()
    {
        return connectionTimeoutMillis;
    }

    public int maxConnectionPoolSize()
    {
        return maxConnectionPoolSize;
    }

    public long connectionAcquisitionTimeoutMillis()
    {
        return connectionAcquisitionTimeoutMillis;
    }

    /**
     * @return the level of encryption required for all connections.
     */
    @Deprecated
    public EncryptionLevel encryptionLevel()
    {
        return encrypted ? EncryptionLevel.REQUIRED : EncryptionLevel.NONE;
    }

    /**
     * @return indicator for encrypted communication.
     */
    public boolean encrypted()
    {
        return encrypted;
    }

    /**
     * @return the strategy to use to determine the authenticity of an encryption certificate provided by the Neo4j instance we are connecting to.
     */
    public TrustStrategy trustStrategy()
    {
        return trustStrategy;
    }

    /**
     * Load balancing strategy
     *
     * @return the strategy to use
     */
    @Experimental
    public LoadBalancingStrategy loadBalancingStrategy()
    {
        return loadBalancingStrategy;
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

    RetrySettings retrySettings()
    {
        return retrySettings;
    }

    /**
     * Used to build new config instances
     */
    public static class ConfigBuilder
    {
        private Logging logging = new JULogging( Level.INFO );
        private boolean logLeakedSessions;
        private int maxIdleConnectionPoolSize = PoolSettings.DEFAULT_MAX_IDLE_CONNECTION_POOL_SIZE;
        private int maxConnectionPoolSize = PoolSettings.DEFAULT_MAX_CONNECTION_POOL_SIZE;
        private long idleTimeBeforeConnectionTest = PoolSettings.DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST;
        private long maxConnectionLifetimeMillis = PoolSettings.DEFAULT_MAX_CONNECTION_LIFETIME;
        private long connectionAcquisitionTimeoutMillis = PoolSettings.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT;
        private boolean encrypted = true;
        private TrustStrategy trustStrategy = trustAllCertificates();
        private LoadBalancingStrategy loadBalancingStrategy = LoadBalancingStrategy.LEAST_CONNECTED;
        private int routingFailureLimit = 1;
        private long routingRetryDelayMillis = TimeUnit.SECONDS.toMillis( 5 );
        private int connectionTimeoutMillis = (int) TimeUnit.SECONDS.toMillis( 5 );
        private RetrySettings retrySettings = RetrySettings.DEFAULT;

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
         * Provide an alternative load balancing strategy for the routing driver to use. By default we use
         * {@link LoadBalancingStrategy#LEAST_CONNECTED}.
         * <p>
         * <b>Note:</b> We are experimenting with different strategies. This could be removed in the next minor version.
         *
         * @param loadBalancingStrategy the strategy to use
         * @return this builder
         */
        @Experimental
        public ConfigBuilder withLoadBalancingStrategy( LoadBalancingStrategy loadBalancingStrategy )
        {
            this.loadBalancingStrategy = loadBalancingStrategy;
            return this;
        }

        /**
         * Enable logging of leaked sessions.
         * <p>
         * Each {@link Session session} is associated with a network connection and thus is a
         * {@link Resource resource} that needs to be explicitly closed. Unclosed sessions will result in socket
         * leaks and could cause {@link OutOfMemoryError}s.
         * <p>
         * Session is considered to be leaked when it is finalized via {@link Object#finalize()} while not being
         * closed. This option turns on logging of such sessions and stacktraces of where they were created.
         * <p>
         * <b>Note:</b> this option should mostly be used in testing environments for session leak investigations.
         * Enabling it will add object finalization overhead.
         *
         * @return this builder
         */
        public ConfigBuilder withLeakedSessionsLogging()
        {
            this.logLeakedSessions = true;
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
         * Please use {@link #withConnectionLivenessCheckTimeout(long, TimeUnit)}.
         *
         * @param timeout minimum idle time in milliseconds
         * @return this builder
         * @see #withConnectionLivenessCheckTimeout(long, TimeUnit)
         * @deprecated please use {@link #withConnectionLivenessCheckTimeout(long, TimeUnit)} method. This method
         * will be removed in future release.
         */
        @Deprecated
        public ConfigBuilder withSessionLivenessCheckTimeout( long timeout )
        {
            withConnectionLivenessCheckTimeout( timeout, TimeUnit.MILLISECONDS );
            return this;
        }

        /**
         * Pooled connections that have been idle in the pool for longer than this timeout
         * will be tested before they are used again, to ensure they are still live.
         * <p>
         * If this option is set too low, an additional network call will be
         * incurred when acquiring a connection, which causes a performance hit.
         * <p>
         * If this is set high, you may receive sessions that are backed by no longer live connections,
         * which will lead to exceptions in your application. Assuming the
         * database is running, these exceptions will go away if you retry acquiring sessions.
         * <p>
         * Hence, this parameter tunes a balance between the likelihood of your
         * application seeing connection problems, and performance.
         * <p>
         * You normally should not need to tune this parameter.
         * No connection liveliness check is done by default.
         * Value {@code 0} means connections will always be tested for
         * validity and negative values mean connections will never be tested.
         *
         * @param value the minimum idle time
         * @param unit the unit in which the duration is given
         * @return this builder
         */
        public ConfigBuilder withConnectionLivenessCheckTimeout( long value, TimeUnit unit )
        {
            this.idleTimeBeforeConnectionTest = unit.toMillis( value );
            return this;
        }

        /**
         * Pooled connections older than this threshold will be closed and removed from the pool. Such discarding
         * happens during connection acquisition so that new session is never backed by an old connection.
         * <p>
         * Setting this option to a low value will cause a high connection churn and might result in a performance hit.
         * <p>
         * It is recommended to set maximum lifetime to a slightly smaller value than the one configured in network
         * equipment (load balancer, proxy, firewall, etc. can also limit maximum connection lifetime).
         * <p>
         * Setting can also be used in combination with {@link #withConnectionLivenessCheckTimeout(long, TimeUnit)}. In
         * this case, it is recommended to set liveness check to a value smaller than network equipment has and maximum
         * lifetime to a reasonably large value to "renew" connections once in a while.
         * <p>
         * No maximum lifetime limit is imposed by default. Zero and negative values result in lifetime not being
         * checked.
         *
         * @param value the maximum connection lifetime
         * @param unit the unit in which the duration is given
         * @return this builder
         */
        public ConfigBuilder withMaxConnectionLifetime( long value, TimeUnit unit )
        {
            this.maxConnectionLifetimeMillis = unit.toMillis( value );
            return this;
        }

        /**
         * Todo: doc and validation
         *
         * @param value
         * @return
         */
        public ConfigBuilder withMaxConnectionPoolSize( int value )
        {
            this.maxConnectionPoolSize = value;
            return this;
        }

        /**
         * Todo: doc and validation
         *
         * @param value
         * @param unit
         * @return
         */
        public ConfigBuilder withConnectionAcquisitionTimeout( long value, TimeUnit unit )
        {
            this.connectionAcquisitionTimeoutMillis = unit.toMillis( value );
            return this;
        }

        /**
         * Configure the {@link EncryptionLevel} to use, use this to control wether the driver uses TLS encryption or not.
         * @param level the TLS level to use
         * @return this builder
         */
        @Deprecated
        public ConfigBuilder withEncryptionLevel( EncryptionLevel level )
        {
            this.encrypted = level == EncryptionLevel.REQUIRED;
            return this;
        }

        /**
         * Set to use encrypted traffic.
         * @return this builder
         */
        public ConfigBuilder withEncryption()
        {
            this.encrypted = true;
            return this;
        }

        /**
         * Set to use unencrypted traffic.
         * @return this builder
         */
        public ConfigBuilder withoutEncryption()
        {
            this.encrypted = false;
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
         * @deprecated in 1.2 because driver memorizes seed URI used during construction and falls back to it at
         * runtime when all other known router servers failed to respond. Driver is also able to perform DNS lookup
         * for the seed URI during rediscovery. This means updates of cluster members will be picked up if they are
         * reflected in a DNS record. This configuration allowed driver to retry rediscovery procedure and postpone
         * failure. Currently there exists a better way of doing retries via
         * {@link Session#readTransaction(TransactionWork)} and {@link Session#writeTransaction(TransactionWork)}.
         * <b>Method will be removed in the next major release.</b>
         */
        @Deprecated
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
         * @deprecated in 1.2 because driver memorizes seed URI used during construction and falls back to it at
         * runtime when all other known router servers failed to respond. Driver is also able to perform DNS lookup
         * for the seed URI during rediscovery. This means updates of cluster members will be picked up if they are
         * reflected in a DNS record. This configuration allowed driver to retry rediscovery procedure and postpone
         * failure. Currently there exists a better way of doing retries via
         * {@link Session#readTransaction(TransactionWork)} and {@link Session#writeTransaction(TransactionWork)}.
         * <b>Method will be removed in the next major release.</b>
         */
        @Deprecated
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
         * Specify socket connection timeout.
         * <p>
         * A timeout of zero is treated as an infinite timeout and will be bound by the timeout configured on the
         * operating system level. The connection will block until established or an error occurs.
         * <p>
         * Timeout value should be greater or equal to zero and represent a valid {@code int} value when converted to
         * {@link TimeUnit#MILLISECONDS milliseconds}.
         * <p>
         * The default value of this parameter is {@code 5 SECONDS}.
         *
         * @param value the timeout duration
         * @param unit the unit in which duration is given
         * @return this builder
         * @throws IllegalArgumentException when given value is negative or does not fit in {@code int} when
         * converted to milliseconds.
         */
        public ConfigBuilder withConnectionTimeout( long value, TimeUnit unit )
        {
            long connectionTimeoutMillis = unit.toMillis( value );
            if ( connectionTimeoutMillis < 0 )
            {
                throw new IllegalArgumentException( String.format(
                        "The connection timeout may not be smaller than 0, but was %d %s.", value, unit ) );
            }
            int connectionTimeoutMillisInt = (int) connectionTimeoutMillis;
            if ( connectionTimeoutMillisInt != connectionTimeoutMillis )
            {
                throw new IllegalArgumentException( String.format(
                        "The connection timeout must represent int value when converted to milliseconds %d.",
                        connectionTimeoutMillis ) );
            }
            this.connectionTimeoutMillis = connectionTimeoutMillisInt;
            return this;
        }

        /**
         * Specify the maximum time transactions are allowed to retry via
         * {@link Session#readTransaction(TransactionWork)} and {@link Session#writeTransaction(TransactionWork)}
         * methods. These methods will retry the given unit of work on {@link ServiceUnavailableException},
         * {@link SessionExpiredException} and {@link TransientException} with exponential backoff using initial
         * delay of 1 second.
         * <p>
         * Default value is 30 seconds.
         *
         * @param value the timeout duration
         * @param unit the unit in which duration is given
         * @return this builder
         * @throws IllegalArgumentException when given value is negative
         */
        public ConfigBuilder withMaxTransactionRetryTime( long value, TimeUnit unit )
        {
            long maxRetryTimeMs = unit.toMillis( value );
            if ( maxRetryTimeMs < 0 )
            {
                throw new IllegalArgumentException( String.format(
                        "The max retry time may not be smaller than 0, but was %d %s.", value, unit ) );
            }
            this.retrySettings = new RetrySettings( maxRetryTimeMs );
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

    @Experimental
    public enum LoadBalancingStrategy
    {
        ROUND_ROBIN,
        LEAST_CONNECTED
    }

    /**
     * Control how the driver determines if it can trust the encryption certificates provided by the Neo4j instance it is connected to.
     */
    public static class TrustStrategy
    {
        /**
         * The trust strategy that the driver supports
         */
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
         *
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
         *
         * @param certFile the trusted certificate file
         * @return an authentication config
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
         * Trust strategy for certificates that can be verified through the local system store.
         *
         * @return an authentication config
         */
        public static TrustStrategy trustSystemCertificates()
        {
            return new TrustStrategy( Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES );
        }

        /**
         * Trust strategy for certificates that can be verified through the local system store.
         *
         * @return an authentication config
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
