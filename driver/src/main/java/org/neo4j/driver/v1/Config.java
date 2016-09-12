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
import java.util.logging.Level;

import org.neo4j.driver.internal.logging.JULogging;
import org.neo4j.driver.internal.net.pooling.PoolSettings;
import org.neo4j.driver.v1.exceptions.ConnectionFailureException;
import org.neo4j.driver.v1.util.Immutable;

import static java.lang.System.getProperty;
import static org.neo4j.driver.v1.Config.TrustStrategy.trustOnFirstUse;

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

    /** The size of connection pool for each database url */
    private final int connectionPoolSize;

    private final int maxIdleConnectionPoolSize;

    /** Connections that have been idle longer than this threshold will have a ping test performed on them. */
    private final long idleTimeBeforeConnectionTest;

    /** Level of encryption we need to adhere to */
    private final EncryptionLevel encryptionLevel;

    /** Strategy for how to trust encryption certificate */
    private final TrustStrategy trustStrategy;

    private final int minServersInCluster;
    private final int readRetries;

    private Config( ConfigBuilder builder)
    {
        this.logging = builder.logging;

        this.connectionPoolSize = builder.connectionPoolSize;
        this.maxIdleConnectionPoolSize = builder.maxIdleConnectionPoolSize;
        this.idleTimeBeforeConnectionTest = builder.idleTimeBeforeConnectionTest;

        this.encryptionLevel = builder.encryptionLevel;
        this.trustStrategy = builder.trustStrategy;
        this.minServersInCluster = builder.minServersInCluster;
        this.readRetries = builder.readRetries;
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
     * @return idle time in milliseconds
     */
    public long idleTimeBeforeConnectionTest()
    {
        return idleTimeBeforeConnectionTest;
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
     * @return the number of retries to be attempted for read sessions
     */
    public int maximumReadRetriesForCluster()
    {
        return readRetries;
    }

    /**
     * @return the minimum number of servers the driver should know about.
     */
    public int minimumKnownClusterSize()
    {
        return minServersInCluster;
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

    /**
     * Used to build new config instances
     */
    public static class ConfigBuilder
    {
        private Logging logging = new JULogging( Level.INFO );
        private int connectionPoolSize = 50;
        private int maxIdleConnectionPoolSize = PoolSettings.DEFAULT_MAX_IDLE_CONNECTION_POOL_SIZE;
        private long idleTimeBeforeConnectionTest = PoolSettings.DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST;
        private EncryptionLevel encryptionLevel = EncryptionLevel.REQUIRED_NON_LOCAL;
        private TrustStrategy trustStrategy = trustOnFirstUse(
                new File( getProperty( "user.home" ), ".neo4j" + File.separator + "known_hosts" ) );
        public int minServersInCluster = 3;
        public int readRetries = 3;

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
            this.connectionPoolSize = size;
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
         */
        public ConfigBuilder withSessionLivenessCheckTimeout( long timeout )
        {
            this.idleTimeBeforeConnectionTest = timeout;
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
         * For read queries the driver can do automatic retries upon server failures,
         *
         * This setting specifies how many retries that should be attempted before giving up
         * and throw a {@link ConnectionFailureException}. If not specified this setting defaults to 3 retries before
         * giving up.
         * @param retries The number or retries to attempt before giving up.
         * @return this builder
         */
        public ConfigBuilder withMaximumReadRetriesForCluster( int retries )
        {
            this.readRetries = retries;
            return this;
        }

        /**
         * Specifies the minimum numbers in a cluster a driver should know about.
         * <p>
         * Once the number of servers drops below this threshold, the driver will automatically trigger a discovery
         * event
         * asking the servers for more members.
         *
         * @param minNumberOfServers the minimum number of servers the driver should know about
         * @return this builder
         */
        public ConfigBuilder withMinimumKnownClusterSize( int minNumberOfServers )
        {
            this.minServersInCluster = minNumberOfServers;
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

        /** With this level, the driver will only connect to the server without encryption if local but with
         * encryption otherwise. */
        REQUIRED_NON_LOCAL,

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
            TRUST_ON_FIRST_USE,
            @Deprecated
            TRUST_SIGNED_CERTIFICATES,
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

        public static TrustStrategy trustSystemCertificates()
        {
            return new TrustStrategy( Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES );
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
         */
        public static TrustStrategy trustOnFirstUse( File knownHostsFile )
        {
            return new TrustStrategy( Strategy.TRUST_ON_FIRST_USE, knownHostsFile );
        }
    }
}
