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
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;

import org.neo4j.driver.internal.logging.JULogging;
import org.neo4j.driver.internal.spi.Logging;
import org.neo4j.driver.v1.util.Immutable;

import static java.lang.System.getProperty;
import static org.neo4j.driver.v1.Config.TrustStrategy.Strategy;
import static org.neo4j.driver.v1.Config.TrustStrategy.trustOnFirstUse;
import static org.neo4j.driver.v1.Config.TrustStrategy.trustSignedBy;

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
 *
 * @since 1.0
 */
@Immutable
public class Config
{
    /** User defined logging */
    private final Logging logging;

    /** The size of connection pool for each database url */
    private final int connectionPoolSize;

    /** Connections that have been idle longer than this threshold will have a ping test performed on them. */
    private final long idleTimeBeforeConnectionTest;

    /** Level of encryption we need to adhere to */
    private final EncryptionLevel encryptionLevel;

    /** Strategy for how to trust encryption certificate */
    private final TrustStrategy trustStrategy;

    private Config( ConfigBuilder builder )
    {
        this.logging = builder.logging;

        this.connectionPoolSize = builder.connectionPoolSize;
        this.idleTimeBeforeConnectionTest = builder.idleTimeBeforeConnectionTest;

        this.encryptionLevel = builder.encryptionLevel;
        this.trustStrategy = builder.trustStrategy;
    }

    /**
     * Logging provider
     *
     * @return the logging provider to use
     */
    public Logging logging()
    {
        return logging;
    }

    /**
     * Max number of connections per URL for this driver.
     *
     * @return the max number of connections
     */
    public int connectionPoolSize()
    {
        return connectionPoolSize;
    }

    /**
     * Pooled connections that have been unused for longer than this timeout will be tested before they are
     * used again, to ensure they are still live.
     *
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
     * @return the strategy to use to determine the authenticity of an encryption certificate provided by the Neo4j
     * instance we are connecting to.
     */
    public TrustStrategy trustStrategy()
    {
        return trustStrategy;
    }

    /**
     * Return a {@link ConfigBuilder} instance
     *
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
        private static final String CERT_FILE_KEY = "certFile";
        private final static File DEFAULT_KNOWN_HOSTS =
                new File( getProperty( "user.home" ), ".neo4j" + File.separator + "known_hosts" );
        private static final String TRUST_STRATEGY_KET = "trustStrategy";
        private static final String KNOWN_HOSTS_KEY = "knownHosts";
        private static final String MAX_SESSIONS_KEY = "maxSessions";
        private static final String ENCRYPTION_LEVEL_KEY = "encryptionLevel";
        private static final String SESSION_LIVENESS_CHECK_TIMEOUT_KEY = "sessionLivenessCheckTimeout";
        private Logging logging = new JULogging( Level.INFO );
        private int connectionPoolSize = 50;
        private long idleTimeBeforeConnectionTest = 200;
        private EncryptionLevel encryptionLevel = EncryptionLevel.REQUIRED;
        private TrustStrategy trustStrategy = trustOnFirstUse( DEFAULT_KNOWN_HOSTS );

        private ConfigBuilder()
        {
        }

        /**
         * Provide an alternative logging implementation for the driver to use. By default we use
         * java util logging.
         *
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
         * <p>
         * If the driver is asked to provide more sessions than this, it will
         * block waiting for another session to be closed, with a timeout.
         *
         * @param size the max number of sessions to keep open
         * @return this builder
         */
        public ConfigBuilder withMaxSessions( int size )
        {
            this.connectionPoolSize = size;
            return this;
        }

        /**
         * Pooled sessions that have been unused for longer than this timeout
         * will be tested before they are used again, to ensure they are still live.
         * <p>
         * If this option is set too low, an additional network call will be
         * incurred when acquiring a session, which causes a performance hit.
         * <p>
         * If this is set high, you may receive sessions that are no longer live,
         * which will lead to exceptions in your application. Assuming the
         * database is running, these exceptions will go away if you retry acquiring
         * sessions.
         * <p>
         * Hence, this parameter tunes a balance between the likelihood of your
         * application seeing connection problems, and performance.
         * <p>
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
         * Configure the {@link EncryptionLevel} to use, use this to control wether the driver uses TLS encryption or
         * not.
         *
         * @param level the TLS level to use
         * @return this builder
         */
        public ConfigBuilder withEncryptionLevel( EncryptionLevel level )
        {
            this.encryptionLevel = level;
            return this;
        }

        /**
         * Specify how to determine the authenticity of an encryption certificate provided by the Neo4j instance we
         * are connecting to.
         * This defaults to {@link TrustStrategy#trustOnFirstUse(File)}.
         * See {@link TrustStrategy#trustSignedBy(File)} for using certificate signatures instead to verify
         * trust.
         * <p>
         * This is an important setting to understand, because unless we know that the remote server we have an
         * encrypted connection to
         * is really Neo4j, there is no point to encrypt at all, since anyone could pretend to be the remote Neo4j
         * instance.
         * <p>
         * For this reason, there is no option to disable trust verification, if you find this cumbersome you should
         * disable encryption using
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
         * Updates a ConfigurationBuilder from a provided map.
         * <p>
         * The provided map can contain the following keys.
         * <ul>
         * <li><tt>trustStrategy</tt> can have values <tt>TRUST_ON_FIRST_USE</tt> or <tt>TRUST_CERTIFICATES
         * {@link #withTrustStrategy(TrustStrategy)}</tt></li>
         * <li><tt>certFile</tt> used together with <tt>TRUST_CERTIFICATES</tt> to configure the location of your
         * trusted certificate, {@link TrustStrategy#trustSignedBy(File)} (File)}.</li>
         * <li><tt>knownHosts</tt> used together with <tt>TRUST_ON_FIRST_USE</tt> to configure the location of your
         * known hosts file {@link TrustStrategy#trustOnFirstUse(File)}.</li>
         * <li><tt>encryptionLevel</tt> can have values <tt>NONE</tt> or <tt>REQUIRED</tt>, {@link
         * #withEncryptionLevel(EncryptionLevel)}</li>
         * <li><tt>maxSessions</tt> {@link #withMaxSessions(int)} </li>
         * <li><tt>maxSessions</tt> {@link #withSessionLivenessCheckTimeout(long)} </li>
         * </ul>
         *
         * @param map A map containing configuration options.
         * @return A ConfigBuilder updated with the provided configuration options.
         */
        public ConfigBuilder fromMap( Map<String,Object> map )
        {

            Strategy trustStrategy =
                    typeSafeGet( Strategy.class, map, TRUST_STRATEGY_KET, this.trustStrategy.strategy() );
            switch ( trustStrategy )
            {
            case TRUST_ON_FIRST_USE:
                withTrustStrategy( trustOnFirstUse( new File( typeSafeGet( String.class, map, KNOWN_HOSTS_KEY,
                        this.trustStrategy.certFile().getAbsolutePath() ) ) ) );

                break;
            case TRUST_SIGNED_CERTIFICATES:
                if ( !map.containsKey( CERT_FILE_KEY ) )
                {
                    throw new IllegalArgumentException(
                            "A 'certFile' must be configured when using 'TRUST_SIGNED_CERTIFICATES'. " +
                            "Please add a file path to your certificate file in your " +
                            "configuration map and try again." );
                }
                withTrustStrategy( trustSignedBy( new File( typeSafeGet( String.class, map, CERT_FILE_KEY,
                        this.trustStrategy.certFile().getAbsolutePath() ) ) ) );
                break;
            }

            withMaxSessions( typeSafeGet( Integer.class, map, MAX_SESSIONS_KEY, this.connectionPoolSize ) );
            withEncryptionLevel(
                    typeSafeGet( EncryptionLevel.class, map, ENCRYPTION_LEVEL_KEY, this.encryptionLevel ) );
            withSessionLivenessCheckTimeout( typeSafeGet( Long.class, map, SESSION_LIVENESS_CHECK_TIMEOUT_KEY,
                    this.idleTimeBeforeConnectionTest ) );

            return this;
        }

        /**
         * Create a config instance from this builder.
         *
         * @return a {@link Config} instance
         */
        public Config toConfig()
        {
            return new Config( this );
        }

        @SuppressWarnings( "unchecked" )
        private <T> T typeSafeGet( Class<T> clazz, Map<String,Object> map, String key, T defaultValue )
        {
            Object o = map.get( key );
            if ( o == null )
            {
                return defaultValue;
            }
            if ( clazz.isAssignableFrom( o.getClass() ) )
            {
                return clazz.cast( o );
            }
            else if ( clazz.isEnum() && o instanceof String )
            {
                try
                {
                    return clazz.cast( Enum.valueOf( (Class<Enum>) clazz, (String) o ) );
                }
                catch ( Exception e )
                {
                    T[] enumConstants = clazz.getEnumConstants();

                    throw new IllegalArgumentException( o + " is not a valid option for '" + key +
                                                        "'. Valid values are " + Arrays.toString( enumConstants ) );
                }
            }
            else
            {
                throw new IllegalArgumentException( "The value corresponding to '" + key + "' must have type " +
                                                    clazz.getSimpleName() );
            }
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
     * Control how the driver determines if it can trust the encryption certificates provided by the Neo4j instance
     * it is connected to.
     */
    public static class TrustStrategy
    {
        public enum Strategy
        {
            TRUST_ON_FIRST_USE,
            TRUST_SIGNED_CERTIFICATES
        }

        private final Strategy strategy;
        private final File certFile;

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
         * Only encrypted connections to Neo4j instances with certificates signed by a trusted certificate will be
         * accepted.
         * The file specified should contain one or more trusted X.509 certificates.
         * <p>
         * The certificate(s) in the file must be encoded using PEM encoding, meaning the certificates in the file
         * should be encoded using Base64,
         * and each certificate is bounded at the beginning by "-----BEGIN CERTIFICATE-----", and bounded at the end
         * by "-----END CERTIFICATE-----".
         *
         * @param certFile the trusted certificate file
         * @return an authentication config
         */
        public static TrustStrategy trustSignedBy( File certFile )
        {
            return new TrustStrategy( Strategy.TRUST_SIGNED_CERTIFICATES, certFile );
        }

        /**
         * Automatically trust a Neo4j instance the first time we see it - but fail to connect if its encryption
         * certificate ever changes.
         * This is similar to the mechanism used in SSH, and protects against man-in-the-middle attacks that occur
         * after the initial setup of your application.
         * <p>
         * Known Neo4j hosts are recorded in a file, {@code certFile}.
         * Each time we reconnect to a known host, we verify that its certificate remains the same, guarding against
         * attackers intercepting our communication.
         * <p>
         * Note that this approach is vulnerable to man-in-the-middle attacks the very first time you connect to a
         * new Neo4j instance.
         * If you do not trust the network you are connecting over, consider using
         * {@link #trustSignedBy(File) signed certificates} instead, or manually adding the
         * trusted host line into the specified file.
         *
         * @param knownHostsFile a file where known certificates are stored.
         * @return an authentication config
         */
        public static TrustStrategy trustOnFirstUse( File knownHostsFile )
        {
            return new TrustStrategy( Strategy.TRUST_ON_FIRST_USE, knownHostsFile );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            { return true; }
            if ( o == null || getClass() != o.getClass() )
            { return false; }

            TrustStrategy that = (TrustStrategy) o;

            if ( strategy != that.strategy )
            { return false; }
            return certFile != null ? certFile.equals( that.certFile ) : that.certFile == null;

        }

        @Override
        public int hashCode()
        {
            int result = strategy != null ? strategy.hashCode() : 0;
            result = 31 * result + (certFile != null ? certFile.hashCode() : 0);
            return result;
        }
    }
}
