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
package org.neo4j.driver;

import static java.lang.String.format;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

import java.io.File;
import java.io.Serial;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.neo4j.driver.internal.SecuritySettings;
import org.neo4j.driver.internal.async.pool.PoolSettings;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.handlers.pulln.FetchSizeUtil;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.net.ServerAddressResolver;
import org.neo4j.driver.util.Experimental;
import org.neo4j.driver.util.Immutable;

/**
 * A configuration class to config driver properties.
 * <p>
 * To build a simple config with custom logging implementation:
 * <pre>
 * {@code
 * Config config = Config.builder()
 *                       .withLogging(new MyLogging())
 *                       .build();
 * }
 * </pre>
 * <p>
 * To build a more complicated config with tuned connection pool options:
 * <pre>
 * {@code
 * Config config = Config.builder()
 *                       .withEncryption()
 *                       .withConnectionTimeout( 10, TimeUnit.SECONDS)
 *                       .withMaxConnectionLifetime(30, TimeUnit.MINUTES)
 *                       .withMaxConnectionPoolSize(10)
 *                       .withConnectionAcquisitionTimeout(20, TimeUnit.SECONDS)
 *                       .build();
 * }
 * </pre>
 *
 * @since 1.0
 */
@Immutable
public final class Config implements Serializable {
    @Serial
    private static final long serialVersionUID = -4496545746399601108L;

    private static final Config EMPTY = builder().build();

    private final BookmarkManager queryBookmarkManager;

    /**
     * User defined logging
     */
    private final Logging logging;

    private final boolean logLeakedSessions;

    private final int maxConnectionPoolSize;

    private final long idleTimeBeforeConnectionTest;
    private final long maxConnectionLifetimeMillis;
    private final long connectionAcquisitionTimeoutMillis;

    private final SecuritySettings securitySettings;

    private final long fetchSize;
    private final long routingTablePurgeDelayMillis;

    private final int connectionTimeoutMillis;
    private final RetrySettings retrySettings;
    private final ServerAddressResolver resolver;

    private final int eventLoopThreads;
    private final String userAgent;
    private final MetricsAdapter metricsAdapter;

    private Config(ConfigBuilder builder) {
        this.queryBookmarkManager = builder.queryBookmarkManager;
        this.logging = builder.logging;
        this.logLeakedSessions = builder.logLeakedSessions;

        this.idleTimeBeforeConnectionTest = builder.idleTimeBeforeConnectionTest;
        this.maxConnectionLifetimeMillis = builder.maxConnectionLifetimeMillis;
        this.maxConnectionPoolSize = builder.maxConnectionPoolSize;
        this.connectionAcquisitionTimeoutMillis = builder.connectionAcquisitionTimeoutMillis;
        this.userAgent = builder.userAgent;

        this.securitySettings = builder.securitySettingsBuilder.build();

        this.connectionTimeoutMillis = builder.connectionTimeoutMillis;
        this.routingTablePurgeDelayMillis = builder.routingTablePurgeDelayMillis;
        this.retrySettings = builder.retrySettings;
        this.resolver = builder.resolver;
        this.fetchSize = builder.fetchSize;

        this.eventLoopThreads = builder.eventLoopThreads;
        this.metricsAdapter = builder.metricsAdapter;
    }

    /**
     * A {@link BookmarkManager} implementation for the driver to use on
     * {@link Driver#executeQuery(Query, QueryConfig)} method and its variants by default.
     * <p>
     * Please note that sessions will not use this automatically, but it is possible to enable it explicitly
     * using {@link SessionConfig.Builder#withBookmarkManager(BookmarkManager)}.
     *
     * @return bookmark manager, must not be {@code null}
     */
    public BookmarkManager queryBookmarkManager() {
        return queryBookmarkManager;
    }

    /**
     * Logging provider
     *
     * @return the Logging provider to use
     */
    public Logging logging() {
        return logging;
    }

    /**
     * Check if leaked sessions logging is enabled.
     *
     * @return {@code true} if enabled, {@code false} otherwise.
     */
    public boolean logLeakedSessions() {
        return logLeakedSessions;
    }

    /**
     * Pooled connections that have been idle in the pool for longer than this timeout
     * will be tested before they are used again, to ensure they are still live.
     *
     * @return idle time in milliseconds
     */
    public long idleTimeBeforeConnectionTest() {
        return idleTimeBeforeConnectionTest;
    }

    /**
     * Pooled connections older than this threshold will be closed and removed from the pool.
     *
     * @return maximum lifetime in milliseconds
     */
    public long maxConnectionLifetimeMillis() {
        return maxConnectionLifetimeMillis;
    }

    /**
     * @return the configured connection timeout value in milliseconds.
     */
    public int connectionTimeoutMillis() {
        return connectionTimeoutMillis;
    }

    public int maxConnectionPoolSize() {
        return maxConnectionPoolSize;
    }

    public long connectionAcquisitionTimeoutMillis() {
        return connectionAcquisitionTimeoutMillis;
    }

    /**
     * @return indicator for encrypted communication.
     */
    public boolean encrypted() {
        return securitySettings.encrypted();
    }

    /**
     * @return the strategy to use to determine the authenticity of an encryption certificate provided by the Neo4j instance we are connecting to.
     */
    public TrustStrategy trustStrategy() {
        return securitySettings.trustStrategy();
    }

    /**
     * Server address resolver.
     *
     * @return the resolver to use.
     */
    public ServerAddressResolver resolver() {
        return resolver;
    }

    /**
     * Start building a {@link Config} object using a newly created builder.
     *
     * @return a new {@link ConfigBuilder} instance.
     */
    public static ConfigBuilder builder() {
        return new ConfigBuilder();
    }

    /**
     * @return A config with all default settings
     */
    public static Config defaultConfig() {
        return EMPTY;
    }

    /**
     * @return the security setting to use when creating connections.
     */
    SecuritySettings securitySettings() {
        return securitySettings;
    }

    RoutingSettings routingSettings() {
        return new RoutingSettings(routingTablePurgeDelayMillis);
    }

    RetrySettings retrySettings() {
        return retrySettings;
    }

    public long fetchSize() {
        return fetchSize;
    }

    public int eventLoopThreads() {
        return eventLoopThreads;
    }

    /**
     * @return if the metrics is enabled or not on this driver.
     */
    public boolean isMetricsEnabled() {
        return this.metricsAdapter != MetricsAdapter.DEV_NULL;
    }

    public MetricsAdapter metricsAdapter() {
        return this.metricsAdapter;
    }

    /**
     * @return the user_agent configured for this driver
     */
    public String userAgent() {
        return userAgent;
    }

    /**
     * Used to build new config instances
     */
    public static final class ConfigBuilder {
        private BookmarkManager queryBookmarkManager =
                BookmarkManagers.defaultManager(BookmarkManagerConfig.builder().build());
        private Logging logging = DEV_NULL_LOGGING;
        private boolean logLeakedSessions;
        private int maxConnectionPoolSize = PoolSettings.DEFAULT_MAX_CONNECTION_POOL_SIZE;
        private long idleTimeBeforeConnectionTest = PoolSettings.DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST;
        private long maxConnectionLifetimeMillis = PoolSettings.DEFAULT_MAX_CONNECTION_LIFETIME;
        private long connectionAcquisitionTimeoutMillis = PoolSettings.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT;
        private String userAgent = format("neo4j-java/%s", driverVersion());
        private final SecuritySettings.SecuritySettingsBuilder securitySettingsBuilder =
                new SecuritySettings.SecuritySettingsBuilder();
        private long routingTablePurgeDelayMillis = RoutingSettings.DEFAULT.routingTablePurgeDelayMs();
        private int connectionTimeoutMillis = (int) TimeUnit.SECONDS.toMillis(30);
        private RetrySettings retrySettings = RetrySettings.DEFAULT;
        private ServerAddressResolver resolver;
        private MetricsAdapter metricsAdapter = MetricsAdapter.DEV_NULL;
        private long fetchSize = FetchSizeUtil.DEFAULT_FETCH_SIZE;
        private int eventLoopThreads = 0;

        private ConfigBuilder() {}

        /**
         * Sets a {@link BookmarkManager} implementation for the driver to use on
         * {@link Driver#executeQuery(Query, QueryConfig)} method and its variants by default.
         * <p>
         * Please note that sessions will not use this automatically, but it is possible to enable it explicitly
         * using {@link SessionConfig.Builder#withBookmarkManager(BookmarkManager)}.
         *
         * @param queryBookmarkManager bookmark manager, must not be {@code null}
         * @return this builder
         */
        public ConfigBuilder withQueryBookmarkManager(BookmarkManager queryBookmarkManager) {
            Objects.requireNonNull(queryBookmarkManager, "queryBookmarkManager must not be null");
            this.queryBookmarkManager = queryBookmarkManager;
            return this;
        }

        /**
         * Provide a logging implementation for the driver to use. Java logging framework {@link java.util.logging} with {@link Level#INFO} is used by default.
         * Callers are expected to either implement {@link Logging} interface or provide one of the existing implementations available from static factory
         * methods in the {@link Logging} interface.
         * <p>
         * Please see documentation in {@link Logging} for more information.
         *
         * @param logging the logging instance to use
         * @return this builder
         * @see Logging
         */
        public ConfigBuilder withLogging(Logging logging) {
            this.logging = logging;
            return this;
        }

        /**
         * Enable logging of leaked sessions.
         * <p>
         * Each {@link Session session} is associated with a network connection and thus is a
         * {@link org.neo4j.driver.util.Resource resource} that needs to be explicitly closed.
         * Unclosed sessions will result in socket leaks and could cause {@link OutOfMemoryError}s.
         * <p>
         * Session is considered to be leaked when it is finalized via {@link Object#finalize()} while not being
         * closed. This option turns on logging of such sessions and stacktraces of where they were created.
         * <p>
         * <b>Note:</b> this option should mostly be used in testing environments for session leak investigations.
         * Enabling it will add object finalization overhead.
         *
         * @return this builder
         */
        public ConfigBuilder withLeakedSessionsLogging() {
            this.logLeakedSessions = true;
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
         * @param unit  the unit in which the duration is given
         * @return this builder
         */
        public ConfigBuilder withConnectionLivenessCheckTimeout(long value, TimeUnit unit) {
            this.idleTimeBeforeConnectionTest = unit.toMillis(value);
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
         * Default maximum connection lifetime is 1 hour. Zero and negative values result in lifetime not being
         * checked.
         *
         * @param value the maximum connection lifetime
         * @param unit  the unit in which the duration is given
         * @return this builder
         */
        public ConfigBuilder withMaxConnectionLifetime(long value, TimeUnit unit) {
            this.maxConnectionLifetimeMillis = unit.toMillis(value);
            return this;
        }

        /**
         * Configure maximum amount of connections in the connection pool towards a single database. This setting
         * limits total amount of connections in the pool when used in direct driver, created for URI with 'bolt'
         * scheme. It will limit amount of connections per cluster member when used with routing driver, created for
         * URI with 'neo4j' scheme.
         * <p>
         * Acquisition will be attempted for at most configured timeout
         * {@link #withConnectionAcquisitionTimeout(long, TimeUnit)} when limit is reached.
         * <p>
         * Default value is {@code 100}. Negative values are allowed and result in unlimited pool. Value of {@code 0}
         * is not allowed.
         *
         * @param value the maximum connection pool size.
         * @return this builder
         * @see #withConnectionAcquisitionTimeout(long, TimeUnit)
         */
        public ConfigBuilder withMaxConnectionPoolSize(int value) {
            if (value == 0) {
                throw new IllegalArgumentException("Zero value is not supported");
            } else if (value < 0) {
                this.maxConnectionPoolSize = Integer.MAX_VALUE;
            } else {
                this.maxConnectionPoolSize = value;
            }
            return this;
        }

        /**
         * Configure maximum amount of time connection acquisition will attempt to acquire a connection from the
         * connection pool. This timeout only kicks in when all existing connections are being used and no new
         * connections can be created because maximum connection pool size has been reached.
         * <p>
         * Exception is raised when connection can't be acquired within configured time.
         * <p>
         * Default value is 60 seconds. Negative values are allowed and result in unlimited acquisition timeout. Value
         * of {@code 0} is allowed and results in no timeout and immediate failure when connection is unavailable.
         *
         * @param value the acquisition timeout
         * @param unit  the unit in which the duration is given
         * @return this builder
         * @see #withMaxConnectionPoolSize(int)
         */
        public ConfigBuilder withConnectionAcquisitionTimeout(long value, TimeUnit unit) {
            long valueInMillis = unit.toMillis(value);
            if (value >= 0) {
                this.connectionAcquisitionTimeoutMillis = valueInMillis;
            } else {
                this.connectionAcquisitionTimeoutMillis = -1;
            }
            return this;
        }

        /**
         * Set to use encrypted traffic.
         *
         * @return this builder
         */
        public ConfigBuilder withEncryption() {
            securitySettingsBuilder.withEncryption();
            return this;
        }

        /**
         * Set to use unencrypted traffic.
         *
         * @return this builder
         */
        public ConfigBuilder withoutEncryption() {
            securitySettingsBuilder.withoutEncryption();
            return this;
        }

        /**
         * Specify how to determine the authenticity of an encryption certificate provided by the Neo4j instance we are connecting to. This defaults to {@link
         * TrustStrategy#trustSystemCertificates()}. See {@link TrustStrategy#trustCustomCertificateSignedBy(File...)} for using certificate signatures instead to
         * verify trust.
         * <p>
         * This is an important setting to understand, because unless we know that the remote server we have an encrypted connection to is really Neo4j, there
         * is no point to encrypt at all, since anyone could pretend to be the remote Neo4j instance.
         * <p>
         * For this reason, there is no option to disable trust verification. However, it is possible to turn off encryption using the {@link
         * ConfigBuilder#withoutEncryption()} option.
         *
         * @param trustStrategy TLS authentication strategy
         * @return this builder
         */
        public ConfigBuilder withTrustStrategy(TrustStrategy trustStrategy) {
            securitySettingsBuilder.withTrustStrategy(trustStrategy);
            return this;
        }

        /**
         * Specify how long to wait before purging stale routing tables.
         * <p>
         * When a routing table is timed out, the routing table will be marked ready to remove after the delay specified here.
         * Driver keeps a routing table for each database seen by the driver.
         * The routing table of a database get refreshed if the database is used frequently.
         * If the database is not used for a long time,
         * the driver use the timeout specified here to purge the stale routing table.
         * <p>
         * After a routing table is removed, next time when using the database of the purged routing table,
         * the driver will fall back to use seed URI for a new routing table.
         *
         * @param delay the amount of time to wait before purging routing tables
         * @param unit  the unit in which the duration is given
         * @return this builder
         */
        public ConfigBuilder withRoutingTablePurgeDelay(long delay, TimeUnit unit) {
            long routingTablePurgeDelayMillis = unit.toMillis(delay);
            if (routingTablePurgeDelayMillis < 0) {
                throw new IllegalArgumentException(String.format(
                        "The routing table purge delay may not be smaller than 0, but was %d %s.", delay, unit));
            }
            this.routingTablePurgeDelayMillis = routingTablePurgeDelayMillis;
            return this;
        }

        /**
         * Specify how many records to fetch in each batch.
         * This config is only valid when the driver is used with servers that support Bolt V4 (Server version 4.0 and later).
         * <p>
         * Bolt V4 enables pulling records in batches to allow client to take control of data population and apply back pressure to server.
         * This config specifies the default fetch size for all query runs using {@link Session} and {@link org.neo4j.driver.async.AsyncSession}.
         * By default, the value is set to {@code 1000}.
         * Use {@code -1} to disables back pressure and config client to pull all records at once after each run.
         * <p>
         * This config only applies to run result obtained via {@link Session} and {@link org.neo4j.driver.async.AsyncSession}.
         * As with {@link org.neo4j.driver.reactive.RxSession}, the batch size is provided via
         * {@link org.reactivestreams.Subscription#request(long)} instead.
         *
         * @param size the default record fetch size when pulling records in batches using Bolt V4.
         * @return this builder
         */
        public ConfigBuilder withFetchSize(long size) {
            this.fetchSize = FetchSizeUtil.assertValidFetchSize(size);
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
         * The default value of this parameter is {@code 30 SECONDS}.
         *
         * @param value the timeout duration
         * @param unit  the unit in which duration is given
         * @return this builder
         * @throws IllegalArgumentException when given value is negative or does not fit in {@code int} when
         *                                  converted to milliseconds.
         */
        public ConfigBuilder withConnectionTimeout(long value, TimeUnit unit) {
            long connectionTimeoutMillis = unit.toMillis(value);
            if (connectionTimeoutMillis < 0) {
                throw new IllegalArgumentException(
                        String.format("The connection timeout may not be smaller than 0, but was %d %s.", value, unit));
            }
            int connectionTimeoutMillisInt = (int) connectionTimeoutMillis;
            if (connectionTimeoutMillisInt != connectionTimeoutMillis) {
                throw new IllegalArgumentException(String.format(
                        "The connection timeout must represent int value when converted to milliseconds %d.",
                        connectionTimeoutMillis));
            }
            this.connectionTimeoutMillis = connectionTimeoutMillisInt;
            return this;
        }

        /**
         * Specify the maximum time transactions are allowed to retry via
         * {@link Session#readTransaction(TransactionWork)} and {@link Session#writeTransaction(TransactionWork)}
         * methods. These methods will retry the given unit of work on {@link org.neo4j.driver.exceptions.ServiceUnavailableException},
         * {@link org.neo4j.driver.exceptions.SessionExpiredException} and {@link org.neo4j.driver.exceptions.TransientException} with
         * exponential backoff using initial delay of 1 second.
         * <p>
         * Default value is 30 seconds.
         *
         * @param value the timeout duration
         * @param unit  the unit in which duration is given
         * @return this builder
         * @throws IllegalArgumentException when given value is negative
         */
        public ConfigBuilder withMaxTransactionRetryTime(long value, TimeUnit unit) {
            long maxRetryTimeMs = unit.toMillis(value);
            if (maxRetryTimeMs < 0) {
                throw new IllegalArgumentException(
                        String.format("The max retry time may not be smaller than 0, but was %d %s.", value, unit));
            }
            this.retrySettings = new RetrySettings(maxRetryTimeMs);
            return this;
        }

        /**
         * Specify a custom server address resolver used by the routing driver to resolve the initial address used to create the driver.
         * Such resolution happens:
         * <ul>
         * <li>during the very first rediscovery when driver is created</li>
         * <li>when all the known routers from the current routing table have failed and driver needs to fallback to the initial address</li>
         * </ul>
         * By default driver performs a DNS lookup for the initial address using {@link InetAddress#getAllByName(String)}.
         *
         * @param resolver the resolver to use.
         * @return this builder.
         * @throws NullPointerException when the given resolver is {@code null}.
         */
        public ConfigBuilder withResolver(ServerAddressResolver resolver) {
            this.resolver = Objects.requireNonNull(resolver, "resolver");
            return this;
        }

        /**
         * Enable driver metrics backed by internal basic implementation. The metrics can be obtained afterwards via {@link Driver#metrics()}.
         *
         * @return this builder.
         */
        public ConfigBuilder withDriverMetrics() {
            return withMetricsEnabled(true);
        }

        /**
         * Disable driver metrics. When disabled, driver metrics cannot be accessed via {@link Driver#metrics()}.
         *
         * @return this builder.
         */
        public ConfigBuilder withoutDriverMetrics() {
            return withMetricsEnabled(false);
        }

        private ConfigBuilder withMetricsEnabled(boolean enabled) {
            if (!enabled) {
                withMetricsAdapter(MetricsAdapter.DEV_NULL);
            } else if (this.metricsAdapter == null || this.metricsAdapter == MetricsAdapter.DEV_NULL) {
                withMetricsAdapter(MetricsAdapter.DEFAULT);
            }
            return this;
        }

        /**
         * Enable driver metrics with given {@link MetricsAdapter}.
         * <p>
         * {@link MetricsAdapter#MICROMETER} enables implementation based on <a href="https://micrometer.io">Micrometer</a>. The metrics can be obtained
         * afterwards via Micrometer means and {@link Driver#metrics()}. Micrometer must be on classpath when using this option.
         *
         * @param metricsAdapter the metrics adapter to use. Use {@link MetricsAdapter#DEV_NULL} to disable metrics.
         * @return this builder.
         */
        @Experimental
        public ConfigBuilder withMetricsAdapter(MetricsAdapter metricsAdapter) {
            this.metricsAdapter = Objects.requireNonNull(metricsAdapter, "metricsAdapter");
            return this;
        }

        /**
         * Configure the event loop thread count. This specifies how many threads the driver can use to handle network I/O events
         * and user's events in driver's I/O threads. By default, 2 * NumberOfProcessors amount of threads will be used instead.
         *
         * @param size the thread count.
         * @return this builder.
         * @throws IllegalArgumentException if the value of the size is set to a number that is less than 1.
         */
        public ConfigBuilder withEventLoopThreads(int size) {
            if (size < 1) {
                throw new IllegalArgumentException(
                        String.format("The event loop thread may not be smaller than 1, but was %d.", size));
            }
            this.eventLoopThreads = size;
            return this;
        }

        /**
         * Configure the user_agent field sent to the server to identify the connected client.
         *
         * @param userAgent the string to configure user_agent.
         * @return this builder.
         */
        public ConfigBuilder withUserAgent(String userAgent) {
            if (userAgent == null || userAgent.isEmpty()) {
                throw new IllegalArgumentException("The user_agent string must not be empty");
            }
            this.userAgent = userAgent;
            return this;
        }

        /**
         * Extracts the driver version from the driver jar MANIFEST.MF file.
         */
        private static String driverVersion() {
            // "Session" is arbitrary - the only thing that matters is that the class we use here is in the
            // 'org.neo4j.driver' package, because that is where the jar manifest specifies the version.
            // This is done as part of the build, adding a MANIFEST.MF file to the generated jarfile.
            Package pkg = Session.class.getPackage();
            if (pkg != null && pkg.getImplementationVersion() != null) {
                return pkg.getImplementationVersion();
            }

            // If there is no version, we're not running from a jar file, but from raw compiled class files.
            // This should only happen during development, so call the version 'dev'.
            return "dev";
        }

        /**
         * Create a config instance from this builder.
         *
         * @return a new {@link Config} instance.
         */
        public Config build() {
            return new Config(this);
        }
    }

    /**
     * Control how the driver determines if it can trust the encryption certificates provided by the Neo4j instance it is connected to.
     */
    public static final class TrustStrategy implements Serializable {
        @Serial
        private static final long serialVersionUID = -1631888096243987740L;

        /**
         * The trust strategy that the driver supports
         */
        public enum Strategy {
            TRUST_ALL_CERTIFICATES,
            TRUST_CUSTOM_CA_SIGNED_CERTIFICATES,
            TRUST_SYSTEM_CA_SIGNED_CERTIFICATES
        }

        private final Strategy strategy;
        private final List<File> certFiles;
        private boolean hostnameVerificationEnabled = true;
        private RevocationCheckingStrategy revocationCheckingStrategy = RevocationCheckingStrategy.NO_CHECKS;

        private TrustStrategy(Strategy strategy) {
            this(strategy, Collections.emptyList());
        }

        private TrustStrategy(Strategy strategy, List<File> certFiles) {
            Objects.requireNonNull(certFiles, "certFiles can't be null");
            this.strategy = strategy;
            this.certFiles = Collections.unmodifiableList(new ArrayList<>(certFiles));
        }

        /**
         * Return the strategy type desired.
         *
         * @return the strategy we should use
         */
        public Strategy strategy() {
            return strategy;
        }

        /**
         * Return the configured certificate file.
         *
         * @return configured certificate or {@code null} if trust strategy does not require a certificate.
         * @deprecated superseded by {@link TrustStrategy#certFiles()}
         */
        @Deprecated
        public File certFile() {
            return certFiles.isEmpty() ? null : certFiles.get(0);
        }

        /**
         * Return the configured certificate files.
         *
         * @return configured certificate files or empty list if trust strategy does not require certificates.
         */
        public List<File> certFiles() {
            return certFiles;
        }

        /**
         * Check if hostname verification is enabled for this trust strategy.
         *
         * @return {@code true} if hostname verification has been enabled via {@link #withHostnameVerification()}, {@code false} otherwise.
         */
        public boolean isHostnameVerificationEnabled() {
            return hostnameVerificationEnabled;
        }

        /**
         * Enable hostname verification for this trust strategy.
         *
         * @return the current trust strategy.
         */
        public TrustStrategy withHostnameVerification() {
            hostnameVerificationEnabled = true;
            return this;
        }

        /**
         * Disable hostname verification for this trust strategy.
         *
         * @return the current trust strategy.
         */
        public TrustStrategy withoutHostnameVerification() {
            hostnameVerificationEnabled = false;
            return this;
        }

        /**
         * Only encrypted connections to Neo4j instances with certificates signed by a trusted certificate will be accepted. The file(s) specified should
         * contain one or more trusted X.509 certificates.
         * <p>
         * The certificate(s) in the file(s) must be encoded using PEM encoding, meaning the certificates in the file(s) should be encoded using Base64, and
         * each certificate is bounded at the beginning by "-----BEGIN CERTIFICATE-----", and bounded at the end by "-----END CERTIFICATE-----".
         *
         * @param certFiles the trusted certificate files, it must not be {@code null} or empty
         * @return an authentication config
         */
        public static TrustStrategy trustCustomCertificateSignedBy(File... certFiles) {
            Objects.requireNonNull(certFiles, "certFiles can't be null");
            if (certFiles.length == 0) {
                throw new IllegalArgumentException("certFiles can't be empty");
            }
            return new TrustStrategy(Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES, Arrays.asList(certFiles));
        }

        /**
         * Trust strategy for certificates that can be verified through the local system store.
         *
         * @return an authentication config
         */
        public static TrustStrategy trustSystemCertificates() {
            return new TrustStrategy(Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES);
        }

        /**
         * Trust strategy for certificates that trust all certificates blindly. Suggested to only use this in tests.
         * <p>
         * This trust strategy comes with hostname verification turned off by default since driver version 5.0.
         *
         * @return an authentication config
         * @since 1.1
         */
        public static TrustStrategy trustAllCertificates() {
            return new TrustStrategy(Strategy.TRUST_ALL_CERTIFICATES).withoutHostnameVerification();
        }

        /**
         * The revocation strategy used for verifying certificates.
         *
         * @return this {@link TrustStrategy}'s revocation strategy
         */
        public RevocationCheckingStrategy revocationCheckingStrategy() {
            return revocationCheckingStrategy;
        }

        /**
         * Configures the {@link TrustStrategy} to not carry out OCSP revocation checks on certificates. This is the
         * option that is configured by default.
         *
         * @return the current trust strategy
         */
        public TrustStrategy withoutCertificateRevocationChecks() {
            this.revocationCheckingStrategy = RevocationCheckingStrategy.NO_CHECKS;
            return this;
        }

        /**
         * Configures the {@link TrustStrategy} to carry out OCSP revocation checks when the revocation status is
         * stapled to the certificate. If no stapled response is found, then certificate verification continues
         * (and does not fail verification). This setting also requires the server to be configured to enable
         * OCSP stapling.
         *
         * @return the current trust strategy
         */
        public TrustStrategy withVerifyIfPresentRevocationChecks() {
            this.revocationCheckingStrategy = RevocationCheckingStrategy.VERIFY_IF_PRESENT;
            return this;
        }

        /**
         * Configures the {@link TrustStrategy} to carry out strict OCSP revocation checks for revocation status that
         * are stapled to the certificate. If no stapled response is found, then the driver will fail certificate verification
         * and not connect to the server. This setting also requires the server to be configured to enable OCSP stapling.
         * <p>
         * Note: enabling this setting will prevent the driver connecting to the server when the server is unable to reach
         * the certificate's configured OCSP responder URL.
         *
         * @return the current trust strategy
         */
        public TrustStrategy withStrictRevocationChecks() {
            this.revocationCheckingStrategy = RevocationCheckingStrategy.STRICT;
            return this;
        }
    }
}
