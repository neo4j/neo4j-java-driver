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
package org.neo4j.driver;

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.util.Preconditions.checkArgument;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.neo4j.driver.internal.util.Extract;

/**
 * Query configuration used by {@link Driver#executableQuery(String)} and its variants.
 * @since 5.5
 */
public final class QueryConfig implements Serializable {
    @Serial
    private static final long serialVersionUID = -2632780731598141754L;

    private static final QueryConfig DEFAULT = builder().build();

    /**
     * The routing mode.
     */
    private final RoutingControl routing;
    /**
     * The target database.
     */
    private final String database;
    /**
     * The impersonated user.
     */
    private final String impersonatedUser;
    /**
     * The bookmark manager.
     */
    private final BookmarkManager bookmarkManager;
    /**
     * The flag indicating if default bookmark manager should be used.
     */
    private final boolean useDefaultBookmarkManager;
    /**
     * The transaction timeout.
     * @since 5.16
     */
    private final Duration timeout;
    /**
     * The transaction metadata.
     * @since 5.16
     */
    @SuppressWarnings("serial")
    private final Map<String, Serializable> metadata;

    /**
     * Returns default config value.
     *
     * @return config value
     */
    public static QueryConfig defaultConfig() {
        return DEFAULT;
    }

    private QueryConfig(Builder builder) {
        this.routing = builder.routing;
        this.database = builder.database;
        this.impersonatedUser = builder.impersonatedUser;
        this.bookmarkManager = builder.bookmarkManager;
        this.useDefaultBookmarkManager = builder.useDefaultBookmarkManager;
        this.timeout = builder.timeout;
        this.metadata = builder.metadata;
    }

    /**
     * Creates a new {@link Builder} used to construct a configuration object with default implementation returning
     * {@link EagerResult}.
     *
     * @return a query configuration builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns routing mode for the query.
     *
     * @return routing mode
     */
    public RoutingControl routing() {
        return routing;
    }

    /**
     * Returns target database for the query.
     *
     * @return target database
     */
    public Optional<String> database() {
        return Optional.ofNullable(database);
    }

    /**
     * Returns impersonated user for the query.
     *
     * @return impersonated user
     */
    public Optional<String> impersonatedUser() {
        return Optional.ofNullable(impersonatedUser);
    }

    /**
     * Returns bookmark manager for the query.
     *
     * @param defaultBookmarkManager default bookmark manager to use when none has been configured explicitly,
     * @return bookmark manager
     */
    public Optional<BookmarkManager> bookmarkManager(BookmarkManager defaultBookmarkManager) {
        requireNonNull(defaultBookmarkManager, "defaultBookmarkManager must not be null");
        return useDefaultBookmarkManager ? Optional.of(defaultBookmarkManager) : Optional.ofNullable(bookmarkManager);
    }

    /**
     * Get the configured transaction timeout.
     *
     * @return an {@link Optional} containing the configured timeout or {@link Optional#empty()} otherwise
     * @since 5.16
     */
    public Optional<Duration> timeout() {
        return Optional.ofNullable(timeout);
    }

    /**
     * Get the configured transaction metadata.
     *
     * @return metadata or empty map when it is not configured
     * @since 5.16
     */
    public Map<String, Serializable> metadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var that = (QueryConfig) o;
        return useDefaultBookmarkManager == that.useDefaultBookmarkManager
                && routing == that.routing
                && Objects.equals(database, that.database)
                && Objects.equals(impersonatedUser, that.impersonatedUser)
                && Objects.equals(bookmarkManager, that.bookmarkManager)
                && Objects.equals(timeout, that.timeout)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                routing, database, impersonatedUser, bookmarkManager, useDefaultBookmarkManager, timeout, metadata);
    }

    @Override
    public String toString() {
        return "QueryConfig{" + "routing="
                + routing + ", database='"
                + database + '\'' + ", impersonatedUser='"
                + impersonatedUser + '\'' + ", bookmarkManager="
                + bookmarkManager + ", useDefaultBookmarkManager="
                + useDefaultBookmarkManager + '\'' + ", timeout='"
                + timeout + '\'' + ", metadata="
                + metadata + '}';
    }

    /**
     * Builder used to configure {@link QueryConfig} which will be used to execute a query.
     */
    public static final class Builder {
        private RoutingControl routing = RoutingControl.WRITE;
        private String database;
        private String impersonatedUser;
        private BookmarkManager bookmarkManager;
        private boolean useDefaultBookmarkManager = true;
        private Duration timeout;
        private Map<String, Serializable> metadata = Collections.emptyMap();

        private Builder() {}

        /**
         * Set routing mode for the query.
         *
         * @param routing routing mode
         * @return this builder
         */
        public Builder withRouting(RoutingControl routing) {
            requireNonNull(routing, "routing must not be null");
            this.routing = routing;
            return this;
        }

        /**
         * Set target database for the query.
         *
         * @param database database
         * @return this builder
         */
        public Builder withDatabase(String database) {
            requireNonNull(database, "database must not be null");
            if (database.isEmpty()) {
                // Empty string is an illegal database
                throw new IllegalArgumentException(String.format("Illegal database '%s'", database));
            }
            this.database = database;
            return this;
        }

        /**
         * Set impersonated user for the query.
         *
         * @param impersonatedUser impersonated user
         * @return this builder
         */
        public Builder withImpersonatedUser(String impersonatedUser) {
            requireNonNull(impersonatedUser, "impersonatedUser must not be null");
            if (impersonatedUser.isEmpty()) {
                // Empty string is an illegal user
                throw new IllegalArgumentException(String.format("Illegal impersonated user '%s'", impersonatedUser));
            }
            this.impersonatedUser = impersonatedUser;
            return this;
        }

        /**
         * Set bookmark manager for the query.
         *
         * @param bookmarkManager bookmark manager
         * @return this builder
         */
        public Builder withBookmarkManager(BookmarkManager bookmarkManager) {
            useDefaultBookmarkManager = false;
            this.bookmarkManager = bookmarkManager;
            return this;
        }

        /**
         * Set the transaction timeout. Transactions that execute longer than the configured timeout will be terminated by the database.
         * <p>
         * This functionality allows user code to limit query/transaction execution time.
         * The specified timeout overrides the default timeout configured in the database using the {@code db.transaction.timeout} setting ({@code dbms.transaction.timeout} before Neo4j 5.0).
         * Values higher than {@code db.transaction.timeout} will be ignored and will fall back to the default for server versions between 4.2 and 5.2 (inclusive).
         * <p>
         * The provided value should not represent a negative duration.
         * {@link Duration#ZERO} will make the transaction execute indefinitely.
         *
         * @param timeout the timeout.
         * @return this builder.
         * @since 5.16
         */
        public Builder withTimeout(Duration timeout) {
            if (timeout != null) {
                checkArgument(!timeout.isNegative(), "Transaction timeout should not be negative");
            }

            this.timeout = timeout;
            return this;
        }

        /**
         * Set the transaction metadata.
         *
         * @param metadata the metadata, must not be {@code null}.
         * @return this builder.
         * @since 5.16
         */
        public Builder withMetadata(Map<String, Serializable> metadata) {
            requireNonNull(metadata, "Metadata should not be null");
            metadata.values()
                    .forEach(Extract::assertParameter); // Just assert valid parameters but don't create a value map yet
            this.metadata = Map.copyOf(metadata); // Create a defensive copy
            return this;
        }

        /**
         * Create a config instance from this builder.
         *
         * @return a new {@link QueryConfig} instance.
         */
        public QueryConfig build() {
            return new QueryConfig(this);
        }
    }
}
