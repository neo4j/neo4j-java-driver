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

import static java.util.Objects.requireNonNull;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import org.neo4j.driver.internal.EagerResultTransformer;

/**
 * Query configuration used by {@link Driver#executeQuery(Query, QueryConfig)} and its variants.
 *
 * @param <T> result type that is defined by {@link ResultTransformer}
 */
public final class QueryConfig<T> implements Serializable {
    @Serial
    private static final long serialVersionUID = -2632780731598141754L;

    private static final QueryConfig<EagerResult> DEFAULT_VALUE = builder().build();

    private final RoutingControl routing;
    private final ResultTransformer<T> resultTransformer;
    private final String database;
    private final String impersonatedUser;
    private final BookmarkManager bookmarkManager;
    private final boolean useDefaultBookmarkManager;

    /**
     * Returns default config value.
     *
     * @return config value
     */
    public static QueryConfig<EagerResult> defaultConfig() {
        return DEFAULT_VALUE;
    }

    private QueryConfig(Builder<T> builder) {
        this.routing = builder.routing;
        this.resultTransformer = builder.resultTransformer;
        this.database = builder.database;
        this.impersonatedUser = builder.impersonatedUser;
        this.bookmarkManager = builder.bookmarkManager;
        this.useDefaultBookmarkManager = builder.useDefaultBookmarkManager;
    }

    /**
     * Creates a new {@link Builder} used to construct a configuration object with default {@link ResultTransformer}
     * implementation returning {@link EagerResult}.
     *
     * @return a query configuration builder
     */
    public static Builder<EagerResult> builder() {
        return new Builder<>(new EagerResultTransformer());
    }

    /**
     * Creates a new {@link Builder} used to construct a configuration object with custom {@link ResultTransformer}
     * implementation.
     *
     * @return a query configuration builder
     */
    public static <T> Builder<T> builder(ResultTransformer<T> resultTransformer) {
        requireNonNull(resultTransformer, "resultTransformer must not be null");
        return new Builder<>(resultTransformer);
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
     * Returns result mapper for the query.
     *
     * @return result mapper
     */
    public ResultTransformer<T> resultTransformer() {
        return resultTransformer;
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
     * {@link Config#queryBookmarkManager()} as a default value by the driver
     * @return bookmark manager
     */
    public Optional<BookmarkManager> bookmarkManager(BookmarkManager defaultBookmarkManager) {
        requireNonNull(defaultBookmarkManager, "defaultBookmarkManager must not be null");
        return useDefaultBookmarkManager ? Optional.of(defaultBookmarkManager) : Optional.ofNullable(bookmarkManager);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryConfig<?> that = (QueryConfig<?>) o;
        return useDefaultBookmarkManager == that.useDefaultBookmarkManager
                && routing == that.routing
                && resultTransformer.equals(that.resultTransformer)
                && Objects.equals(database, that.database)
                && Objects.equals(impersonatedUser, that.impersonatedUser)
                && Objects.equals(bookmarkManager, that.bookmarkManager);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                routing, resultTransformer, database, impersonatedUser, bookmarkManager, useDefaultBookmarkManager);
    }

    @Override
    public String toString() {
        return "QueryConfig{" + "routing="
                + routing + ", resultTransformer="
                + resultTransformer + ", database='"
                + database + '\'' + ", impersonatedUser='"
                + impersonatedUser + '\'' + ", bookmarkManager="
                + bookmarkManager + ", useDefaultBookmarkManager="
                + useDefaultBookmarkManager + '}';
    }

    /**
     * Builder used to configure {@link QueryConfig} which will be used to execute a query.
     */
    public static final class Builder<T> {
        private RoutingControl routing = RoutingControl.WRITERS;
        private final ResultTransformer<T> resultTransformer;
        private String database;
        private String impersonatedUser;
        private BookmarkManager bookmarkManager;
        private boolean useDefaultBookmarkManager = true;

        private Builder(ResultTransformer<T> resultTransformer) {
            this.resultTransformer = resultTransformer;
        }

        /**
         * Set routing mode for the query.
         *
         * @param routing routing mode
         * @return this builder
         */
        public Builder<T> withRouting(RoutingControl routing) {
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
        public Builder<T> withDatabase(String database) {
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
        public Builder<T> withImpersonatedUser(String impersonatedUser) {
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
        public Builder<T> withBookmarkManager(BookmarkManager bookmarkManager) {
            useDefaultBookmarkManager = false;
            this.bookmarkManager = bookmarkManager;
            return this;
        }

        /**
         * Create a config instance from this builder.
         *
         * @return a new {@link QueryConfig} instance.
         */
        public QueryConfig<T> build() {
            return new QueryConfig<>(this);
        }
    }
}
