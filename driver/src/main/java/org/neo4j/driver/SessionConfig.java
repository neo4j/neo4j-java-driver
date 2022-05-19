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
import static org.neo4j.driver.internal.handlers.pulln.FetchSizeUtil.assertValidFetchSize;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.reactive.ReactiveSession;
import org.neo4j.driver.reactive.RxSession;
import org.reactivestreams.Subscription;

/**
 * The session configurations used to configure a session.
 */
public class SessionConfig implements Serializable {
    private static final long serialVersionUID = 5773462156979050657L;

    private static final SessionConfig EMPTY = builder().build();

    private final Iterable<Bookmark> bookmarks;
    private final AccessMode defaultAccessMode;
    private final String database;
    private final Long fetchSize;
    private final String impersonatedUser;

    private SessionConfig(Builder builder) {
        this.bookmarks = builder.bookmarks;
        this.defaultAccessMode = builder.defaultAccessMode;
        this.database = builder.database;
        this.fetchSize = builder.fetchSize;
        this.impersonatedUser = builder.impersonatedUser;
    }

    /**
     * Creates a new {@link Builder} used to construct a configuration object.
     *
     * @return a session configuration builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns a static {@link SessionConfig} with default values for a general purpose session.
     *
     * @return a session config for a general purpose session.
     */
    public static SessionConfig defaultConfig() {
        return EMPTY;
    }

    /**
     * Returns a {@link SessionConfig} for the specified database
     * @param database the database the session binds to.
     * @return a session config for a session for the specified database.
     */
    public static SessionConfig forDatabase(String database) {
        return new Builder().withDatabase(database).build();
    }

    /**
     * Returns the initial bookmarks.
     * First transaction in the session created with this {@link SessionConfig}
     * will ensure that server hosting is at least as up-to-date as the
     * latest transaction referenced by the supplied initial bookmarks.
     *
     * @return the initial bookmarks.
     */
    public Iterable<Bookmark> bookmarks() {
        return bookmarks;
    }

    /**
     * The type of access required by units of work in this session,
     * e.g. {@link AccessMode#READ read access} or {@link AccessMode#WRITE write access}.
     *
     * @return the access mode.
     */
    public AccessMode defaultAccessMode() {
        return defaultAccessMode;
    }

    /**
     * The database where the session is going to connect to.
     *
     * @return the nullable database name where the session is going to connect to.
     */
    public Optional<String> database() {
        return Optional.ofNullable(database);
    }

    /**
     * This value if set, overrides the default fetch size set on {@link Config#fetchSize()}.
     *
     * @return an optional value of fetch size.
     */
    public Optional<Long> fetchSize() {
        return Optional.ofNullable(fetchSize);
    }

    /**
     * The impersonated user the session is going to use for query execution.
     *
     * @return an optional value of the impersonated user.
     */
    public Optional<String> impersonatedUser() {
        return Optional.ofNullable(impersonatedUser);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SessionConfig that = (SessionConfig) o;
        return Objects.equals(bookmarks, that.bookmarks)
                && defaultAccessMode == that.defaultAccessMode
                && Objects.equals(database, that.database)
                && Objects.equals(fetchSize, that.fetchSize)
                && Objects.equals(impersonatedUser, that.impersonatedUser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bookmarks, defaultAccessMode, database, impersonatedUser);
    }

    @Override
    public String toString() {
        return "SessionParameters{" + "bookmarks=" + bookmarks + ", defaultAccessMode=" + defaultAccessMode
                + ", database='" + database + '\'' + ", fetchSize=" + fetchSize + "impersonatedUser=" + impersonatedUser
                + '}';
    }

    /**
     * Builder used to configure {@link SessionConfig} which will be used to create a session.
     */
    public static class Builder {
        private Long fetchSize = null;
        private Iterable<Bookmark> bookmarks = null;
        private AccessMode defaultAccessMode = AccessMode.WRITE;
        private String database = null;
        private String impersonatedUser = null;

        private Builder() {}

        /**
         * Set the initial bookmarks to be used in a session.
         * <p>
         * First transaction in a session will ensure that server hosting is at least as up-to-date as the latest transaction referenced by the supplied
         * bookmarks. The bookmarks can be obtained via {@link Session#lastBookmarks()}, {@link AsyncSession#lastBookmarks()}, and/or {@link
         * ReactiveSession#lastBookmarks()}.
         *
         * @param bookmarks a series of initial bookmarks. Both {@code null} value and empty array are permitted, and indicate that the bookmarks do not exist
         *                  or are unknown.
         * @return this builder.
         */
        public Builder withBookmarks(Bookmark... bookmarks) {
            if (bookmarks == null) {
                this.bookmarks = null;
            } else {
                this.bookmarks = Arrays.asList(bookmarks);
            }
            return this;
        }

        /**
         * Set the initial bookmarks to be used in a session. First transaction in a session will ensure that server hosting is at least as up-to-date as the
         * latest transaction referenced by the supplied bookmarks. The bookmarks can be obtained via {@link Session#lastBookmarks()}, {@link
         * AsyncSession#lastBookmarks()}, and/or {@link ReactiveSession#lastBookmarks()}.
         * <p>
         * Multiple immutable sets of bookmarks may be joined in the following way:
         * <pre>
         * {@code
         * Set<Bookmark> bookmarks = new HashSet<>();
         * bookmarks.addAll( session1.lastBookmarks() );
         * bookmarks.addAll( session2.lastBookmarks() );
         * bookmarks.addAll( session3.lastBookmarks() );
         * }
         * </pre>
         *
         * @param bookmarks initial references to some previous transactions. Both {@code null} value and empty iterable are permitted, and indicate that the
         *                  bookmarks do not exist or are unknown.
         * @return this builder
         */
        public Builder withBookmarks(Iterable<Bookmark> bookmarks) {
            this.bookmarks = bookmarks;
            return this;
        }

        /**
         * Set the type of access required by units of work in this session,
         * e.g. {@link AccessMode#READ read access} or {@link AccessMode#WRITE write access}.
         * This access mode is used to route transactions in the session to the server who has the right to carry out the specified operations.
         *
         * @param mode access mode.
         * @return this builder.
         */
        public Builder withDefaultAccessMode(AccessMode mode) {
            this.defaultAccessMode = mode;
            return this;
        }

        /**
         * Set the database that the newly created session is going to connect to.
         * <p>
         * For connecting to servers that support multi-databases,
         * it is highly recommended to always set the database explicitly in the {@link SessionConfig} for each session.
         * If the database name is not set, then session defaults to connecting to the default database configured in server configuration.
         * <p>
         * For servers that do not support multi-databases, leave this database value unset. The only database will be used instead.
         *
         * @param database the database the session going to connect to. Provided value should not be {@code null}.
         * @return this builder.
         */
        public Builder withDatabase(String database) {
            requireNonNull(database, "Database name should not be null.");
            if (database.isEmpty()) {
                // Empty string is an illegal database name. Fail fast on client.
                throw new IllegalArgumentException(String.format("Illegal database name '%s'.", database));
            }
            this.database = database;
            return this;
        }

        /**
         * Specify how many records to fetch in each batch for this session.
         * This config will overrides the default value set on {@link Config#fetchSize()}.
         * This config is only valid when the driver is used with servers that support Bolt V4 (Server version 4.0 and later).
         *
         * Bolt V4 enables pulling records in batches to allow client to take control of data population and apply back pressure to server.
         * This config specifies the default fetch size for all query runs using {@link Session} and {@link AsyncSession}.
         * By default, the value is set to {@code 1000}.
         * Use {@code -1} to disables back pressure and config client to pull all records at once after each run.
         *
         * This config only applies to run result obtained via {@link Session} and {@link AsyncSession}.
         * As with {@link RxSession}, the batch size is provided via {@link Subscription#request(long)} instead.
         * @param size the default record fetch size when pulling records in batches using Bolt V4.
         * @return this builder
         */
        public Builder withFetchSize(long size) {
            this.fetchSize = assertValidFetchSize(size);
            return this;
        }

        /**
         * Set the impersonated user that the newly created session is going to use for query execution.
         * <p>
         * The principal provided to the driver on creation must have the necessary permissions to impersonate and run queries as the impersonated user.
         * <p>
         * When {@link #withDatabase(String)} is not used, the driver will discover the default database name of the impersonated user on first session usage.
         * From that moment, the discovered database name will be used as the default database name for the whole lifetime of the new session.
         * <p>
         * <b>Compatible with 4.4+ only.</b> You MUST have all servers running 4.4 version or above and communicating over Bolt 4.4 or above.
         *
         * @param impersonatedUser the user to impersonate. Provided value should not be {@code null}.
         * @return this builder
         */
        public Builder withImpersonatedUser(String impersonatedUser) {
            requireNonNull(impersonatedUser, "Impersonated user should not be null.");
            if (impersonatedUser.isEmpty()) {
                // Empty string is an illegal user. Fail fast on client.
                throw new IllegalArgumentException(String.format("Illegal impersonated user '%s'.", impersonatedUser));
            }
            this.impersonatedUser = impersonatedUser;
            return this;
        }

        public SessionConfig build() {
            return new SessionConfig(this);
        }
    }
}
