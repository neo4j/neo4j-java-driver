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

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.exceptions.UnsupportedFeatureException;
import org.neo4j.driver.internal.InternalNotificationConfig;
import org.neo4j.driver.reactive.ReactiveSession;
import org.neo4j.driver.util.Preview;

/**
 * The session configurations used to configure a session.
 */
public final class SessionConfig implements Serializable {
    @Serial
    private static final long serialVersionUID = 5773462156979050657L;

    private static final SessionConfig EMPTY = builder().build();

    /**
     * The initial bookmarks.
     */
    @SuppressWarnings("serial")
    private final Iterable<Bookmark> bookmarks;
    /**
     * The default type of access.
     */
    private final AccessMode defaultAccessMode;
    /**
     * The target database name.
     */
    private final String database;
    /**
     * The fetch size.
     */
    private final Long fetchSize;
    /**
     * The impersonated user.
     */
    private final String impersonatedUser;
    /**
     * The bookmark manager.
     */
    private final BookmarkManager bookmarkManager;
    /**
     * The notification config.
     */
    private final NotificationConfig notificationConfig;

    private SessionConfig(Builder builder) {
        this.bookmarks = builder.bookmarks;
        this.defaultAccessMode = builder.defaultAccessMode;
        this.database = builder.database;
        this.fetchSize = builder.fetchSize;
        this.impersonatedUser = builder.impersonatedUser;
        this.bookmarkManager = builder.bookmarkManager;
        this.notificationConfig = builder.notificationConfig;
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

    /**
     * A {@link BookmarkManager} implementation for the session to use.
     *
     * @return bookmark implementation
     */
    public Optional<BookmarkManager> bookmarkManager() {
        return Optional.ofNullable(bookmarkManager);
    }

    /**
     * Returns notification config.
     * @return the notification config
     * @since 5.7
     */
    public NotificationConfig notificationConfig() {
        return notificationConfig;
    }

    /**
     * Returns a minimum notification severity.
     *
     * @return an {@link Optional} of minimum {@link NotificationSeverity} or an empty {@link Optional} if it is not set
     * @since 5.22.0
     */
    @Preview(name = "GQL-status object")
    public Optional<NotificationSeverity> minimumNotificationSeverity() {
        return Optional.ofNullable(((InternalNotificationConfig) notificationConfig).minimumSeverity());
    }

    /**
     * Returns a set of disabled notification classifications.
     * @return the {@link Set} of disabled {@link NotificationClassification}
     * @since 5.22.0
     */
    @Preview(name = "GQL-status object")
    public Set<NotificationClassification> disabledNotificationClassifications() {
        var disabledCategories = ((InternalNotificationConfig) notificationConfig).disabledCategories();
        return disabledCategories != null
                ? ((InternalNotificationConfig) notificationConfig)
                        .disabledCategories().stream()
                                .map(NotificationClassification.class::cast)
                                .collect(Collectors.toUnmodifiableSet())
                : Collections.emptySet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (SessionConfig) o;
        return Objects.equals(bookmarks, that.bookmarks)
                && defaultAccessMode == that.defaultAccessMode
                && Objects.equals(database, that.database)
                && Objects.equals(fetchSize, that.fetchSize)
                && Objects.equals(impersonatedUser, that.impersonatedUser)
                && Objects.equals(bookmarkManager, that.bookmarkManager)
                && Objects.equals(notificationConfig, that.notificationConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bookmarks, defaultAccessMode, database, impersonatedUser, bookmarkManager);
    }

    @Override
    public String toString() {
        return String.format(
                """
                SessionParameters{bookmarks=%s, defaultAccessMode=%s, database='%s', fetchSize=%d, impersonatedUser=%s, \
                bookmarkManager=%s}\
                """,
                bookmarks, defaultAccessMode, database, fetchSize, impersonatedUser, bookmarkManager);
    }

    /**
     * Builder used to configure {@link SessionConfig} which will be used to create a session.
     */
    public static final class Builder {
        private Long fetchSize = null;
        private Iterable<Bookmark> bookmarks = null;
        private AccessMode defaultAccessMode = AccessMode.WRITE;
        private String database = null;
        private String impersonatedUser = null;
        private BookmarkManager bookmarkManager;

        private NotificationConfig notificationConfig = NotificationConfig.defaultConfig();

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
                this.bookmarks = Arrays.stream(bookmarks).toList();
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
            if (bookmarks != null) {
                this.bookmarks =
                        StreamSupport.stream(bookmarks.spliterator(), false).toList();
            }
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
         * Sets target database name for queries executed within session.
         * <p>
         * This option has no explicit value by default, as such it is recommended to set a value if the target database
         * is known in advance. This has the benefit of ensuring a consistent target database name throughout the
         * session in a straightforward way and potentially simplifies driver logic, which reduces network communication
         * and might result in better performance.
         * <p>
         * Cypher clauses such as USE are not a replacement for this option as Cypher is handled by the server and not
         * the driver.
         * <p>
         * When no explicit name is set, the driver behavior depends on the connection URI scheme supplied to the driver
         * on instantiation and Bolt protocol version.
         * <p>
         * Specifically, the following applies:
         * <ul>
         * <li><b>bolt schemes</b> - queries are dispatched to the server for execution without explicit database name
         * supplied, meaning that the target database name for query execution is determined by the server. It is
         * important to note that the target database may change (even within the same session), for instance if the
         * user's home database is changed on the server.</li>
         * <li><b>neo4j schemes</b> - providing that Bolt protocol version 4.4, which was introduced with Neo4j server
         * 4.4, or above is available, the driver fetches the user's home database name from the server on first query
         * execution within the session and uses the fetched database name explicitly for all queries executed within
         * the session. This ensures that the database name remains consistent within the given session. For instance,
         * if the user's home database name is 'movies' and the server supplies it to the driver upon database name
         * fetching for the session, all queries within that session are executed with the explicit database name
         * 'movies' supplied. Any change to the user’s home database is reflected only in sessions created after such
         * change takes effect. This behavior requires additional network communication. In clustered environments, it
         * is strongly recommended to avoid a single point of failure. For instance, by ensuring that the connection URI
         * resolves to multiple endpoints. For older Bolt protocol versions the behavior is the same as described for
         * the <b>bolt</b> schemes above.</li>
         * </ul>
         *
         * @param database the target database name, must not be {@code null}
         * @return this builder
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
         * <p>
         * Bolt V4 enables pulling records in batches to allow client to take control of data population and apply back pressure to server.
         * This config specifies the default fetch size for all query runs using {@link Session} and {@link AsyncSession}.
         * By default, the value is set to {@code 1000}.
         * Use {@code -1} to disables back pressure and config client to pull all records at once after each run.
         * <p>
         * This config only applies to run result obtained via {@link Session} and {@link AsyncSession}.
         * As with the reactive sessions the batch size is managed by the subscription requests instead.
         *
         * @param size the default record fetch size when pulling records in batches using Bolt V4.
         * @return this builder
         */
        public Builder withFetchSize(long size) {
            if (size <= 0 && size != -1) {
                throw new IllegalArgumentException(String.format(
                        "The record fetch size may not be 0 or negative. Illegal record fetch size: %s.", size));
            }
            this.fetchSize = size;
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

        /**
         * Sets a {@link BookmarkManager} implementation for the session to use.
         * <p>
         * By default, bookmark manager is effectively disabled.
         *
         * @param bookmarkManager bookmark manager implementation. Providing {@code null} effectively disables bookmark manager.
         * @return this builder.
         */
        public Builder withBookmarkManager(BookmarkManager bookmarkManager) {
            this.bookmarkManager = bookmarkManager;
            return this;
        }

        /**
         * Sets notification config.
         * <p>
         * Any configuration other than the {@link NotificationConfig#defaultConfig()} requires a minimum Bolt protocol
         * version 5.2. Otherwise, an {@link UnsupportedFeatureException} will be emitted when the driver comes across a
         * Bolt connection that does not support this feature. For instance, when running a query.
         *
         * @param notificationConfig the notification config
         * @return this builder
         * @since 5.7
         */
        public Builder withNotificationConfig(NotificationConfig notificationConfig) {
            this.notificationConfig = Objects.requireNonNull(notificationConfig, "notificationConfig must not be null");
            return this;
        }

        /**
         * Sets a minimum severity for notifications produced by the server.
         *
         * @param minimumNotificationSeverity the minimum notification severity
         * @return this builder
         * @since 5.22.0
         */
        @Preview(name = "GQL-status object")
        public Builder withMinimumNotificationSeverity(NotificationSeverity minimumNotificationSeverity) {
            if (minimumNotificationSeverity == null) {
                notificationConfig = NotificationConfig.disableAllConfig();
            } else {
                notificationConfig = notificationConfig.enableMinimumSeverity(minimumNotificationSeverity);
            }
            return this;
        }

        /**
         * Sets a set of disabled classifications for notifications produced by the server.
         *
         * @param disabledNotificationClassifications the set of disabled notification classifications
         * @return this builder
         * @since 5.22.0
         */
        @Preview(name = "GQL-status object")
        public Builder withDisabledNotificationClassifications(
                Set<NotificationClassification> disabledNotificationClassifications) {
            var disabledCategories = disabledNotificationClassifications == null
                    ? Collections.<NotificationCategory>emptySet()
                    : disabledNotificationClassifications.stream()
                            .map(NotificationCategory.class::cast)
                            .collect(Collectors.toSet());
            notificationConfig = notificationConfig.disableCategories(disabledCategories);
            return this;
        }

        /**
         * Builds the {@link SessionConfig}.
         * @return the config
         */
        public SessionConfig build() {
            return new SessionConfig(this);
        }
    }
}
