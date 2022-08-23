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

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class DriverQueryConfig {
    private final String database;
    private final Set<Bookmark> bookmarks;
    private final String impersonatedUser;
    private final ClusterMemberAccess clusterMemberAccess;
    private final Duration timeout;
    private final Map<String, Object> metadata;
    private final boolean explicitTransaction;
    private final boolean skipRecords;
    private final long maxRecordCount;
    private final RetryStrategy retryStrategy;

    private DriverQueryConfig(DriverQueryConfigBuilder builder) {
        this.database = builder.database;
        this.bookmarks = builder.bookmarks;
        this.impersonatedUser = builder.impersonatedUser;
        this.clusterMemberAccess = builder.clusterMemberAccess;
        this.timeout = builder.timeout;
        this.metadata = builder.metadata;
        this.explicitTransaction = builder.explicitTransaction;
        this.skipRecords = builder.skipRecords;
        this.maxRecordCount = builder.maxRecordCount;
        this.retryStrategy = builder.retryStrategy;
    }

    public static DriverQueryConfigBuilder builder() {
        return new DriverQueryConfigBuilder();
    }

    public Optional<String> database() {
        return Optional.ofNullable(database);
    }

    public Set<Bookmark> bookmarks() {
        return bookmarks;
    }

    public Optional<String> impersonatedUser() {
        return Optional.ofNullable(impersonatedUser);
    }

    public ClusterMemberAccess clusterMemberAccess() {
        return clusterMemberAccess;
    }

    public Optional<Duration> timeout() {
        return Optional.ofNullable(timeout);
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public boolean explicitTransaction() {
        return explicitTransaction;
    }

    public long maxRecordCount() {
        return maxRecordCount;
    }

    public boolean skipRecords() {
        return skipRecords;
    }

    public RetryStrategy retryStrategy() {
        return retryStrategy;
    }

    public static final class DriverQueryConfigBuilder {
        private String database;
        private Set<Bookmark> bookmarks = Collections.emptySet();
        private String impersonatedUser;
        private ClusterMemberAccess clusterMemberAccess = ClusterMemberAccess.WRITERS;
        private Duration timeout;
        private Map<String, Object> metadata = Collections.emptyMap();
        private boolean explicitTransaction = true;
        private boolean skipRecords = false;
        private long maxRecordCount = 1000;
        private RetryStrategy retryStrategy = RetryStrategies.defaultStrategy(2);

        private DriverQueryConfigBuilder() {}

        public DriverQueryConfigBuilder withDatabase(String database) {
            this.database = database;
            return this;
        }

        public DriverQueryConfigBuilder withBookmarks(Set<Bookmark> bookmarks) {
            Objects.requireNonNull(bookmarks, "bookmarks must not be null");
            this.bookmarks = bookmarks;
            return this;
        }

        public DriverQueryConfigBuilder withImpersonatedUser(String impersonatedUser) {
            this.impersonatedUser = impersonatedUser;
            return this;
        }

        public DriverQueryConfigBuilder withClusterMemberAccess(ClusterMemberAccess clusterMemberAccess) {
            Objects.requireNonNull(clusterMemberAccess, "clusterMemberAccess must not be null");
            this.clusterMemberAccess = clusterMemberAccess;
            return this;
        }

        public DriverQueryConfigBuilder withTimeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public DriverQueryConfigBuilder withMetadata(Map<String, Object> metadata) {
            Objects.requireNonNull(metadata, "metadata must not be null");
            this.metadata = metadata;
            return this;
        }

        public DriverQueryConfigBuilder withExplicitTransaction(boolean explicitTransaction) {
            this.explicitTransaction = explicitTransaction;
            return this;
        }

        public DriverQueryConfigBuilder withSkipRecords(boolean skipRecords) {
            this.skipRecords = skipRecords;
            return this;
        }

        public DriverQueryConfigBuilder withMaxRecordCount(long maxRecordCount) {
            this.maxRecordCount = maxRecordCount;
            return this;
        }

        public DriverQueryConfigBuilder withRetryStrategy(RetryStrategy retryStrategy) {
            Objects.requireNonNull(retryStrategy, "retryStrategy must not be null");
            this.retryStrategy = retryStrategy;
            return this;
        }

        public DriverQueryConfig build() {
            return new DriverQueryConfig(this);
        }
    }
}
