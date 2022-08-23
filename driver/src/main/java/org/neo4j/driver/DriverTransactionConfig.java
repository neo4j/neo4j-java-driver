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

public final class DriverTransactionConfig {
    private final String database;
    private final Set<Bookmark> bookmarks;
    private final String impersonatedUser;
    private final TransactionClusterMemberAccess clusterMemberAccess;
    private final Duration timeout;
    private final Map<String, Object> metadata;
    private final RetryStrategy retryStrategy;

    private DriverTransactionConfig(DriverTransactionConfigBuilder builder) {
        this.database = builder.database;
        this.bookmarks = builder.bookmarks;
        this.impersonatedUser = builder.impersonatedUser;
        this.clusterMemberAccess = builder.clusterMemberAccess;
        this.timeout = builder.timeout;
        this.metadata = builder.metadata;
        this.retryStrategy = builder.retryStrategy;
    }

    public static DriverTransactionConfigBuilder builder() {
        return new DriverTransactionConfigBuilder();
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

    public TransactionClusterMemberAccess clusterMemberAccess() {
        return clusterMemberAccess;
    }

    public Optional<Duration> timeout() {
        return Optional.ofNullable(timeout);
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public RetryStrategy retryStrategy() {
        return retryStrategy;
    }

    public static final class DriverTransactionConfigBuilder {
        private String database;
        private Set<Bookmark> bookmarks = Collections.emptySet();
        private String impersonatedUser;
        private TransactionClusterMemberAccess clusterMemberAccess = TransactionClusterMemberAccess.WRITERS;
        private Duration timeout;
        private Map<String, Object> metadata = Collections.emptyMap();
        private RetryStrategy retryStrategy = RetryStrategies.defaultStrategy(2);

        private DriverTransactionConfigBuilder() {}

        public DriverTransactionConfigBuilder withDatabase(String database) {
            this.database = database;
            return this;
        }

        public DriverTransactionConfigBuilder withBookmarks(Set<Bookmark> bookmarks) {
            Objects.requireNonNull(bookmarks, "bookmarks must not be null");
            this.bookmarks = bookmarks;
            return this;
        }

        public DriverTransactionConfigBuilder withImpersonatedUser(String impersonatedUser) {
            this.impersonatedUser = impersonatedUser;
            return this;
        }

        public DriverTransactionConfigBuilder withClusterMemberAccess(
                TransactionClusterMemberAccess clusterMemberAccess) {
            Objects.requireNonNull(clusterMemberAccess, "clusterMemberAccess must not be null");
            this.clusterMemberAccess = clusterMemberAccess;
            return this;
        }

        public DriverTransactionConfigBuilder withTimeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public DriverTransactionConfigBuilder withMetadata(Map<String, Object> metadata) {
            Objects.requireNonNull(metadata, "metadata must not be null");
            this.metadata = metadata;
            return this;
        }

        public DriverTransactionConfigBuilder withRetryStrategy(RetryStrategy retryStrategy) {
            Objects.requireNonNull(retryStrategy, "retryStrategy must not be null");
            this.retryStrategy = retryStrategy;
            return this;
        }

        public DriverTransactionConfig build() {
            return new DriverTransactionConfig(this);
        }
    }
}
