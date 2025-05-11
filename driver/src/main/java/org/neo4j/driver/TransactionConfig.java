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

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.util.Preconditions.checkArgument;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.internal.util.Extract;

/**
 * Configuration object containing settings for transactions.
 * Instances are immutable and can be reused for multiple transactions.
 * <p>
 * Configuration is supported for:
 * <ul>
 * <li>queries executed in auto-commit transactions - using various overloads of {@link Session#run(String, TransactionConfig)} and
 * {@link AsyncSession#runAsync(String, TransactionConfig)}</li>
 * <li>transactions started by transaction functions - using {@link Session#executeWrite(TransactionCallback, TransactionConfig)},
 * {@link Session#executeRead(TransactionCallback, TransactionConfig)} and the other similar variants</li>
 * <li>unmanaged transactions - using {@link Session#beginTransaction(TransactionConfig)} and {@link AsyncSession#beginTransactionAsync(TransactionConfig)}</li>
 * </ul>
 * <p>
 * Creation of configuration objects can be done using the builder API:
 * <pre>
 * {@code
 * Map<String, Object> metadata = new HashMap<>();
 * metadata.put("type", "update user");
 * metadata.put("application", "my application");
 *
 * TransactionConfig config = TransactionConfig.builder()
 *                 .withTimeout(Duration.ofSeconds(4))
 *                 .withMetadata(metadata)
 *                 .build();
 * }
 * </pre>
 *
 * @see Session
 */
public final class TransactionConfig implements Serializable {
    @Serial
    private static final long serialVersionUID = -7954949878657177280L;

    private static final TransactionConfig EMPTY = builder().build();

    /**
     * the transaction timeout
     */
    private final Duration timeout;
    /**
     * The transaction metadata.
     */
    @SuppressWarnings("serial")
    private final Map<String, Object> metadata;

    // Values are not serializable, hence, we keep a transient volatile map of them around
    private transient volatile Map<String, Value> convertedMetadata;

    private TransactionConfig(Builder builder) {
        this.timeout = builder.timeout;
        this.metadata = builder.metadata;
    }

    /**
     * Get a configuration object that does not have any values configures.
     *
     * @return an empty configuration object.
     */
    public static TransactionConfig empty() {
        return EMPTY;
    }

    /**
     * Create new {@link Builder} used to construct a configuration object.
     *
     * @return new builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Get the configured transaction timeout.
     *
     * @return timeout or {@code null} when it is not configured.
     */
    public Duration timeout() {
        return timeout;
    }

    /**
     * Get the configured transaction metadata.
     *
     * @return metadata or empty map when it is not configured.
     */
    public Map<String, Value> metadata() {
        var result = this.convertedMetadata;
        if (result == null) {
            synchronized (this) {
                result = this.convertedMetadata;
                if (result == null) {
                    this.convertedMetadata = unmodifiableMap(Extract.mapOfValues(this.metadata));
                    result = this.convertedMetadata;
                }
            }
        }
        return result;
    }

    /**
     * Check if this configuration object contains any values.
     *
     * @return {@code true} when no values are configured, {@code false otherwise}.
     */
    public boolean isEmpty() {
        return timeout == null && (metadata == null || metadata.isEmpty());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (TransactionConfig) o;
        return Objects.equals(timeout, that.timeout) && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeout, metadata);
    }

    @Override
    public String toString() {
        return "TransactionConfig{" + "timeout=" + timeout + ", metadata=" + metadata + '}';
    }

    /**
     * Builder used to construct {@link TransactionConfig transaction configuration} objects.
     */
    public static final class Builder {
        private Duration timeout;
        private Map<String, Object> metadata = emptyMap();

        private Builder() {}

        /**
         * Set the transaction timeout. Transactions that execute longer than the configured timeout will be terminated by the database.
         * See also {@link #withDefaultTimeout}.
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
         */
        public Builder withTimeout(Duration timeout) {
            requireNonNull(timeout, "Transaction timeout should not be null");
            checkArgument(!timeout.isNegative(), "Transaction timeout should not be negative");

            this.timeout = timeout;
            return this;
        }

        /**
         * Set the transaction timeout to the server-side configured default timeout. This is the default behaviour if
         * {@link #withTimeout} has not been called.
         * See also {@link #withTimeout}.
         *
         * @return this builder.
         */
        public Builder withDefaultTimeout() {
            this.timeout = null;
            return this;
        }

        /**
         * Set the transaction metadata. Specified metadata will be attached to the executing transaction and visible in the output of
         * {@code dbms.listQueries} and {@code dbms.listTransactions} procedures. It will also get logged to the {@code query.log}.
         * <p>
         * This functionality makes it easier to tag transactions and is equivalent to {@code dbms.setTXMetaData} procedure.
         * <p>
         * Provided value should not be {@code null}.
         *
         * @param metadata the metadata.
         * @return this builder.
         */
        public Builder withMetadata(Map<String, ?> metadata) {
            requireNonNull(metadata, "Transaction metadata should not be null");
            metadata.values()
                    .forEach(Extract::assertParameter); // Just assert valid parameters but don't create a value map yet
            this.metadata = new HashMap<>(metadata); // Create a defensive copy
            return this;
        }

        /**
         * Build the transaction configuration object using the specified settings.
         *
         * @return new transaction configuration object.
         */
        public TransactionConfig build() {
            return new TransactionConfig(this);
        }
    }
}
