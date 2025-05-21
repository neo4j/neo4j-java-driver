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

import java.util.concurrent.CompletionStage;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.UnsupportedFeatureException;

/**
 * Accessor for a specific Neo4j graph database.
 * <p>
 * Driver implementations are typically thread-safe, act as a template
 * for session creation and host a connection pool. All configuration
 * and authentication settings are held immutably by the Driver. Should
 * different settings be required, a new Driver instance should be created.
 * <p>
 * A driver maintains a connection pool for each remote Neo4j server. Therefore,
 * the most efficient way to make use of a Driver is to use the same instance
 * across the application.
 * <p>
 * To construct a new Driver, use one of the
 * {@link GraphDatabase#driver(String, AuthToken) GraphDatabase.driver} methods.
 * The <a href="https://tools.ietf.org/html/rfc3986">URI</a> passed to
 * this method determines the type of Driver created.
 * <br>
 * <table border="1" style="border-collapse: collapse">
 *     <caption>Available schemes and drivers</caption>
 *     <thead>
 *         <tr><th>URI Scheme</th><th>Driver</th></tr>
 *     </thead>
 *     <tbody>
 *         <tr>
 *             <td><code>bolt</code></td>
 *             <td>Direct driver: connects directly to the host and port specified in the URI.</td>
 *         </tr>
 *         <tr>
 *             <td><code>neo4j</code></td>
 *             <td>Routing driver: can automatically discover members of a Causal Cluster and route {@link Session sessions} based on {@link AccessMode}.</td>
 *         </tr>
 *     </tbody>
 * </table>
 */
public interface Driver extends AutoCloseable {
    /**
     * Creates a new {@link ExecutableQuery} instance that executes a query in a managed transaction with automatic retries on
     * retryable errors.
     *
     * @param query query string
     * @return new executable query instance
     * @since 5.7
     */
    ExecutableQuery executableQuery(String query);

    /**
     * Returns an instance of {@link BookmarkManager} used by {@link ExecutableQuery} instances by default.
     *
     * @return bookmark manager, must not be {@code null}
     * @since 5.7
     */
    BookmarkManager executableQueryBookmarkManager();

    /**
     * Return a flag to indicate whether encryption is used for this driver.
     *
     * @return true if the driver requires encryption, false otherwise
     */
    boolean isEncrypted();

    /**
     * Create a new general purpose {@link Session} with default {@link SessionConfig session configuration}.
     * <p>
     * Alias to {@link #session(SessionConfig)}}.
     *
     * @return a new {@link Session} object.
     */
    default Session session() {
        return session(Session.class);
    }

    /**
     * Instantiate a new {@link Session} with a specified {@link SessionConfig session configuration}.
     * Use {@link SessionConfig#forDatabase(String)} to obtain a general purpose session configuration for the specified database.
     *
     * @param sessionConfig specifies session configurations for this session.
     * @return a new {@link Session} object.
     * @see SessionConfig
     */
    default Session session(SessionConfig sessionConfig) {
        return session(Session.class, sessionConfig);
    }

    /**
     * Instantiate a new session of supported type with default {@link SessionConfig session configuration}.
     * <p>
     * Supported types are:
     * <ul>
     *     <li>{@link org.neo4j.driver.Session} - synchronous session</li>
     *     <li>{@link org.neo4j.driver.async.AsyncSession} - asynchronous session</li>
     *     <li>{@link org.neo4j.driver.reactive.ReactiveSession} - reactive session using Flow API</li>
     *     <li>{@link org.neo4j.driver.reactivestreams.ReactiveSession} - reactive session using Reactive Streams
     * API</li>
     * </ul>
     * <p>
     * Sample usage:
     * <pre>
     * {@code
     * var session = driver.session(AsyncSession.class);
     * }
     * </pre>
     *
     * @param sessionClass session type class, must not be null
     * @return session instance
     * @param <T> session type
     * @throws IllegalArgumentException for unsupported session types
     * @since 5.2
     */
    default <T extends BaseSession> T session(Class<T> sessionClass) {
        return session(sessionClass, SessionConfig.defaultConfig());
    }

    /**
     * Instantiate a new session of a supported type with the supplied {@link AuthToken}.
     * <p>
     * This method allows creating a session with a different {@link AuthToken} to the one used on the driver level.
     * The minimum Bolt protocol version is 5.1. An {@link UnsupportedFeatureException} will be emitted on session interaction
     * for previous Bolt versions.
     * <p>
     * Supported types are:
     * <ul>
     *     <li>{@link org.neo4j.driver.Session} - synchronous session</li>
     *     <li>{@link org.neo4j.driver.async.AsyncSession} - asynchronous session</li>
     *     <li>{@link org.neo4j.driver.reactive.ReactiveSession} - reactive session using Flow API</li>
     *     <li>{@link org.neo4j.driver.reactivestreams.ReactiveSession} - reactive session using Reactive Streams
     * API</li>
     * </ul>
     * <p>
     * Sample usage:
     * <pre>
     * {@code
     * var session = driver.session(AsyncSession.class);
     * }
     * </pre>
     *
     * @param sessionClass session type class, must not be null
     * @param sessionAuthToken a token, null will result in driver-level configuration being used
     * @return session instance
     * @param <T> session type
     * @throws IllegalArgumentException for unsupported session types
     * @since 5.8
     */
    default <T extends BaseSession> T session(Class<T> sessionClass, AuthToken sessionAuthToken) {
        return session(sessionClass, SessionConfig.defaultConfig(), sessionAuthToken);
    }

    /**
     * Create a new session of supported type with a specified {@link SessionConfig session configuration}.
     * <p>
     * Supported types are:
     * <ul>
     *     <li>{@link org.neo4j.driver.Session} - synchronous session</li>
     *     <li>{@link org.neo4j.driver.async.AsyncSession} - asynchronous session</li>
     *     <li>{@link org.neo4j.driver.reactive.ReactiveSession} - reactive session using Flow API</li>
     *     <li>{@link org.neo4j.driver.reactivestreams.ReactiveSession} - reactive session using Reactive Streams
     * API</li>
     * </ul>
     * <p>
     * Sample usage:
     * <pre>
     * {@code
     * var session = driver.session(AsyncSession.class);
     * }
     * </pre>
     *
     * @param sessionClass session type class, must not be null
     * @param sessionConfig session config, must not be null
     * @return session instance
     * @param <T> session type
     * @throws IllegalArgumentException for unsupported session types
     * @since 5.2
     */
    default <T extends BaseSession> T session(Class<T> sessionClass, SessionConfig sessionConfig) {
        return session(sessionClass, sessionConfig, null);
    }

    /**
     * Instantiate a new session of a supported type with the supplied {@link SessionConfig session configuration} and
     * {@link AuthToken}.
     * <p>
     * This method allows creating a session with a different {@link AuthToken} to the one used on the driver level.
     * The minimum Bolt protocol version is 5.1. An {@link UnsupportedFeatureException} will be emitted on session interaction
     * for previous Bolt versions.
     * <p>
     * Supported types are:
     * <ul>
     *     <li>{@link org.neo4j.driver.Session} - synchronous session</li>
     *     <li>{@link org.neo4j.driver.async.AsyncSession} - asynchronous session</li>
     *     <li>{@link org.neo4j.driver.reactive.ReactiveSession} - reactive session using Flow API</li>
     *     <li>{@link org.neo4j.driver.reactivestreams.ReactiveSession} - reactive session using Reactive Streams
     * API</li>
     * </ul>
     * <p>
     * Sample usage:
     * <pre>
     * {@code
     * var session = driver.session(AsyncSession.class);
     * }
     * </pre>
     *
     * @param sessionClass session type class, must not be null
     * @param sessionConfig session config, must not be null
     * @param sessionAuthToken a token, null will result in driver-level configuration being used
     * @return session instance
     * @param <T> session type
     * @throws IllegalArgumentException for unsupported session types
     * @since 5.8
     */
    <T extends BaseSession> T session(Class<T> sessionClass, SessionConfig sessionConfig, AuthToken sessionAuthToken);

    /**
     * Close all the resources assigned to this driver, including open connections and IO threads.
     * <p>
     * This operation works the same way as {@link #closeAsync()} but blocks until all resources are closed.
     * <p>
     * Since this method is intended for graceful shutdown only, it is strongly recommended to finish interaction with
     * all driver resources (like sessions, transactions, results, etc.) before invoking this method. Not doing this may
     * result in unspecified behaviour, including leaving driver execution unfinished indefinitely.
     */
    @Override
    void close();

    /**
     * Close all the resources assigned to this driver, including open connections and IO threads.
     * <p>
     * This operation is asynchronous and returns a {@link CompletionStage}. This stage is completed with
     * {@code null} when all resources are closed. It is completed exceptionally if termination fails.
     * <p>
     * Since this method is intended for graceful shutdown only, it is strongly recommended to finish interaction with
     * all driver resources (like sessions, transactions, results, etc.) before invoking this method. Not doing this may
     * result in unspecified behaviour, including leaving driver execution unfinished indefinitely.
     *
     * @return a {@link CompletionStage completion stage} that represents the asynchronous close.
     */
    CompletionStage<Void> closeAsync();

    /**
     * Returns the driver metrics if metrics reporting is enabled via {@link Config.ConfigBuilder#withDriverMetrics()}.
     * Otherwise, a {@link ClientException} will be thrown.
     *
     * @return the driver metrics if enabled.
     * @throws ClientException if the driver metrics reporting is not enabled.
     */
    Metrics metrics();

    /**
     * Returns true if the driver metrics reporting is enabled via {@link Config.ConfigBuilder#withDriverMetrics()}, otherwise false.
     *
     * @return true if the metrics reporting is enabled.
     */
    boolean isMetricsEnabled();

    /**
     * This verifies if the driver can connect to a remote server or a cluster
     * by establishing a network connection with the remote and possibly exchanging a few data before closing the connection.
     * <p>
     * It throws exception if fails to connect. Use the exception to further understand the cause of the connectivity problem.
     * Note: Even if this method throws an exception, the driver still need to be closed via {@link #close()} to free up all resources.
     */
    void verifyConnectivity();

    /**
     * This verifies if the driver can connect to a remote server or cluster
     * by establishing a network connection with the remote and possibly exchanging a few data before closing the connection.
     * <p>
     * This operation is asynchronous and returns a {@link CompletionStage}. This stage is completed with
     * {@code null} when the driver connects to the remote server or cluster successfully.
     * It is completed exceptionally if the driver failed to connect the remote server or cluster.
     * This exception can be used to further understand the cause of the connectivity problem.
     * Note: Even if this method complete exceptionally, the driver still need to be closed via {@link #closeAsync()} to free up all resources.
     *
     * @return a {@link CompletionStage completion stage} that represents the asynchronous verification.
     */
    CompletionStage<Void> verifyConnectivityAsync();

    /**
     * Verifies if the given {@link AuthToken} is valid.
     * <p>
     * This check works on Bolt 5.1 version or above only.
     * @param authToken the token
     * @return the verification outcome
     * @since 5.8
     */
    boolean verifyAuthentication(AuthToken authToken);

    /**
     * Checks if session auth is supported.
     * @return the check outcome
     * @since 5.8
     * @see Driver#session(Class, SessionConfig, AuthToken)
     */
    boolean supportsSessionAuth();

    /**
     * Returns true if the server or cluster the driver connects to supports multi-databases, otherwise false.
     *
     * @return true if the server or cluster the driver connects to supports multi-databases, otherwise false.
     */
    boolean supportsMultiDb();

    /**
     * Asynchronous check if the server or cluster the driver connects to supports multi-databases.
     *
     * @return a {@link CompletionStage completion stage} that returns true if the server or cluster
     * the driver connects to supports multi-databases, otherwise false.
     */
    CompletionStage<Boolean> supportsMultiDbAsync();
}
