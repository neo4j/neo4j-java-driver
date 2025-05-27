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
package neo4j.org.testkit.backend;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.Getter;
import neo4j.org.testkit.backend.holder.AsyncSessionHolder;
import neo4j.org.testkit.backend.holder.AsyncTransactionHolder;
import neo4j.org.testkit.backend.holder.DriverHolder;
import neo4j.org.testkit.backend.holder.ReactiveResultHolder;
import neo4j.org.testkit.backend.holder.ReactiveResultStreamsHolder;
import neo4j.org.testkit.backend.holder.ReactiveSessionHolder;
import neo4j.org.testkit.backend.holder.ReactiveSessionStreamsHolder;
import neo4j.org.testkit.backend.holder.ReactiveTransactionHolder;
import neo4j.org.testkit.backend.holder.ReactiveTransactionStreamsHolder;
import neo4j.org.testkit.backend.holder.ResultCursorHolder;
import neo4j.org.testkit.backend.holder.ResultHolder;
import neo4j.org.testkit.backend.holder.SessionHolder;
import neo4j.org.testkit.backend.holder.TransactionHolder;
import neo4j.org.testkit.backend.messages.requests.TestkitCallbackResult;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.bolt.connection.routed.impl.cluster.RoutingTableRegistry;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.BookmarkManager;
import org.neo4j.driver.ClientCertificateManager;
import reactor.core.publisher.Mono;

public class TestkitState {
    private static final String DRIVER_NOT_FOUND_MESSAGE = "Could not find driver";
    private static final String SESSION_NOT_FOUND_MESSAGE = "Could not find session";
    private static final String TRANSACTION_NOT_FOUND_MESSAGE = "Could not find transaction";
    private static final String RESULT_NOT_FOUND_MESSAGE = "Could not find result";
    private static final String BOOKMARK_MANAGER_NOT_FOUND_MESSAGE = "Could not find bookmark manager";
    private static final String AUTH_PROVIDER_NOT_FOUND_MESSAGE = "Could not find authentication provider";
    private static final String CLIENT_CERTIFICATE_PROVIDER_NOT_FOUND_MESSAGE =
            "Could not find client certificate provider";

    private final Map<String, DriverHolder> driverIdToDriverHolder = new HashMap<>();

    @Getter
    private final Map<String, RoutingTableRegistry> routingTableRegistry = new HashMap<>();

    private final Map<String, SessionHolder> sessionIdToSessionHolder = new HashMap<>();
    private final Map<String, AsyncSessionHolder> sessionIdToAsyncSessionHolder = new HashMap<>();
    private final Map<String, ReactiveSessionHolder> sessionIdToReactiveSessionHolder = new HashMap<>();
    private final Map<String, ReactiveSessionStreamsHolder> sessionIdToReactiveSessionStreamsHolder = new HashMap<>();
    private final Map<String, ResultHolder> resultIdToResultHolder = new HashMap<>();
    private final Map<String, ResultCursorHolder> resultIdToResultCursorHolder = new HashMap<>();
    private final Map<String, ReactiveResultHolder> resultIdToReactiveResultHolder = new HashMap<>();
    private final Map<String, ReactiveResultStreamsHolder> resultIdToReactiveResultStreamsHolder = new HashMap<>();
    private final Map<String, TransactionHolder> transactionIdToTransactionHolder = new HashMap<>();
    private final Map<String, AsyncTransactionHolder> transactionIdToAsyncTransactionHolder = new HashMap<>();
    private final Map<String, ReactiveTransactionHolder> transactionIdToReactiveTransactionHolder = new HashMap<>();
    private final Map<String, ReactiveTransactionStreamsHolder> transactionIdToReactiveTransactionStreamsHolder =
            new HashMap<>();
    private final Map<String, BookmarkManager> bookmarkManagerIdToBookmarkManager = new HashMap<>();

    private final Map<String, AuthTokenManager> authProviderIdToAuthProvider = new HashMap<>();
    private final Map<String, ClientCertificateManager> managerIdToClientCertificateManager = new HashMap<>();

    @Getter
    private final Map<String, Exception> errors = new HashMap<>();

    private final AtomicInteger idGenerator = new AtomicInteger(0);

    @Getter
    private final Consumer<TestkitResponse> responseWriter;

    @Getter
    private final Map<String, CompletableFuture<TestkitCallbackResult>> callbackIdToFuture = new HashMap<>();

    public TestkitState(Consumer<TestkitResponse> responseWriter) {
        this.responseWriter = responseWriter;
    }

    public String newId() {
        return String.valueOf(idGenerator.getAndIncrement());
    }

    public void addDriverHolder(String id, DriverHolder driverHolder) {
        driverIdToDriverHolder.put(id, driverHolder);
    }

    public DriverHolder getDriverHolder(String id) {
        return get(id, driverIdToDriverHolder, DRIVER_NOT_FOUND_MESSAGE);
    }

    public String addSessionHolder(SessionHolder sessionHolder) {
        return add(sessionHolder, sessionIdToSessionHolder);
    }

    public SessionHolder getSessionHolder(String id) {
        return get(id, sessionIdToSessionHolder, SESSION_NOT_FOUND_MESSAGE);
    }

    public String addAsyncSessionHolder(AsyncSessionHolder sessionHolder) {
        return add(sessionHolder, sessionIdToAsyncSessionHolder);
    }

    public CompletionStage<AsyncSessionHolder> getAsyncSessionHolder(String id) {
        return getAsync(id, sessionIdToAsyncSessionHolder, SESSION_NOT_FOUND_MESSAGE);
    }

    public String addReactiveSessionHolder(ReactiveSessionHolder sessionHolder) {
        return add(sessionHolder, sessionIdToReactiveSessionHolder);
    }

    public Mono<ReactiveSessionHolder> getReactiveSessionHolder(String id) {
        return getRx(id, sessionIdToReactiveSessionHolder, SESSION_NOT_FOUND_MESSAGE);
    }

    public String addReactiveSessionStreamsHolder(ReactiveSessionStreamsHolder sessionHolder) {
        return add(sessionHolder, sessionIdToReactiveSessionStreamsHolder);
    }

    public Mono<ReactiveSessionStreamsHolder> getReactiveSessionStreamsHolder(String id) {
        return getRx(id, sessionIdToReactiveSessionStreamsHolder, SESSION_NOT_FOUND_MESSAGE);
    }

    public String addTransactionHolder(TransactionHolder transactionHolder) {
        return add(transactionHolder, transactionIdToTransactionHolder);
    }

    public TransactionHolder getTransactionHolder(String id) {
        return get(id, transactionIdToTransactionHolder, TRANSACTION_NOT_FOUND_MESSAGE);
    }

    public String addAsyncTransactionHolder(AsyncTransactionHolder transactionHolder) {
        return add(transactionHolder, transactionIdToAsyncTransactionHolder);
    }

    public CompletionStage<AsyncTransactionHolder> getAsyncTransactionHolder(String id) {
        return getAsync(id, transactionIdToAsyncTransactionHolder, TRANSACTION_NOT_FOUND_MESSAGE);
    }

    public String addReactiveTransactionHolder(ReactiveTransactionHolder transactionHolder) {
        return add(transactionHolder, transactionIdToReactiveTransactionHolder);
    }

    public Mono<ReactiveTransactionHolder> getReactiveTransactionHolder(String id) {
        return getRx(id, transactionIdToReactiveTransactionHolder, TRANSACTION_NOT_FOUND_MESSAGE);
    }

    public String addReactiveTransactionStreamsHolder(ReactiveTransactionStreamsHolder transactionHolder) {
        return add(transactionHolder, transactionIdToReactiveTransactionStreamsHolder);
    }

    public Mono<ReactiveTransactionStreamsHolder> getReactiveTransactionStreamsHolder(String id) {
        return getRx(id, transactionIdToReactiveTransactionStreamsHolder, TRANSACTION_NOT_FOUND_MESSAGE);
    }

    public String addResultHolder(ResultHolder resultHolder) {
        return add(resultHolder, resultIdToResultHolder);
    }

    public ResultHolder getResultHolder(String id) {
        return get(id, resultIdToResultHolder, RESULT_NOT_FOUND_MESSAGE);
    }

    public String addAsyncResultHolder(ResultCursorHolder resultHolder) {
        return add(resultHolder, resultIdToResultCursorHolder);
    }

    public CompletionStage<ResultCursorHolder> getAsyncResultHolder(String id) {
        return getAsync(id, resultIdToResultCursorHolder, RESULT_NOT_FOUND_MESSAGE);
    }

    public String addReactiveResultHolder(ReactiveResultHolder resultHolder) {
        return add(resultHolder, resultIdToReactiveResultHolder);
    }

    public Mono<ReactiveResultHolder> getReactiveResultHolder(String id) {
        return getRx(id, resultIdToReactiveResultHolder, RESULT_NOT_FOUND_MESSAGE);
    }

    public String addReactiveResultStreamsHolder(ReactiveResultStreamsHolder resultHolder) {
        return add(resultHolder, resultIdToReactiveResultStreamsHolder);
    }

    public Mono<ReactiveResultStreamsHolder> getReactiveResultStreamsHolder(String id) {
        return getRx(id, resultIdToReactiveResultStreamsHolder, RESULT_NOT_FOUND_MESSAGE);
    }

    public void addBookmarkManager(String id, BookmarkManager bookmarkManager) {
        bookmarkManagerIdToBookmarkManager.put(id, bookmarkManager);
    }

    public BookmarkManager getBookmarkManager(String id) {
        return get(id, bookmarkManagerIdToBookmarkManager, BOOKMARK_MANAGER_NOT_FOUND_MESSAGE);
    }

    public void removeBookmarkManager(String id) {
        if (bookmarkManagerIdToBookmarkManager.remove(id) == null) {
            throw new RuntimeException(BOOKMARK_MANAGER_NOT_FOUND_MESSAGE);
        }
    }

    public void addAuthProvider(String id, AuthTokenManager authProvider) {
        authProviderIdToAuthProvider.put(id, authProvider);
    }

    public AuthTokenManager getAuthProvider(String id) {
        return get(id, authProviderIdToAuthProvider, AUTH_PROVIDER_NOT_FOUND_MESSAGE);
    }

    public void removeAuthProvider(String id) {
        if (authProviderIdToAuthProvider.remove(id) == null) {
            throw new RuntimeException(AUTH_PROVIDER_NOT_FOUND_MESSAGE);
        }
    }

    public void addClientCertificateManager(String id, ClientCertificateManager manager) {
        managerIdToClientCertificateManager.put(id, manager);
    }

    public ClientCertificateManager getClientCertificateManager(String id) {
        return get(id, managerIdToClientCertificateManager, CLIENT_CERTIFICATE_PROVIDER_NOT_FOUND_MESSAGE);
    }

    public void removeClientCertificateManager(String id) {
        if (managerIdToClientCertificateManager.remove(id) == null) {
            throw new RuntimeException(CLIENT_CERTIFICATE_PROVIDER_NOT_FOUND_MESSAGE);
        }
    }

    private <T> String add(T value, Map<String, T> idToT) {
        var id = newId();
        idToT.put(id, value);
        return id;
    }

    private <T> T get(String id, Map<String, T> idToT, String notFoundMessage) {
        var value = idToT.get(id);
        if (value == null) {
            throw new RuntimeException(notFoundMessage);
        }
        return value;
    }

    private <T> CompletableFuture<T> getAsync(String id, Map<String, T> idToT, String notFoundMessage) {
        var result = new CompletableFuture<T>();
        var value = idToT.get(id);
        if (value == null) {
            result.completeExceptionally(new RuntimeException(notFoundMessage));
        } else {
            result.complete(value);
        }
        return result;
    }

    private <T> Mono<T> getRx(String id, Map<String, T> idToT, String notFoundMessage) {
        return Mono.fromCompletionStage(getAsync(id, idToT, notFoundMessage));
    }
}
