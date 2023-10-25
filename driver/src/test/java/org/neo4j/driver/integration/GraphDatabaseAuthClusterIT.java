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
package org.neo4j.driver.integration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.reactivestreams.FlowAdapters.toPublisher;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.AuthTokenManagerExecutionException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.reactive.ReactiveSession;
import org.neo4j.driver.testutil.cc.LocalOrRemoteClusterExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@DisabledIfSystemProperty(named = "skipDockerTests", matches = "^true$")
class GraphDatabaseAuthClusterIT {
    @RegisterExtension
    static final LocalOrRemoteClusterExtension clusterRule = new LocalOrRemoteClusterExtension();

    @Test
    void shouldEmitNullStageAsErrorOnDiscovery() {
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return null;
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager);
                var session = driver.session()) {
            assertThrows(AuthTokenManagerExecutionException.class, () -> session.run("RETURN 1"));
        }
    }

    @Test
    void shouldEmitNullStageAsErrorOnQueryExecution() {
        var returnNull = new AtomicBoolean();
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return returnNull.get() ? null : clusterRule.getAuthToken().getToken();
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager);
                var session = driver.session()) {
            session.run("RETURN 1").consume();
            returnNull.set(true);
            assertThrows(AuthTokenManagerExecutionException.class, () -> session.run("RETURN 1"));
        }
    }

    @Test
    void shouldEmitNullStageAsErrorOnQueryExecutionAndRecoverIfSubsequentStageIsValid() {
        var returnNull = new AtomicBoolean();
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return returnNull.get() ? null : clusterRule.getAuthToken().getToken();
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager);
                var session = driver.session()) {
            session.run("RETURN 1").consume();
            returnNull.set(true);
            assertThrows(AuthTokenManagerExecutionException.class, () -> session.run("RETURN 1"));
            returnNull.set(false);
            session.run("RETURN 1").consume();
        }
    }

    @Test
    void shouldEmitInvalidTokenAsErrorOnDiscovery() {
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager);
                var session = driver.session()) {
            var exception = assertThrows(AuthTokenManagerExecutionException.class, () -> session.run("RETURN 1"));
            assertTrue(exception.getCause() instanceof NullPointerException);
        }
    }

    @Test
    void shouldInvalidTokenAsErrorOnQueryExecution() {
        var returnNull = new AtomicBoolean();
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return returnNull.get()
                        ? CompletableFuture.completedFuture(null)
                        : clusterRule.getAuthToken().getToken();
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager);
                var session = driver.session()) {
            session.run("RETURN 1").consume();
            returnNull.set(true);
            assertThrows(AuthTokenManagerExecutionException.class, () -> session.run("RETURN 1"));
        }
    }

    @Test
    void shouldInvalidTokenAsErrorOnQueryExecutionAndRecoverIfSubsequentStageIsValid() {
        var returnNull = new AtomicBoolean();
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return returnNull.get()
                        ? CompletableFuture.completedFuture(null)
                        : clusterRule.getAuthToken().getToken();
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager);
                var session = driver.session()) {
            session.run("RETURN 1").consume();
            returnNull.set(true);
            assertThrows(AuthTokenManagerExecutionException.class, () -> session.run("RETURN 1"));
            returnNull.set(false);
            session.run("RETURN 1").consume();
        }
    }

    @Test
    void shouldEmitNullStageAsErrorOnDiscoveryAsync() {
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return null;
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(AsyncSession.class);
            var exception = assertThrows(
                    CompletionException.class,
                    () -> session.runAsync("RETURN 1").toCompletableFuture().join());
            assertTrue(exception.getCause() instanceof AuthTokenManagerExecutionException);
        }
    }

    @Test
    void shouldEmitNullStageAsErrorOnQueryExecutionAsync() {
        var returnNull = new AtomicBoolean();
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return returnNull.get() ? null : clusterRule.getAuthToken().getToken();
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(AsyncSession.class);
            session.runAsync("RETURN 1")
                    .thenCompose(ResultCursor::consumeAsync)
                    .toCompletableFuture()
                    .join();
            returnNull.set(true);
            var exception = assertThrows(
                    CompletionException.class,
                    () -> session.runAsync("RETURN 1").toCompletableFuture().join());
            assertTrue(exception.getCause() instanceof AuthTokenManagerExecutionException);
        }
    }

    @Test
    void shouldEmitNullStageAsErrorOnQueryExecutionAndRecoverIfSubsequentStageIsValidAsync() {
        var returnNull = new AtomicBoolean();
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return returnNull.get() ? null : clusterRule.getAuthToken().getToken();
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(AsyncSession.class);
            session.runAsync("RETURN 1")
                    .thenCompose(ResultCursor::consumeAsync)
                    .toCompletableFuture()
                    .join();
            returnNull.set(true);
            var exception = assertThrows(
                    CompletionException.class,
                    () -> session.runAsync("RETURN 1").toCompletableFuture().join());
            assertTrue(exception.getCause() instanceof AuthTokenManagerExecutionException);
            returnNull.set(false);
            session.runAsync("RETURN 1")
                    .thenCompose(ResultCursor::consumeAsync)
                    .toCompletableFuture()
                    .join();
        }
    }

    @Test
    void shouldEmitInvalidTokenAsErrorOnDiscoveryAsync() {
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(AsyncSession.class);
            var exception = assertThrows(
                    CompletionException.class,
                    () -> session.runAsync("RETURN 1").toCompletableFuture().join());
            assertTrue(exception.getCause() instanceof AuthTokenManagerExecutionException);
            assertTrue(exception.getCause().getCause() instanceof NullPointerException);
        }
    }

    @Test
    void shouldInvalidTokenAsErrorOnQueryExecutionAsync() {
        var returnNull = new AtomicBoolean();
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return returnNull.get()
                        ? CompletableFuture.completedFuture(null)
                        : clusterRule.getAuthToken().getToken();
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(AsyncSession.class);
            session.runAsync("RETURN 1")
                    .thenCompose(ResultCursor::consumeAsync)
                    .toCompletableFuture()
                    .join();
            returnNull.set(true);
            var exception = assertThrows(
                    CompletionException.class,
                    () -> session.runAsync("RETURN 1").toCompletableFuture().join());
            assertTrue(exception.getCause() instanceof AuthTokenManagerExecutionException);
            assertTrue(exception.getCause().getCause() instanceof NullPointerException);
        }
    }

    @Test
    void shouldInvalidTokenAsErrorOnQueryExecutionAndRecoverIfSubsequentStageIsValidAsync() {
        var returnNull = new AtomicBoolean();
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return returnNull.get()
                        ? CompletableFuture.completedFuture(null)
                        : clusterRule.getAuthToken().getToken();
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(AsyncSession.class);
            session.runAsync("RETURN 1")
                    .thenCompose(ResultCursor::consumeAsync)
                    .toCompletableFuture()
                    .join();
            returnNull.set(true);
            var exception = assertThrows(
                    CompletionException.class,
                    () -> session.runAsync("RETURN 1").toCompletableFuture().join());
            assertTrue(exception.getCause() instanceof AuthTokenManagerExecutionException);
            assertTrue(exception.getCause().getCause() instanceof NullPointerException);
            returnNull.set(false);
            session.runAsync("RETURN 1")
                    .thenCompose(ResultCursor::consumeAsync)
                    .toCompletableFuture()
                    .join();
        }
    }

    @Test
    void shouldEmitNullStageAsErrorOnDiscoveryFlux() {
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return null;
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(ReactiveSession.class);
            StepVerifier.create(toPublisher(session.run("RETURN 1")))
                    .expectErrorMatches(error -> error instanceof AuthTokenManagerExecutionException)
                    .verify();
        }
    }

    @Test
    void shouldEmitNullStageAsErrorOnQueryExecutionFlux() {
        var returnNull = new AtomicBoolean();
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return returnNull.get() ? null : clusterRule.getAuthToken().getToken();
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(ReactiveSession.class);
            StepVerifier.create(Mono.fromDirect(toPublisher(session.run("RETURN 1")))
                            .flatMap(result -> Mono.fromDirect(toPublisher(result.consume()))))
                    .expectNextCount(1)
                    .verifyComplete();
            returnNull.set(true);
            StepVerifier.create(toPublisher(session.run("RETURN 1")))
                    .expectErrorMatches(error -> error instanceof AuthTokenManagerExecutionException)
                    .verify();
        }
    }

    @Test
    void shouldEmitNullStageAsErrorOnQueryExecutionAndRecoverIfSubsequentStageIsValidFlux() {
        var returnNull = new AtomicBoolean();
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return returnNull.get() ? null : clusterRule.getAuthToken().getToken();
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(ReactiveSession.class);
            StepVerifier.create(Mono.fromDirect(toPublisher(session.run("RETURN 1")))
                            .flatMap(result -> Mono.fromDirect(toPublisher(result.consume()))))
                    .expectNextCount(1)
                    .verifyComplete();
            returnNull.set(true);
            StepVerifier.create(toPublisher(session.run("RETURN 1")))
                    .expectErrorMatches(error -> error instanceof AuthTokenManagerExecutionException)
                    .verify();
            returnNull.set(false);
            StepVerifier.create(Mono.fromDirect(toPublisher(session.run("RETURN 1")))
                            .flatMap(result -> Mono.fromDirect(toPublisher(result.consume()))))
                    .expectNextCount(1)
                    .verifyComplete();
        }
    }

    @Test
    void shouldEmitInvalidTokenAsErrorOnDiscoveryFlux() {
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(ReactiveSession.class);
            StepVerifier.create(toPublisher(session.run("RETURN 1")))
                    .expectErrorMatches(error -> error instanceof AuthTokenManagerExecutionException
                            && error.getCause() instanceof NullPointerException)
                    .verify();
        }
    }

    @Test
    void shouldInvalidTokenAsErrorOnQueryExecutionFlux() {
        var returnNull = new AtomicBoolean();
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return returnNull.get()
                        ? CompletableFuture.completedFuture(null)
                        : clusterRule.getAuthToken().getToken();
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(ReactiveSession.class);
            StepVerifier.create(Mono.fromDirect(toPublisher(session.run("RETURN 1")))
                            .flatMap(result -> Mono.fromDirect(toPublisher(result.consume()))))
                    .expectNextCount(1)
                    .verifyComplete();
            returnNull.set(true);
            StepVerifier.create(toPublisher(session.run("RETURN 1")))
                    .expectErrorMatches(error -> error instanceof AuthTokenManagerExecutionException
                            && error.getCause() instanceof NullPointerException)
                    .verify();
        }
    }

    @Test
    void shouldInvalidTokenAsErrorOnQueryExecutionAndRecoverIfSubsequentStageIsValidFlux() {
        var returnNull = new AtomicBoolean();
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return returnNull.get()
                        ? CompletableFuture.completedFuture(null)
                        : clusterRule.getAuthToken().getToken();
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(ReactiveSession.class);
            StepVerifier.create(Mono.fromDirect(toPublisher(session.run("RETURN 1")))
                            .flatMap(result -> Mono.fromDirect(toPublisher(result.consume()))))
                    .expectNextCount(1)
                    .verifyComplete();
            returnNull.set(true);
            StepVerifier.create(toPublisher(session.run("RETURN 1")))
                    .expectErrorMatches(error -> error instanceof AuthTokenManagerExecutionException
                            && error.getCause() instanceof NullPointerException)
                    .verify();
            returnNull.set(false);
            StepVerifier.create(Mono.fromDirect(toPublisher(session.run("RETURN 1")))
                            .flatMap(result -> Mono.fromDirect(toPublisher(result.consume()))))
                    .expectNextCount(1)
                    .verifyComplete();
        }
    }

    @Test
    void shouldEmitNullStageAsErrorOnDiscoveryReactiveStreams() {
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return null;
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(org.neo4j.driver.reactivestreams.ReactiveSession.class);
            StepVerifier.create(session.run("RETURN 1"))
                    .expectErrorMatches(error -> error instanceof AuthTokenManagerExecutionException)
                    .verify();
        }
    }

    @Test
    void shouldEmitNullStageAsErrorOnQueryExecutionReactiveStreams() {
        var returnNull = new AtomicBoolean();
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return returnNull.get() ? null : clusterRule.getAuthToken().getToken();
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(org.neo4j.driver.reactivestreams.ReactiveSession.class);
            StepVerifier.create(Mono.fromDirect(session.run("RETURN 1"))
                            .flatMap(result -> Mono.fromDirect(result.consume())))
                    .expectNextCount(1)
                    .verifyComplete();
            returnNull.set(true);
            StepVerifier.create(session.run("RETURN 1"))
                    .expectErrorMatches(error -> error instanceof AuthTokenManagerExecutionException)
                    .verify();
        }
    }

    @Test
    void shouldEmitNullStageAsErrorOnQueryExecutionAndRecoverIfSubsequentStageIsValidReactiveStreams() {
        var returnNull = new AtomicBoolean();
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return returnNull.get() ? null : clusterRule.getAuthToken().getToken();
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(org.neo4j.driver.reactivestreams.ReactiveSession.class);
            StepVerifier.create(Mono.fromDirect(session.run("RETURN 1"))
                            .flatMap(result -> Mono.fromDirect(result.consume())))
                    .expectNextCount(1)
                    .verifyComplete();
            returnNull.set(true);
            StepVerifier.create(session.run("RETURN 1"))
                    .expectErrorMatches(error -> error instanceof AuthTokenManagerExecutionException)
                    .verify();
            returnNull.set(false);
            StepVerifier.create(Mono.fromDirect(session.run("RETURN 1"))
                            .flatMap(result -> Mono.fromDirect(result.consume())))
                    .expectNextCount(1)
                    .verifyComplete();
        }
    }

    @Test
    void shouldEmitInvalidTokenAsErrorOnDiscoveryReactiveStreams() {
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(org.neo4j.driver.reactivestreams.ReactiveSession.class);
            StepVerifier.create(session.run("RETURN 1"))
                    .expectErrorMatches(error -> error instanceof AuthTokenManagerExecutionException
                            && error.getCause() instanceof NullPointerException)
                    .verify();
        }
    }

    @Test
    void shouldInvalidTokenAsErrorOnQueryExecutionReactiveStreams() {
        var returnNull = new AtomicBoolean();
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return returnNull.get()
                        ? CompletableFuture.completedFuture(null)
                        : clusterRule.getAuthToken().getToken();
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(org.neo4j.driver.reactivestreams.ReactiveSession.class);
            StepVerifier.create(Mono.fromDirect(session.run("RETURN 1"))
                            .flatMap(result -> Mono.fromDirect(result.consume())))
                    .expectNextCount(1)
                    .verifyComplete();
            returnNull.set(true);
            StepVerifier.create(session.run("RETURN 1"))
                    .expectErrorMatches(error -> error instanceof AuthTokenManagerExecutionException
                            && error.getCause() instanceof NullPointerException)
                    .verify();
        }
    }

    @Test
    void shouldInvalidTokenAsErrorOnQueryExecutionAndRecoverIfSubsequentStageIsValidReactiveStreams() {
        var returnNull = new AtomicBoolean();
        var manager = new AuthTokenManager() {
            @Override
            public CompletionStage<AuthToken> getToken() {
                return returnNull.get()
                        ? CompletableFuture.completedFuture(null)
                        : clusterRule.getAuthToken().getToken();
            }

            @Override
            public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
                return false;
            }
        };
        try (var driver = GraphDatabase.driver(clusterRule.getClusterUri(), manager)) {
            var session = driver.session(org.neo4j.driver.reactivestreams.ReactiveSession.class);
            StepVerifier.create(Mono.fromDirect(session.run("RETURN 1"))
                            .flatMap(result -> Mono.fromDirect(result.consume())))
                    .expectNextCount(1)
                    .verifyComplete();
            returnNull.set(true);
            StepVerifier.create(session.run("RETURN 1"))
                    .expectErrorMatches(error -> error instanceof AuthTokenManagerExecutionException
                            && error.getCause() instanceof NullPointerException)
                    .verify();
            returnNull.set(false);
            StepVerifier.create(Mono.fromDirect(session.run("RETURN 1"))
                            .flatMap(result -> Mono.fromDirect(result.consume())))
                    .expectNextCount(1)
                    .verifyComplete();
        }
    }
}
