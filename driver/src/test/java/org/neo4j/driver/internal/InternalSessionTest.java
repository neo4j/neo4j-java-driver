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
package org.neo4j.driver.internal;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatcher;
import org.neo4j.bolt.connection.TelemetryApi;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.TransactionContext;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;

public class InternalSessionTest {
    NetworkSession networkSession;
    Session session;

    @BeforeEach
    void beforeEach() {
        networkSession = mock(NetworkSession.class);
        session = new InternalSession(networkSession);
    }

    @ParameterizedTest
    @MethodSource("executeVariations")
    void shouldDelegateExecuteReadToRetryLogic(ExecuteVariation executeVariation) {
        // GIVEN
        var logic = mock(RetryLogic.class);
        var expected = "";
        given(logic.retry(any())).willReturn(expected);
        given(networkSession.retryLogic()).willReturn(logic);
        TransactionCallback<String> tc = (ignored) -> expected;
        Consumer<TransactionContext> consumer = (ignored) -> {};
        var config = TransactionConfig.builder().build();

        // WHEN
        String actual = null;
        if (executeVariation.readOnly) {
            actual = executeVariation.explicitTxConfig ? session.executeRead(tc, config) : session.executeRead(tc);
        } else {
            if (executeVariation.hasResult) {
                actual =
                        executeVariation.explicitTxConfig ? session.executeWrite(tc, config) : session.executeWrite(tc);
            } else {
                if (executeVariation.explicitTxConfig) {
                    session.executeWriteWithoutResult(consumer, config);
                } else {
                    session.executeWriteWithoutResult(consumer);
                }
            }
        }

        // THEN
        if (executeVariation.hasResult) {
            assertEquals(expected, actual);
        }
        then(networkSession).should().retryLogic();
        then(logic).should().retry(any());
    }

    @Test
    void shouldDelegateBeginWithType() {
        var internalSession = (InternalSession) session;
        var config = TransactionConfig.empty();
        var type = "TYPE";
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        ArgumentMatcher<ApiTelemetryWork> apiMatcher =
                argument -> apiTelemetryWork.telemetryApi().equals(argument.telemetryApi());
        given(networkSession.beginTransactionAsync(eq(config), eq(type), argThat(apiMatcher)))
                .willReturn(completedFuture(mock(UnmanagedTransaction.class)));

        internalSession.beginTransaction(config, type);

        then(networkSession).should().beginTransactionAsync(eq(config), eq(type), argThat(apiMatcher));
    }

    static List<ExecuteVariation> executeVariations() {
        return Arrays.asList(
                new ExecuteVariation(false, false, false),
                new ExecuteVariation(false, false, true),
                new ExecuteVariation(false, true, false),
                new ExecuteVariation(false, true, true),
                new ExecuteVariation(true, false, true),
                new ExecuteVariation(true, true, true));
    }

    private record ExecuteVariation(boolean readOnly, boolean explicitTxConfig, boolean hasResult) {}
}
