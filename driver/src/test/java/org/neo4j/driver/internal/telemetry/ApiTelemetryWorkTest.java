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
package org.neo4j.driver.internal.telemetry;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.testutil.TestUtil;

class ApiTelemetryWorkTest {

    @ParameterizedTest
    @MethodSource("shouldNotSendTelemetrySource")
    public void shouldNotCallTelemetryAndCompleteStage(
            boolean telemetryEnabled, Consumer<ApiTelemetryWork> transformWorker) {
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        var protocol = Mockito.mock(BoltProtocol.class);
        var connection = Mockito.mock(Connection.class);
        Mockito.doReturn(telemetryEnabled).when(connection).isTelemetryEnabled();
        transformWorker.accept(apiTelemetryWork);

        TestUtil.await(apiTelemetryWork.execute(connection, protocol));

        Mockito.verify(protocol, Mockito.never()).telemetry(Mockito.any(), Mockito.any());
    }

    @ParameterizedTest
    @MethodSource("shouldCallTelemetry")
    public void shouldCallTelemetryWithCorrectValuesAndResolveFuture(
            TelemetryApi telemetryApi, boolean telemetryEnabled, Consumer<ApiTelemetryWork> transformWorker) {
        var apiTelemetryWork = new ApiTelemetryWork(telemetryApi);
        var protocol = Mockito.mock(BoltProtocol.class);
        var connection = Mockito.mock(Connection.class);
        Mockito.doReturn(telemetryEnabled).when(connection).isTelemetryEnabled();
        Mockito.doReturn(CompletableFuture.completedFuture(null))
                .when(protocol)
                .telemetry(Mockito.any(), Mockito.any());
        transformWorker.accept(apiTelemetryWork);

        TestUtil.await(apiTelemetryWork.execute(connection, protocol));

        Mockito.verify(protocol, Mockito.only()).telemetry(connection, telemetryApi.getValue());
    }

    @ParameterizedTest
    @MethodSource("shouldCallTelemetry")
    public void shouldCallTelemetryWithCorrectValuesAndFailedFuture(
            TelemetryApi telemetryApi, boolean telemetryEnabled, Consumer<ApiTelemetryWork> transformWorker) {
        var apiTelemetryWork = new ApiTelemetryWork(telemetryApi);
        var protocol = Mockito.mock(BoltProtocol.class);
        var connection = Mockito.mock(Connection.class);
        var exception = new RuntimeException("something wrong");
        Mockito.doReturn(telemetryEnabled).when(connection).isTelemetryEnabled();
        Mockito.doReturn(CompletableFuture.failedFuture(exception))
                .when(protocol)
                .telemetry(Mockito.any(), Mockito.any());
        transformWorker.accept(apiTelemetryWork);

        Assertions.assertThrows(
                RuntimeException.class, () -> TestUtil.await(apiTelemetryWork.execute(connection, protocol)));

        Mockito.verify(protocol, Mockito.only()).telemetry(connection, telemetryApi.getValue());
    }

    public static Stream<Arguments> shouldNotSendTelemetrySource() {
        return Stream.of(
                Arguments.of(false, (Consumer<ApiTelemetryWork>)
                        ApiTelemetryWorkTest::callApiTelemetryWorkSetEnabledWithFalse),
                Arguments.of(false, (Consumer<ApiTelemetryWork>)
                        ApiTelemetryWorkTest::callApiTelemetryWorkSetEnabledWithTrue),
                Arguments.of(false, (Consumer<ApiTelemetryWork>)
                        ApiTelemetryWorkTest::callApiTelemetryWorkExecuteWithSuccess),
                Arguments.of(
                        false, (Consumer<ApiTelemetryWork>) ApiTelemetryWorkTest::callApiTelemetryWorkExecuteWithError),
                Arguments.of(false, (Consumer<ApiTelemetryWork>) ApiTelemetryWorkTest::noop),
                Arguments.of(true, (Consumer<ApiTelemetryWork>)
                        ApiTelemetryWorkTest::callApiTelemetryWorkSetEnabledWithFalse),
                Arguments.of(true, (Consumer<ApiTelemetryWork>)
                        ApiTelemetryWorkTest::callApiTelemetryWorkExecuteWithSuccess));
    }

    private static Stream<Arguments> shouldCallTelemetry() {
        return Stream.of(TelemetryApi.values())
                .flatMap(telemetryApi -> Stream.of(
                        Arguments.of(telemetryApi, true, (Consumer<ApiTelemetryWork>)
                                ApiTelemetryWorkTest::callApiTelemetryWorkSetEnabledWithTrue),
                        Arguments.of(telemetryApi, true, (Consumer<ApiTelemetryWork>)
                                ApiTelemetryWorkTest::callApiTelemetryWorkExecuteWithError),
                        Arguments.of(telemetryApi, true, (Consumer<ApiTelemetryWork>) ApiTelemetryWorkTest::noop)));
    }

    private static void callApiTelemetryWorkSetEnabledWithTrue(ApiTelemetryWork apiTelemetryWork) {
        apiTelemetryWork.setEnabled(true);
    }

    private static void callApiTelemetryWorkSetEnabledWithFalse(ApiTelemetryWork apiTelemetryWork) {
        apiTelemetryWork.setEnabled(false);
    }

    @SuppressWarnings("EmptyMethod")
    private static void noop(ApiTelemetryWork apiTelemetryWork) {}

    private static void callApiTelemetryWorkExecuteWithSuccess(ApiTelemetryWork apiTelemetryWork) {
        var protocol = Mockito.mock(BoltProtocol.class);
        var connection = Mockito.mock(Connection.class);
        Mockito.doReturn(CompletableFuture.completedFuture(null))
                .when(protocol)
                .telemetry(Mockito.any(), Mockito.any());
        Mockito.doReturn(true).when(connection).isTelemetryEnabled();

        TestUtil.await(apiTelemetryWork.execute(connection, protocol));
    }

    private static void callApiTelemetryWorkExecuteWithError(ApiTelemetryWork apiTelemetryWork) {
        var protocol = Mockito.mock(BoltProtocol.class);
        var connection = Mockito.mock(Connection.class);
        Mockito.doReturn(CompletableFuture.failedFuture(new RuntimeException("WRONG")))
                .when(protocol)
                .telemetry(Mockito.any(), Mockito.any());
        Mockito.doReturn(true).when(connection).isTelemetryEnabled();

        try {
            TestUtil.await(apiTelemetryWork.execute(connection, protocol));
        } catch (Exception ex) {
            // ignore since the error is expected.
        }
    }
}
