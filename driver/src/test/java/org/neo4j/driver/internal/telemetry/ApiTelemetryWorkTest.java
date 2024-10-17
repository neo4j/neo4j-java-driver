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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.TelemetryApi;

class ApiTelemetryWorkTest {

    @ParameterizedTest
    @EnumSource(TelemetryApi.class)
    void shouldPipelineTelemetryWhenTelemetryIsEnabledAndConnectionSupportsTelemetry(TelemetryApi telemetryApi) {
        var apiTelemetryWork = new ApiTelemetryWork(telemetryApi);
        apiTelemetryWork.setEnabled(true);
        var boltConnection = Mockito.mock(BoltConnection.class);
        var boltConnectionStage = CompletableFuture.completedFuture(boltConnection);
        given(boltConnection.telemetrySupported()).willReturn(true);
        given(boltConnection.telemetry(telemetryApi)).willReturn(boltConnectionStage);

        var stage = apiTelemetryWork.pipelineTelemetryIfEnabled(boltConnection);

        assertEquals(boltConnectionStage, stage);
        then(boltConnection).should().telemetry(telemetryApi);
    }

    @ParameterizedTest
    @EnumSource(TelemetryApi.class)
    void shouldNotPipelineTelemetryWhenTelemetryIsEnabledAndConnectionDoesNotSupportTelemetry(
            TelemetryApi telemetryApi) {
        var apiTelemetryWork = new ApiTelemetryWork(telemetryApi);
        apiTelemetryWork.setEnabled(true);
        var boltConnection = Mockito.mock(BoltConnection.class);

        var future = apiTelemetryWork.pipelineTelemetryIfEnabled(boltConnection).toCompletableFuture();

        assertTrue(future.isDone());
        assertEquals(boltConnection, future.join());
        then(boltConnection).should().telemetrySupported();
        then(boltConnection).shouldHaveNoMoreInteractions();
    }

    @ParameterizedTest
    @EnumSource(TelemetryApi.class)
    void shouldNotPipelineTelemetryWhenTelemetryIsDisabledAndConnectionDoesNotSupportTelemetry(
            TelemetryApi telemetryApi) {
        var apiTelemetryWork = new ApiTelemetryWork(telemetryApi);
        var boltConnection = Mockito.mock(BoltConnection.class);

        var future = apiTelemetryWork.pipelineTelemetryIfEnabled(boltConnection).toCompletableFuture();

        assertTrue(future.isDone());
        assertEquals(boltConnection, future.join());
        then(boltConnection).shouldHaveNoInteractions();
    }

    @ParameterizedTest
    @EnumSource(TelemetryApi.class)
    void shouldNotPipelineTelemetryWhenTelemetryIsDisabledAndConnectionSupportsTelemetry(TelemetryApi telemetryApi) {
        var apiTelemetryWork = new ApiTelemetryWork(telemetryApi);
        var boltConnection = Mockito.mock(BoltConnection.class);
        given(boltConnection.telemetrySupported()).willReturn(true);

        var future = apiTelemetryWork.pipelineTelemetryIfEnabled(boltConnection).toCompletableFuture();

        assertTrue(future.isDone());
        assertEquals(boltConnection, future.join());
        then(boltConnection).shouldHaveNoInteractions();
    }
}
