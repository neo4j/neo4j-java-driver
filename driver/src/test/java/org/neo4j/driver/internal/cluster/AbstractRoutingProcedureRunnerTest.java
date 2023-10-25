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
package org.neo4j.driver.internal.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.spi.Connection;

abstract class AbstractRoutingProcedureRunnerTest {
    @Test
    void shouldReturnFailedResponseOnClientException() {
        var error = new ClientException("Hi");
        var runner = singleDatabaseRoutingProcedureRunner(failedFuture(error));

        var response = await(runner.run(connection(), defaultDatabase(), Collections.emptySet(), null));

        assertFalse(response.isSuccess());
        assertEquals(error, response.error());
    }

    @Test
    void shouldReturnFailedStageOnError() {
        var error = new Exception("Hi");
        var runner = singleDatabaseRoutingProcedureRunner(failedFuture(error));

        var e = assertThrows(
                Exception.class,
                () -> await(runner.run(connection(), defaultDatabase(), Collections.emptySet(), null)));
        assertEquals(error, e);
    }

    @Test
    void shouldReleaseConnectionOnSuccess() {
        var runner = singleDatabaseRoutingProcedureRunner();

        var connection = connection();
        var response = await(runner.run(connection, defaultDatabase(), Collections.emptySet(), null));

        assertTrue(response.isSuccess());
        verify(connection).release();
    }

    @Test
    void shouldPropagateReleaseError() {
        var runner = singleDatabaseRoutingProcedureRunner();

        var releaseError = new RuntimeException("Release failed");
        var connection = connection(failedFuture(releaseError));

        var e = assertThrows(
                RuntimeException.class,
                () -> await(runner.run(connection, defaultDatabase(), Collections.emptySet(), null)));
        assertEquals(releaseError, e);
        verify(connection).release();
    }

    abstract SingleDatabaseRoutingProcedureRunner singleDatabaseRoutingProcedureRunner();

    abstract SingleDatabaseRoutingProcedureRunner singleDatabaseRoutingProcedureRunner(
            CompletionStage<List<Record>> runProcedureResult);

    static Connection connection() {
        return connection(completedWithNull());
    }

    static Connection connection(CompletionStage<Void> releaseStage) {
        var connection = mock(Connection.class);
        var boltProtocol = mock(BoltProtocol.class);
        var protocolVersion = new BoltProtocolVersion(4, 4);
        when(boltProtocol.version()).thenReturn(protocolVersion);
        when(connection.protocol()).thenReturn(boltProtocol);
        when(connection.release()).thenReturn(releaseStage);
        return connection;
    }
}
