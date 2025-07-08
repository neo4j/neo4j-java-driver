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
package org.neo4j.driver.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.Value;

class Neo4jExceptionTest {

    @ParameterizedTest
    @MethodSource("shouldInitArgs")
    void shouldInit(
            String gqlStatus,
            String description,
            String code,
            String message,
            Map<String, Value> diagnosticRecord,
            Throwable cause) {

        var exception = new Neo4jException(gqlStatus, description, code, message, diagnosticRecord, cause);

        assertEquals(gqlStatus, exception.gqlStatus());
        assertEquals(description, exception.statusDescription());
        assertEquals(code, exception.code());
        assertEquals(message, exception.getMessage());
        assertEquals(diagnosticRecord, exception.diagnosticRecord());
        assertEquals(cause, exception.getCause());
        assertEquals(cause, exception.gqlCause().orElse(null));
    }

    private static Stream<Arguments> shouldInitArgs() {
        return Stream.of(
                Arguments.of(
                        "status",
                        "description",
                        "code",
                        "message",
                        Collections.emptyMap(),
                        new Neo4jException("status", "description", "code", "message", Collections.emptyMap(), null)),
                Arguments.of(
                        "status",
                        "description",
                        "code",
                        "message",
                        Collections.emptyMap(),
                        new ServiceUnavailableException("message")));
    }

    @Test
    void shouldFindGqlCauseOnNonInterruptedChainOnly() {
        var exception4 = new ServiceUnavailableException("message", null);
        var exception3 = new IllegalStateException("message", exception4);
        var exception2 =
                new Neo4jException("status", "description", "code", "message", Collections.emptyMap(), exception3);
        var exception1 =
                new Neo4jException("status", "description", "code", "message", Collections.emptyMap(), exception2);
        var exception =
                new ClientException("status", "description", "code", "message", Collections.emptyMap(), exception1);

        assertError(exception, exception1, exception1);
        assertError(exception1, exception2, exception2);
        assertError(exception2, exception3, null);
    }

    @ParameterizedTest
    @MethodSource("gqlErrorsArgs")
    void shouldFindByGqlStatus(Neo4jException exception, int statusIndex, boolean shouldBeFound) {
        // given
        var status = "status" + statusIndex;

        // when
        var exceptionWithStatus = exception.findByGqlStatus(status);

        // then
        if (shouldBeFound) {
            assertTrue(exceptionWithStatus.isPresent());
            assertEquals(getByIndex(exception, statusIndex), exceptionWithStatus.get());
        } else {
            assertTrue(exceptionWithStatus.isEmpty());
        }
    }

    @ParameterizedTest
    @MethodSource("gqlErrorsArgs")
    void shouldReturnIfErrorContainsGqlStatus(Neo4jException exception, int statusIndex, boolean shouldBeFound) {
        // given
        var status = "status" + statusIndex;

        // when
        var contains = exception.containsGqlStatus(status);

        // then
        assertEquals(shouldBeFound, contains);
    }

    static Stream<Arguments> gqlErrorsArgs() {
        return Stream.of(
                Arguments.of(buildGqlErrors(1, 1), 0, true),
                Arguments.of(buildGqlErrors(1, 1), -1, false),
                Arguments.of(buildGqlErrors(20, 20), 0, true),
                Arguments.of(buildGqlErrors(20, 20), 10, true),
                Arguments.of(buildGqlErrors(20, 20), 19, true),
                Arguments.of(buildGqlErrors(20, 20), -1, false),
                Arguments.of(buildGqlErrors(20, 15), 0, true),
                Arguments.of(buildGqlErrors(20, 15), 14, true),
                Arguments.of(buildGqlErrors(20, 15), -1, false));
    }

    @SuppressWarnings("DataFlowIssue")
    static Neo4jException buildGqlErrors(int totalSize, int gqlErrorsSize) {
        Exception exception = null;
        for (var i = totalSize - 1; i >= gqlErrorsSize; i--) {
            exception = new IllegalStateException("illegal state" + i, exception);
        }
        for (var i = gqlErrorsSize - 1; i >= 0; i--) {
            exception = new Neo4jException(
                    "status" + i, "description" + i, "code", "message", Collections.emptyMap(), exception);
        }
        return (Neo4jException) exception;
    }

    static Throwable getByIndex(Throwable exception, int depth) {
        var result = exception;
        for (var i = 0; i < depth; i++) {
            result = result.getCause();
        }
        return result;
    }

    private void assertError(Neo4jException exception, Throwable expectedCause, Neo4jException expectedGqlCause) {
        assertEquals(expectedCause, exception.getCause());
        assertEquals(expectedGqlCause, exception.gqlCause().orElse(null));
    }
}
