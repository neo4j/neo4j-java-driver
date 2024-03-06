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
package org.neo4j.driver.org.neo4j.driver;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.ClientCertificates;

class ClientCertificatesTests {
    @ParameterizedTest
    @MethodSource("shouldThrowOnNullArgs")
    void shouldThrowOnNull(File certificate, File key) {
        assertThrows(NullPointerException.class, () -> ClientCertificates.of(certificate, key));
    }

    static Stream<Arguments> shouldThrowOnNullArgs() {
        return Stream.of(
                Arguments.of(null, mock(File.class)), Arguments.of(mock(File.class), null), Arguments.of(null, null));
    }

    @ParameterizedTest
    @MethodSource("shouldThrowOnNullWithPassword")
    void shouldThrowOnNullWithPassword(File certificate, File key, String password) {
        assertThrows(NullPointerException.class, () -> ClientCertificates.of(certificate, key, password));
    }

    static Stream<Arguments> shouldThrowOnNullWithPassword() {
        return Stream.of(
                Arguments.of(null, mock(File.class), "password"),
                Arguments.of(mock(File.class), null, "password"),
                Arguments.of(null, null, "password"));
    }

    @Test
    void shouldAcceptNullPassword() {
        var clientCertificate = ClientCertificates.of(mock(File.class), mock(File.class), null);

        assertNotNull(clientCertificate);
    }
}
