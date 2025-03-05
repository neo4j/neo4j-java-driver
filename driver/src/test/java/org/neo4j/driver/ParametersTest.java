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

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.connection.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.ValueFactory.emptyNodeValue;
import static org.neo4j.driver.internal.util.ValueFactory.emptyRelationshipValue;
import static org.neo4j.driver.internal.util.ValueFactory.filledPathValue;

import java.util.Collections;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.InternalSession;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnectionProvider;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.security.BoltSecurityPlanManager;

class ParametersTest {
    static Stream<Arguments> addressesToParse() {
        return Stream.of(
                // Node
                Arguments.of(emptyNodeValue(), "Nodes can't be used as parameters."),
                Arguments.of(emptyNodeValue().asNode(), "Nodes can't be used as parameters."),

                // Relationship
                Arguments.of(emptyRelationshipValue(), "Relationships can't be used as parameters."),
                Arguments.of(emptyRelationshipValue().asRelationship(), "Relationships can't be used as parameters."),

                // Path
                Arguments.of(filledPathValue(), "Paths can't be used as parameters."),
                Arguments.of(filledPathValue().asPath(), "Paths can't be used as parameters."));
    }

    @ParameterizedTest
    @MethodSource("addressesToParse")
    void shouldGiveHelpfulMessageOnMisalignedInput(Object obj, String expectedMsg) {
        var e = assertThrows(ClientException.class, () -> Values.parameters("1", obj, "2"));
        assertThat(
                e.getMessage(),
                startsWith("Parameters function requires an even number of arguments, alternating key and value."));
    }

    @ParameterizedTest
    @MethodSource("addressesToParse")
    @SuppressWarnings("resource")
    void shouldNotBePossibleToUseInvalidParameterTypesViaParameters(Object obj, String expectedMsg) {
        var session = mockedSession();
        var e = assertThrows(ClientException.class, () -> session.run("RETURN {a}", parameters("a", obj)));
        assertEquals(expectedMsg, e.getMessage());
    }

    @ParameterizedTest
    @MethodSource("addressesToParse")
    @SuppressWarnings("resource")
    void shouldNotBePossibleToUseInvalidParametersViaMap(Object obj, String expectedMsg) {
        var session = mockedSession();
        var e = assertThrows(ClientException.class, () -> session.run("RETURN {a}", singletonMap("a", obj)));
        assertEquals(expectedMsg, e.getMessage());
    }

    @ParameterizedTest
    @MethodSource("addressesToParse")
    @SuppressWarnings("resource")
    void shouldNotBePossibleToUseInvalidParametersViaRecord(Object obj, String expectedMsg) {
        assumeTrue(obj instanceof Value);
        Record record = new InternalRecord(singletonList("a"), new Value[] {(Value) obj});
        var session = mockedSession();

        var e = assertThrows(ClientException.class, () -> session.run("RETURN {a}", record));
        assertEquals(expectedMsg, e.getMessage());
    }

    private Session mockedSession() {
        var provider = mock(DriverBoltConnectionProvider.class);
        var retryLogic = mock(RetryLogic.class);
        var session = new NetworkSession(
                BoltSecurityPlanManager.insecure(),
                provider,
                retryLogic,
                defaultDatabase(),
                AccessMode.WRITE,
                Collections.emptySet(),
                null,
                -1,
                DEV_NULL_LOGGING,
                mock(BookmarkManager.class),
                Config.defaultConfig().notificationConfig(),
                Config.defaultConfig().notificationConfig(),
                null,
                false,
                mock(AuthTokenManager.class),
                mock());
        return new InternalSession(session);
    }
}
