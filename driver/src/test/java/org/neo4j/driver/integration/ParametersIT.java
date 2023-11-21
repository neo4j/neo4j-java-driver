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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.Values.ofInteger;
import static org.neo4j.driver.Values.ofValue;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.util.ValueFactory.emptyNodeValue;
import static org.neo4j.driver.internal.util.ValueFactory.emptyRelationshipValue;
import static org.neo4j.driver.internal.util.ValueFactory.filledPathValue;
import static org.neo4j.driver.types.TypeSystem.getDefault;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.testutil.ParallelizableIT;
import org.neo4j.driver.testutil.SessionExtension;
import org.neo4j.driver.testutil.TestUtil;

@ParallelizableIT
class ParametersIT {
    private static final int LONG_VALUE_SIZE = 1_000_000;

    @RegisterExtension
    static final SessionExtension session = new SessionExtension();

    @Test
    void shouldBeAbleToSetAndReturnBooleanProperty() {
        // When
        var result = session.run("CREATE (a {value:$value}) RETURN a.value", parameters("value", true));

        // Then
        for (var record : result.list()) {
            var value = record.get("a.value");
            assertThat(value.hasType(getDefault().BOOLEAN()), equalTo(true));
            assertThat(value.asBoolean(), equalTo(true));
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnByteProperty() {
        // When
        var result = session.run("CREATE (a {value:$value}) RETURN a.value", parameters("value", (byte) 1));

        // Then
        for (var record : result.list()) {
            var value = record.get("a.value");
            assertThat(value.hasType(getDefault().INTEGER()), equalTo(true));
            assertThat(value.asLong(), equalTo(1L));
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnShortProperty() {
        // When
        var result = session.run("CREATE (a {value:$value}) RETURN a.value", parameters("value", (short) 1));

        // Then
        for (var record : result.list()) {
            var value = record.get("a.value");
            assertThat(value.hasType(getDefault().INTEGER()), equalTo(true));
            assertThat(value.asLong(), equalTo(1L));
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnIntegerProperty() {
        // When
        var result = session.run("CREATE (a {value:$value}) RETURN a.value", parameters("value", 1));

        // Then
        for (var record : result.list()) {
            var value = record.get("a.value");
            assertThat(value.hasType(getDefault().INTEGER()), equalTo(true));
            assertThat(value.asLong(), equalTo(1L));
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnLongProperty() {
        // When
        var result = session.run("CREATE (a {value:$value}) RETURN a.value", parameters("value", 1L));

        // Then
        for (var record : result.list()) {
            var value = record.get("a.value");
            assertThat(value.hasType(getDefault().INTEGER()), equalTo(true));
            assertThat(value.asLong(), equalTo(1L));
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnDoubleProperty() {
        // When
        var result = session.run("CREATE (a {value:$value}) RETURN a.value", parameters("value", 6.28));

        // Then
        for (var record : result.list()) {
            var value = record.get("a.value");
            assertThat(value.hasType(getDefault().FLOAT()), equalTo(true));
            assertThat(value.asDouble(), equalTo(6.28));
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnBytesProperty() {
        testBytesProperty(new byte[0]);
        for (var i = 0; i < 16; i++) {
            var length = (int) Math.pow(2, i);
            testBytesProperty(randomByteArray(length));
            testBytesProperty(randomByteArray(length - 1));
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnStringProperty() {
        testStringProperty("");
        testStringProperty("π≈3.14");
        testStringProperty("Mjölnir");
        testStringProperty("*** Hello World! ***");
    }

    @Test
    void shouldBeAbleToSetAndReturnBooleanArrayProperty() {
        // When
        var arrayValue = new boolean[] {true, true, true};
        var result = session.run("CREATE (a {value:$value}) RETURN a.value", parameters("value", arrayValue));

        // Then
        for (var record : result.list()) {
            var value = record.get("a.value");
            assertThat(value.hasType(getDefault().LIST()), equalTo(true));
            assertThat(value.size(), equalTo(3));
            for (var item : value.asList(ofValue())) {
                assertThat(item.hasType(getDefault().BOOLEAN()), equalTo(true));
                assertThat(item.asBoolean(), equalTo(true));
            }
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnIntegerArrayProperty() {
        // When
        var arrayValue = new int[] {42, 42, 42};
        var result = session.run("CREATE (a {value:$value}) RETURN a.value", parameters("value", arrayValue));

        // Then
        for (var record : result.list()) {
            var value = record.get("a.value");
            assertThat(value.hasType(getDefault().LIST()), equalTo(true));
            assertThat(value.size(), equalTo(3));
            for (var item : value.asList(ofValue())) {
                assertThat(item.hasType(getDefault().INTEGER()), equalTo(true));
                assertThat(item.asLong(), equalTo(42L));
            }
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnDoubleArrayProperty() {
        // When
        var arrayValue = new double[] {6.28, 6.28, 6.28};
        var result = session.run("CREATE (a {value:$value}) RETURN a.value", parameters("value", arrayValue));

        // Then
        for (var record : result.list()) {
            var value = record.get("a.value");
            assertThat(value.hasType(getDefault().LIST()), equalTo(true));
            assertThat(value.size(), equalTo(3));
            for (var item : value.asList(ofValue())) {
                assertThat(item.hasType(getDefault().FLOAT()), equalTo(true));
                assertThat(item.asDouble(), equalTo(6.28));
            }
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnStringArrayProperty() {
        testStringArrayContaining("cat");
        testStringArrayContaining("Mjölnir");
    }

    private static void testStringArrayContaining(String str) {
        var arrayValue = new String[] {str, str, str};

        var result = session.run("CREATE (a {value:$value}) RETURN a.value", parameters("value", arrayValue));

        // Then
        for (var record : result.list()) {
            var value = record.get("a.value");
            assertThat(value.hasType(getDefault().LIST()), equalTo(true));
            assertThat(value.size(), equalTo(3));
            for (var item : value.asList(ofValue())) {
                assertThat(item.hasType(getDefault().STRING()), equalTo(true));
                assertThat(item.asString(), equalTo(str));
            }
        }
    }

    @Test
    void shouldHandleLargeString() {
        // Given
        var bigStr = new char[1024 * 10];
        for (var i = 0; i < bigStr.length; i += 4) {
            bigStr[i] = 'a';
            bigStr[i + 1] = 'b';
            bigStr[i + 2] = 'c';
            bigStr[i + 3] = 'd';
        }

        var bigString = new String(bigStr);

        // When
        var val =
                session.run("RETURN $p AS p", parameters("p", bigString)).peek().get("p");

        // Then
        assertThat(val.asString(), equalTo(bigString));
    }

    @Test
    void shouldBeAbleToSetAndReturnBooleanPropertyWithinMap() {
        // When
        var result =
                session.run("CREATE (a {value:$value.v}) RETURN a.value", parameters("value", parameters("v", true)));

        // Then
        for (var record : result.list()) {
            var value = record.get("a.value");
            assertThat(value.hasType(getDefault().BOOLEAN()), equalTo(true));
            assertThat(value.asBoolean(), equalTo(true));
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnIntegerPropertyWithinMap() {
        // When
        var result =
                session.run("CREATE (a {value:$value.v}) RETURN a.value", parameters("value", parameters("v", 42)));

        // Then
        for (var record : result.list()) {
            var value = record.get("a.value");
            assertThat(value.hasType(getDefault().INTEGER()), equalTo(true));
            assertThat(value.asLong(), equalTo(42L));
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnDoublePropertyWithinMap() {
        // When
        var result =
                session.run("CREATE (a {value:$value.v}) RETURN a.value", parameters("value", parameters("v", 6.28)));

        // Then
        for (var record : result.list()) {
            var value = record.get("a.value");
            assertThat(value.hasType(getDefault().FLOAT()), equalTo(true));
            assertThat(value.asDouble(), equalTo(6.28));
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnStringPropertyWithinMap() {
        // When
        var result = session.run(
                "CREATE (a {value:$value.v}) RETURN a.value", parameters("value", parameters("v", "Mjölnir")));

        // Then
        for (var record : result.list()) {
            var value = record.get("a.value");
            assertThat(value.hasType(getDefault().STRING()), equalTo(true));
            assertThat(value.asString(), equalTo("Mjölnir"));
        }
    }

    @Test
    void settingInvalidParameterTypeShouldThrowHelpfulError() {
        var e = assertThrows(ClientException.class, () -> session.run("anything", parameters("k", new Object())));
        assertEquals("Unable to convert java.lang.Object to Neo4j Value.", e.getMessage());
    }

    @Test
    void settingInvalidParameterTypeDirectlyShouldThrowHelpfulError() {
        var e = assertThrows(IllegalArgumentException.class, () -> session.run("anything", emptyNodeValue()));
        assertEquals(
                "The parameters should be provided as Map type. Unsupported parameters type: NODE", e.getMessage());
    }

    @Test
    void shouldNotBePossibleToUseNodeAsParameterInMapValue() {
        // GIVEN
        Value node = emptyNodeValue();
        Map<String, Value> params = new HashMap<>();
        params.put("a", node);
        var mapValue = new MapValue(params);

        // WHEN
        expectIOExceptionWithMessage(mapValue, "Unknown type: NODE");
    }

    @Test
    void shouldNotBePossibleToUseRelationshipAsParameterViaMapValue() {
        // GIVEN
        Value relationship = emptyRelationshipValue();
        Map<String, Value> params = new HashMap<>();
        params.put("a", relationship);
        var mapValue = new MapValue(params);

        // WHEN
        expectIOExceptionWithMessage(mapValue, "Unknown type: RELATIONSHIP");
    }

    @Test
    void shouldNotBePossibleToUsePathAsParameterViaMapValue() {
        // GIVEN
        Value path = filledPathValue();
        Map<String, Value> params = new HashMap<>();
        params.put("a", path);
        var mapValue = new MapValue(params);

        // WHEN
        expectIOExceptionWithMessage(mapValue, "Unknown type: PATH");
    }

    @Test
    void shouldSendAndReceiveLongString() {
        var string = TestUtil.randomString(LONG_VALUE_SIZE);
        testSendAndReceiveValue(string);
    }

    @Test
    void shouldSendAndReceiveLongListOfLongs() {
        var longs = ThreadLocalRandom.current().longs(LONG_VALUE_SIZE).boxed().collect(toList());

        testSendAndReceiveValue(longs);
    }

    @Test
    void shouldSendAndReceiveLongArrayOfBytes() {
        var bytes = new byte[LONG_VALUE_SIZE];
        ThreadLocalRandom.current().nextBytes(bytes);

        testSendAndReceiveValue(bytes);
    }

    @Test
    void shouldAcceptStreamsAsQueryParameters() {
        var stream = Stream.of(1, 2, 3, 4, 5, 42);

        var result = session.run("RETURN $value", singletonMap("value", stream));
        var receivedValue = result.single().get(0);

        assertEquals(asList(1, 2, 3, 4, 5, 42), receivedValue.asList(ofInteger()));
    }

    private static void testBytesProperty(byte[] array) {
        var result = session.run("CREATE (a {value:$value}) RETURN a.value", parameters("value", array));

        for (var record : result.list()) {
            var value = record.get("a.value");
            assertThat(value.hasType(getDefault().BYTES()), equalTo(true));
            assertThat(value.asByteArray(), equalTo(array));
        }
    }

    private static void testStringProperty(String string) {
        var result = session.run("CREATE (a {value:$value}) RETURN a.value", parameters("value", string));

        for (var record : result.list()) {
            var value = record.get("a.value");
            assertThat(value.hasType(getDefault().STRING()), equalTo(true));
            assertThat(value.asString(), equalTo(string));
        }
    }

    private static byte[] randomByteArray(int length) {
        var result = new byte[length];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    private static void expectIOExceptionWithMessage(Value value, String message) {
        var e = assertThrows(ServiceUnavailableException.class, () -> session.run("RETURN {a}", value)
                .consume());
        var cause = e.getCause();
        assertThat(cause, instanceOf(IOException.class));
        assertThat(cause.getMessage(), equalTo(message));
    }

    private static void testSendAndReceiveValue(Object value) {
        var result = session.run("RETURN $value", singletonMap("value", value));
        var receivedValue = result.single().get(0).asObject();
        assertArrayEquals(new Object[] {value}, new Object[] {receivedValue});
    }
}
