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
package org.neo4j.driver.internal.bolt.api;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RoutingContextTest {
    @Test
    void emptyContextIsNotDefined() {
        assertFalse(RoutingContext.EMPTY.isDefined());
    }

    @Test
    void emptyContextInEmptyMap() {
        assertTrue(RoutingContext.EMPTY.toMap().isEmpty());
    }

    @Test
    void uriWithoutQueryIsParsedToEmptyContext() {
        testEmptyRoutingContext(URI.create("neo4j://localhost:7687/"));
    }

    @Test
    void uriWithEmptyQueryIsParsedToEmptyContext() {
        testEmptyRoutingContext(URI.create("neo4j://localhost:7687?"));
        testEmptyRoutingContext(URI.create("neo4j://localhost:7687/?"));
    }

    @Test
    void uriWithQueryIsParsed() {
        var uri = URI.create("neo4j://localhost:7687/?key1=value1&key2=value2&key3=value3");
        var context = new RoutingContext(uri);

        assertTrue(context.isDefined());
        Map<String, String> expectedMap = new HashMap<>();
        expectedMap.put("key1", "value1");
        expectedMap.put("key2", "value2");
        expectedMap.put("key3", "value3");
        expectedMap.put("address", "localhost:7687");
        assertEquals(expectedMap, context.toMap());
    }

    @Test
    void boltUriDisablesServerSideRouting() {
        var uri = URI.create("bolt://localhost:7687/?key1=value1&key2=value2&key3=value3");
        var context = new RoutingContext(uri);

        assertFalse(context.isServerRoutingEnabled());
    }

    @Test
    void neo4jUriEnablesServerSideRouting() {
        var uri = URI.create("neo4j://localhost:7687/?key1=value1&key2=value2&key3=value3");
        var context = new RoutingContext(uri);

        assertTrue(context.isServerRoutingEnabled());
    }

    @Test
    void throwsForInvalidUriQuery() {
        testIllegalUri(URI.create("neo4j://localhost:7687/?justKey"));
    }

    @Test
    void throwsForInvalidUriQueryKey() {
        testIllegalUri(URI.create("neo4j://localhost:7687/?=value1&key2=value2"));
    }

    @Test
    void throwsForInvalidUriQueryValue() {
        testIllegalUri(URI.create("neo4j://localhost:7687/key1?=value1&key2="));
    }

    @Test
    void throwsForDuplicatedUriQueryParameters() {
        testIllegalUri(URI.create("neo4j://localhost:7687/?key1=value1&key2=value2&key1=value2"));
    }

    @Test
    void mapRepresentationIsUnmodifiable() {
        var uri = URI.create("neo4j://localhost:7687/?key1=value1");
        var context = new RoutingContext(uri);

        Map<String, String> expectedMap = new HashMap<>();
        expectedMap.put("key1", "value1");
        expectedMap.put("address", "localhost:7687");

        assertEquals(expectedMap, context.toMap());

        assertThrows(UnsupportedOperationException.class, () -> context.toMap().put("key2", "value2"));
        assertEquals(expectedMap, context.toMap());
    }

    @Test
    void populateAddressWithDefaultPort() {
        var uri = URI.create("neo4j://localhost/");
        var context = new RoutingContext(uri);

        assertEquals(singletonMap("address", "localhost:7687"), context.toMap());
    }

    @Test
    void throwsExceptionIfAddressIsUsedInContext() {
        var uri = URI.create("neo4j://localhost:7687/?key1=value1&address=someaddress:9010");

        var e = assertThrows(IllegalArgumentException.class, () -> new RoutingContext(uri));
        assertEquals("The key 'address' is reserved for routing context.", e.getMessage());
    }

    private static void testIllegalUri(URI uri) {
        assertThrows(IllegalArgumentException.class, () -> new RoutingContext(uri));
    }

    private static void testEmptyRoutingContext(URI uri) {
        var context = new RoutingContext(uri);

        assertFalse(context.isDefined());
        assertEquals(singletonMap("address", "localhost:7687"), context.toMap());
    }
}
