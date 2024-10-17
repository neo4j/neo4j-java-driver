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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.request;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.bolt.api.AccessMode.READ;
import static org.neo4j.driver.internal.bolt.api.AccessMode.WRITE;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.TransactionMetadataBuilder.buildMetadata;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.bolt.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.NotificationClassification;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.NotificationSeverity;

public class TransactionMetadataBuilderTest {
    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldHaveCorrectMetadata(AccessMode mode) {
        var bookmarks = new HashSet<>(asList("neo4j:bookmark:v1:tx11", "neo4j:bookmark:v1:tx52"));

        Map<String, Value> txMetadata = new HashMap<>();
        txMetadata.put("foo", value("bar"));
        txMetadata.put("baz", value(111));
        txMetadata.put("time", value(LocalDateTime.now()));

        var txTimeout = Duration.ofSeconds(7);

        var metadata = buildMetadata(
                txTimeout,
                txMetadata,
                defaultDatabase(),
                mode,
                bookmarks,
                null,
                null,
                null,
                false,
                NoopLoggingProvider.INSTANCE);

        Map<String, Value> expectedMetadata = new HashMap<>();
        expectedMetadata.put(
                "bookmarks", value(bookmarks.stream().map(Values::value).collect(Collectors.toSet())));
        expectedMetadata.put("tx_timeout", value(7000));
        expectedMetadata.put("tx_metadata", value(txMetadata));
        if (mode == READ) {
            expectedMetadata.put("mode", value("r"));
        }

        assertEquals(expectedMetadata, metadata);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "foo", "data"})
    void shouldHaveCorrectMetadataForDatabaseName(String databaseName) {
        var bookmarks = new HashSet<>(asList("neo4j:bookmark:v1:tx11", "neo4j:bookmark:v1:tx52"));

        Map<String, Value> txMetadata = new HashMap<>();
        txMetadata.put("foo", value("bar"));
        txMetadata.put("baz", value(111));
        txMetadata.put("time", value(LocalDateTime.now()));

        var txTimeout = Duration.ofSeconds(7);

        var metadata = buildMetadata(
                txTimeout,
                txMetadata,
                database(databaseName),
                WRITE,
                bookmarks,
                null,
                null,
                null,
                false,
                NoopLoggingProvider.INSTANCE);

        Map<String, Value> expectedMetadata = new HashMap<>();
        expectedMetadata.put(
                "bookmarks", value(bookmarks.stream().map(Values::value).collect(Collectors.toSet())));
        expectedMetadata.put("tx_timeout", value(7000));
        expectedMetadata.put("tx_metadata", value(txMetadata));
        expectedMetadata.put("db", value(databaseName));

        assertEquals(expectedMetadata, metadata);
    }

    @Test
    void shouldNotHaveMetadataForDatabaseNameWhenIsNull() {
        var metadata = buildMetadata(
                null,
                null,
                defaultDatabase(),
                WRITE,
                Collections.emptySet(),
                null,
                null,
                null,
                false,
                NoopLoggingProvider.INSTANCE);
        assertTrue(metadata.isEmpty());
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    void shouldIncludeNotificationConfig() {
        var metadata = buildMetadata(
                null,
                null,
                defaultDatabase(),
                WRITE,
                Collections.emptySet(),
                null,
                null,
                new NotificationConfig(
                        NotificationSeverity.WARNING,
                        Set.of(NotificationClassification.valueOf("UNSUPPORTED").get())),
                false,
                NoopLoggingProvider.INSTANCE);

        var expectedMetadata = new HashMap<String, Value>();
        expectedMetadata.put("notifications_minimum_severity", value("WARNING"));
        expectedMetadata.put("notifications_disabled_classifications", value(Set.of("UNSUPPORTED")));
        assertEquals(expectedMetadata, metadata);
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 1_000_001, 100_500_000, 100_700_000, 1_000_000_001})
    void shouldRoundUpFractionalTimeoutAndLog(long nanosValue) {
        // given
        var logging = mock(LoggingProvider.class);
        var logger = mock(System.Logger.class);
        given(logging.getLog(TransactionMetadataBuilder.class)).willReturn(logger);

        // when
        var metadata = buildMetadata(
                Duration.ofNanos(nanosValue),
                null,
                defaultDatabase(),
                WRITE,
                Collections.emptySet(),
                null,
                null,
                null,
                false,
                logging);

        // then
        var expectedMetadata = new HashMap<String, Value>();
        var expectedMillis = nanosValue / 1_000_000 + 1;
        expectedMetadata.put("tx_timeout", value(expectedMillis));
        assertEquals(expectedMetadata, metadata);
        then(logging).should().getLog(TransactionMetadataBuilder.class);
        then(logger)
                .should()
                .log(
                        System.Logger.Level.INFO,
                        "The transaction timeout has been rounded up to next millisecond value since the config had a fractional millisecond value");
    }

    @Test
    void shouldNotLogWhenRoundingDoesNotHappen() {
        // given
        var logging = mock(LoggingProvider.class);
        var logger = mock(System.Logger.class);
        given(logging.getLog(TransactionMetadataBuilder.class)).willReturn(logger);
        var timeout = 1000;

        // when
        var metadata = buildMetadata(
                Duration.ofMillis(timeout),
                null,
                defaultDatabase(),
                WRITE,
                Collections.emptySet(),
                null,
                null,
                null,
                false,
                logging);

        // then
        var expectedMetadata = new HashMap<String, Value>();
        expectedMetadata.put("tx_timeout", value(timeout));
        assertEquals(expectedMetadata, metadata);
        then(logging).shouldHaveNoInteractions();
        then(logger).shouldHaveNoInteractions();
    }
}
