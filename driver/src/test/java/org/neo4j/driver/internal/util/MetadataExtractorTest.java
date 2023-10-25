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
package org.neo4j.driver.internal.util;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.Values.values;
import static org.neo4j.driver.internal.summary.InternalSummaryCounters.EMPTY_STATS;
import static org.neo4j.driver.internal.util.MetadataExtractor.extractDatabaseInfo;
import static org.neo4j.driver.internal.util.MetadataExtractor.extractServer;
import static org.neo4j.driver.summary.QueryType.READ_ONLY;
import static org.neo4j.driver.summary.QueryType.READ_WRITE;
import static org.neo4j.driver.summary.QueryType.SCHEMA_WRITE;
import static org.neo4j.driver.summary.QueryType.WRITE_ONLY;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.NotificationCategory;
import org.neo4j.driver.NotificationSeverity;
import org.neo4j.driver.Query;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.UntrustedServerException;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.messaging.v43.BoltProtocolV43;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.summary.InternalInputPosition;
import org.neo4j.driver.summary.ResultSummary;

class MetadataExtractorTest {
    private static final String RESULT_AVAILABLE_AFTER_KEY = "available_after";
    private static final String RESULT_CONSUMED_AFTER_KEY = "consumed_after";

    private final MetadataExtractor extractor =
            new MetadataExtractor(RESULT_AVAILABLE_AFTER_KEY, RESULT_CONSUMED_AFTER_KEY);

    @Test
    void shouldExtractQueryKeys() {
        var keys = asList("hello", " ", "world", "!");
        Map<String, Integer> keyIndex = new HashMap<>();
        keyIndex.put("hello", 0);
        keyIndex.put(" ", 1);
        keyIndex.put("world", 2);
        keyIndex.put("!", 3);

        var extracted = extractor.extractQueryKeys(singletonMap("fields", value(keys)));
        assertEquals(keys, extracted.keys());
        assertEquals(keyIndex, extracted.keyIndex());
    }

    @Test
    void shouldExtractEmptyQueryKeysWhenNoneInMetadata() {
        var extracted = extractor.extractQueryKeys(emptyMap());
        assertEquals(emptyList(), extracted.keys());
        assertEquals(emptyMap(), extracted.keyIndex());
    }

    @Test
    void shouldExtractResultAvailableAfter() {
        var metadata = singletonMap(RESULT_AVAILABLE_AFTER_KEY, value(424242));
        var extractedResultAvailableAfter = extractor.extractResultAvailableAfter(metadata);
        assertEquals(424242L, extractedResultAvailableAfter);
    }

    @Test
    void shouldExtractNoResultAvailableAfterWhenNoneInMetadata() {
        var extractedResultAvailableAfter = extractor.extractResultAvailableAfter(emptyMap());
        assertEquals(-1, extractedResultAvailableAfter);
    }

    @Test
    void shouldBuildResultSummaryWithQuery() {
        var query =
                new Query("UNWIND range(10, 100) AS x CREATE (:Node {name: $name, x: x})", singletonMap("name", "Apa"));

        var summary = extractor.extractSummary(query, connectionMock(), 42, emptyMap());

        assertEquals(query, summary.query());
    }

    @Test
    void shouldBuildResultSummaryWithServerAddress() {
        var connection = connectionMock(new BoltServerAddress("server:42"));

        var summary = extractor.extractSummary(query(), connection, 42, emptyMap());

        assertEquals("server:42", summary.server().address());
    }

    @Test
    void shouldBuildResultSummaryWithQueryType() {
        assertEquals(READ_ONLY, createWithQueryType(value("r")).queryType());
        assertEquals(READ_WRITE, createWithQueryType(value("rw")).queryType());
        assertEquals(WRITE_ONLY, createWithQueryType(value("w")).queryType());
        assertEquals(SCHEMA_WRITE, createWithQueryType(value("s")).queryType());

        assertNull(createWithQueryType(null).queryType());
    }

    @Test
    void shouldBuildResultSummaryWithCounters() {
        var stats = parameters(
                "nodes-created", value(42),
                "nodes-deleted", value(4242),
                "relationships-created", value(24),
                "relationships-deleted", value(24),
                "properties-set", null,
                "labels-added", value(5),
                "labels-removed", value(10),
                "indexes-added", null,
                "indexes-removed", value(0),
                "constraints-added", null,
                "constraints-removed", value(2));

        var metadata = singletonMap("stats", stats);

        var summary = extractor.extractSummary(query(), connectionMock(), 42, metadata);

        assertEquals(42, summary.counters().nodesCreated());
        assertEquals(4242, summary.counters().nodesDeleted());
        assertEquals(24, summary.counters().relationshipsCreated());
        assertEquals(24, summary.counters().relationshipsDeleted());
        assertEquals(0, summary.counters().propertiesSet());
        assertEquals(5, summary.counters().labelsAdded());
        assertEquals(10, summary.counters().labelsRemoved());
        assertEquals(0, summary.counters().indexesAdded());
        assertEquals(0, summary.counters().indexesRemoved());
        assertEquals(0, summary.counters().constraintsAdded());
        assertEquals(2, summary.counters().constraintsRemoved());
    }

    @Test
    void shouldBuildResultSummaryWithoutCounters() {
        var summary = extractor.extractSummary(query(), connectionMock(), 42, emptyMap());
        assertEquals(EMPTY_STATS, summary.counters());
    }

    @Test
    void shouldBuildResultSummaryWithPlan() {
        var plan = value(parameters(
                "operatorType", "Projection",
                "args", parameters("n", 42),
                "identifiers", values("a", "b"),
                "children",
                        values(parameters(
                                "operatorType", "AllNodeScan",
                                "args", parameters("x", 4242),
                                "identifiers", values("n", "t", "f")))));
        var metadata = singletonMap("plan", plan);

        var summary = extractor.extractSummary(query(), connectionMock(), 42, metadata);

        assertTrue(summary.hasPlan());
        assertEquals("Projection", summary.plan().operatorType());
        assertEquals(singletonMap("n", value(42)), summary.plan().arguments());
        assertEquals(asList("a", "b"), summary.plan().identifiers());

        var children = summary.plan().children();
        assertEquals(1, children.size());
        var child = children.get(0);

        assertEquals("AllNodeScan", child.operatorType());
        assertEquals(singletonMap("x", value(4242)), child.arguments());
        assertEquals(asList("n", "t", "f"), child.identifiers());
        assertEquals(0, child.children().size());
    }

    @Test
    void shouldBuildResultSummaryWithoutPlan() {
        var summary = extractor.extractSummary(query(), connectionMock(), 42, emptyMap());
        assertFalse(summary.hasPlan());
        assertNull(summary.plan());
    }

    @Test
    void shouldBuildResultSummaryWithProfiledPlan() {
        var profile = value(parameters(
                "operatorType", "ProduceResult",
                "args", parameters("a", 42),
                "identifiers", values("a", "b"),
                "rows", value(424242),
                "dbHits", value(242424),
                "time", value(999),
                "children",
                        values(parameters(
                                "operatorType", "LabelScan",
                                "args", parameters("x", 1),
                                "identifiers", values("y", "z"),
                                "rows", value(2),
                                "dbHits", value(4)))));
        var metadata = singletonMap("profile", profile);

        var summary = extractor.extractSummary(query(), connectionMock(), 42, metadata);

        assertTrue(summary.hasPlan());
        assertTrue(summary.hasProfile());
        assertEquals("ProduceResult", summary.profile().operatorType());
        assertEquals(singletonMap("a", value(42)), summary.profile().arguments());
        assertEquals(asList("a", "b"), summary.profile().identifiers());
        assertEquals(424242, summary.profile().records());
        assertEquals(242424, summary.profile().dbHits());
        assertEquals(999, summary.profile().time());
        assertFalse(summary.profile().hasPageCacheStats());
        assertEquals(0, summary.profile().pageCacheHitRatio());
        assertEquals(0, summary.profile().pageCacheMisses());
        assertEquals(0, summary.profile().pageCacheHits());

        var children = summary.profile().children();
        assertEquals(1, children.size());
        var child = children.get(0);

        assertEquals("LabelScan", child.operatorType());
        assertEquals(singletonMap("x", value(1)), child.arguments());
        assertEquals(asList("y", "z"), child.identifiers());
        assertEquals(2, child.records());
        assertEquals(4, child.dbHits());
    }

    @Test
    void shouldBuildResultSummaryWithoutProfiledPlan() {
        var summary = extractor.extractSummary(query(), connectionMock(), 42, emptyMap());
        assertFalse(summary.hasProfile());
        assertNull(summary.profile());
    }

    @Test
    @SuppressWarnings({"deprecation", "OptionalGetWithoutIsPresent"})
    void shouldBuildResultSummaryWithNotifications() {
        var notification1 = parameters(
                "description", "Almost bad thing",
                "code", "Neo.DummyNotification",
                "title", "A title",
                "severity", "WARNING",
                "category", "DEPRECATION",
                "position",
                        parameters(
                                "offset", 42,
                                "line", 4242,
                                "column", 424242));
        var notification2 = parameters(
                "description", "Almost good thing",
                "code", "Neo.GoodNotification",
                "title", "Good",
                "severity", "INFO",
                "position",
                        parameters(
                                "offset", 1,
                                "line", 2,
                                "column", 3));
        var notifications = value(notification1, notification2);
        var metadata = singletonMap("notifications", notifications);

        var summary = extractor.extractSummary(query(), connectionMock(), 42, metadata);

        assertEquals(2, summary.notifications().size());
        var firstNotification = summary.notifications().get(0);
        var secondNotification = summary.notifications().get(1);

        assertEquals("Almost bad thing", firstNotification.description());
        assertEquals("Neo.DummyNotification", firstNotification.code());
        assertEquals("A title", firstNotification.title());
        assertEquals("WARNING", firstNotification.severity());
        assertEquals(
                NotificationSeverity.WARNING, firstNotification.severityLevel().get());
        assertEquals("WARNING", firstNotification.rawSeverityLevel().get());
        assertEquals(
                NotificationCategory.DEPRECATION, firstNotification.category().get());
        assertEquals("DEPRECATION", firstNotification.rawCategory().get());
        assertEquals(new InternalInputPosition(42, 4242, 424242), firstNotification.position());

        assertEquals("Almost good thing", secondNotification.description());
        assertEquals("Neo.GoodNotification", secondNotification.code());
        assertEquals("Good", secondNotification.title());
        assertEquals("INFO", secondNotification.severity());
        assertEquals(new InternalInputPosition(1, 2, 3), secondNotification.position());
    }

    @Test
    void shouldBuildResultSummaryWithoutNotifications() {
        var summary = extractor.extractSummary(query(), connectionMock(), 42, emptyMap());
        assertEquals(0, summary.notifications().size());
    }

    @Test
    void shouldBuildResultSummaryWithResultAvailableAfter() {
        var value = 42_000;

        var summary = extractor.extractSummary(query(), connectionMock(), value, emptyMap());

        assertEquals(42, summary.resultAvailableAfter(TimeUnit.SECONDS));
        assertEquals(value, summary.resultAvailableAfter(TimeUnit.MILLISECONDS));
    }

    @Test
    void shouldBuildResultSummaryWithResultConsumedAfter() {
        var value = 42_000;
        var metadata = singletonMap(RESULT_CONSUMED_AFTER_KEY, value(value));

        var summary = extractor.extractSummary(query(), connectionMock(), 42, metadata);

        assertEquals(42, summary.resultConsumedAfter(TimeUnit.SECONDS));
        assertEquals(value, summary.resultConsumedAfter(TimeUnit.MILLISECONDS));
    }

    @Test
    void shouldBuildResultSummaryWithoutResultConsumedAfter() {
        var summary = extractor.extractSummary(query(), connectionMock(), 42, emptyMap());
        assertEquals(-1, summary.resultConsumedAfter(TimeUnit.SECONDS));
        assertEquals(-1, summary.resultConsumedAfter(TimeUnit.MILLISECONDS));
    }

    @Test
    void shouldExtractBookmark() {
        var bookmarkValue = "neo4j:bookmark:v1:tx123456";

        var bookmark = MetadataExtractor.extractBookmark(singletonMap("bookmark", value(bookmarkValue)));

        assertEquals(InternalBookmark.parse(bookmarkValue), bookmark);
    }

    @Test
    void shouldExtractNoBookmarkWhenMetadataContainsNull() {
        var bookmark = MetadataExtractor.extractBookmark(singletonMap("bookmark", null));

        assertNull(bookmark);
    }

    @Test
    void shouldExtractNoBookmarkWhenMetadataContainsNullValue() {
        var bookmark = MetadataExtractor.extractBookmark(singletonMap("bookmark", Values.NULL));

        assertNull(bookmark);
    }

    @Test
    void shouldExtractNoBookmarkWhenMetadataContainsValueOfIncorrectType() {
        var bookmark = MetadataExtractor.extractBookmark(singletonMap("bookmark", value(42)));

        assertNull(bookmark);
    }

    @Test
    void shouldExtractServer() {
        var agent = "Neo4j/3.5.0";
        var metadata = singletonMap("server", value(agent));

        var serverValue = extractServer(metadata);

        assertEquals(agent, serverValue.asString());
    }

    @Test
    void shouldExtractDatabase() {
        // Given
        var metadata = singletonMap("db", value("MyAwesomeDatabase"));

        // When
        var db = extractDatabaseInfo(metadata);

        // Then
        assertEquals("MyAwesomeDatabase", db.name());
    }

    @Test
    void shouldDefaultToNullDatabaseName() {
        // Given
        var metadata = singletonMap("no_db", value("no_db"));

        // When
        var db = extractDatabaseInfo(metadata);

        // Then
        assertNull(db.name());
    }

    @Test
    void shouldErrorWhenTypeIsWrong() {
        // Given
        var metadata = singletonMap("db", value(10L));

        // When
        var error = assertThrows(Uncoercible.class, () -> extractDatabaseInfo(metadata));

        // Then
        assertThat(error.getMessage(), startsWith("Cannot coerce INTEGER to Java String"));
    }

    @Test
    void shouldFailToExtractServerVersionWhenMetadataDoesNotContainIt() {
        assertThrows(UntrustedServerException.class, () -> extractServer(singletonMap("server", Values.NULL)));
        assertThrows(UntrustedServerException.class, () -> extractServer(singletonMap("server", null)));
    }

    @Test
    void shouldFailToExtractServerVersionFromNonNeo4jProduct() {
        assertThrows(
                UntrustedServerException.class, () -> extractServer(singletonMap("server", value("NotNeo4j/1.2.3"))));
    }

    private ResultSummary createWithQueryType(Value typeValue) {
        var metadata = singletonMap("type", typeValue);
        return extractor.extractSummary(query(), connectionMock(), 42, metadata);
    }

    private static Query query() {
        return new Query("RETURN 1");
    }

    private static Connection connectionMock() {
        return connectionMock(BoltServerAddress.LOCAL_DEFAULT);
    }

    private static Connection connectionMock(BoltServerAddress address) {
        var connection = mock(Connection.class);
        when(connection.serverAddress()).thenReturn(address);
        when(connection.protocol()).thenReturn(BoltProtocolV43.INSTANCE);
        when(connection.serverAgent()).thenReturn("Neo4j/4.2.5");
        return connection;
    }
}
