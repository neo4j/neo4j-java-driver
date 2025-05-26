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
import static org.neo4j.driver.summary.QueryType.READ_ONLY;
import static org.neo4j.driver.summary.QueryType.READ_WRITE;
import static org.neo4j.driver.summary.QueryType.SCHEMA_WRITE;
import static org.neo4j.driver.summary.QueryType.WRITE_ONLY;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.driver.NotificationCategory;
import org.neo4j.driver.NotificationSeverity;
import org.neo4j.driver.Query;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnection;
import org.neo4j.driver.internal.summary.InternalInputPosition;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.ResultSummary;

class MetadataExtractorTest {
    private static final String RESULT_CONSUMED_AFTER_KEY = "consumed_after";

    private final MetadataExtractor extractor = new MetadataExtractor(RESULT_CONSUMED_AFTER_KEY);

    @Test
    void shouldBuildResultSummaryWithQuery() {
        var query =
                new Query("UNWIND range(10, 100) AS x CREATE (:Node {name: $name, x: x})", singletonMap("name", "Apa"));

        var summary = extractor.extractSummary(query, connectionMock(), 42, emptyMap(), false, null);

        assertEquals(query, summary.query());
    }

    @Test
    void shouldBuildResultSummaryWithServerAddress() {
        var connection = connectionMock(new BoltServerAddress("server:42"));

        var summary = extractor.extractSummary(query(), connection, 42, emptyMap(), false, null);

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

        var summary = extractor.extractSummary(query(), connectionMock(), 42, metadata, false, null);

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
        var summary = extractor.extractSummary(query(), connectionMock(), 42, emptyMap(), false, null);
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

        var summary = extractor.extractSummary(query(), connectionMock(), 42, metadata, false, null);

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
        var summary = extractor.extractSummary(query(), connectionMock(), 42, emptyMap(), false, null);
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

        var summary = extractor.extractSummary(query(), connectionMock(), 42, metadata, false, null);

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
        var summary = extractor.extractSummary(query(), connectionMock(), 42, emptyMap(), false, null);
        assertFalse(summary.hasProfile());
        assertNull(summary.profile());
    }

    @Test
    @SuppressWarnings({"deprecation", "OptionalGetWithoutIsPresent"})
    void shouldBuildResultSummaryWithNotifications() {
        var notification1 = parameters(
                "description",
                "Almost bad thing",
                "code",
                "Neo.DummyNotification",
                "title",
                "A title",
                "severity",
                "WARNING",
                "category",
                "DEPRECATION",
                "position",
                parameters(
                        "offset", 42,
                        "line", 4242,
                        "column", 424242));
        var notification2 = parameters(
                "description", "Almost good thing",
                "code", "Neo.GoodNotification",
                "title", "Good",
                "severity", "INFO");
        var notifications = value(notification1, notification2);
        var metadata = singletonMap("notifications", notifications);

        var summary = extractor.extractSummary(query(), connectionMock(), 42, metadata, true, null);

        assertEquals(2, summary.notifications().size());
        var firstNotification = summary.notifications().get(0);
        var secondNotification = summary.notifications().get(1);

        assertEquals("Almost bad thing", firstNotification.description());
        assertEquals("Neo.DummyNotification", firstNotification.code());
        assertEquals("A title", firstNotification.title());
        assertEquals(
                NotificationSeverity.WARNING, firstNotification.severityLevel().get());
        assertEquals("WARNING", firstNotification.rawSeverityLevel().get());
        assertEquals(
                NotificationCategory.DEPRECATION, firstNotification.category().get());
        assertEquals("DEPRECATION", firstNotification.rawCategory().get());
        assertEquals(new InternalInputPosition(42, 4242, 424242), firstNotification.position());
        assertEquals(
                Map.of(
                        "OPERATION",
                        Values.value(""),
                        "OPERATION_CODE",
                        Values.value("0"),
                        "CURRENT_SCHEMA",
                        Values.value("/"),
                        "_severity",
                        Values.value("WARNING"),
                        "_classification",
                        Values.value("DEPRECATION"),
                        "_position",
                        parameters(
                                "offset", 42,
                                "line", 4242,
                                "column", 424242)),
                firstNotification.diagnosticRecord());

        assertEquals("Almost good thing", secondNotification.description());
        assertEquals("Neo.GoodNotification", secondNotification.code());
        assertEquals("Good", secondNotification.title());
        assertFalse(secondNotification.severityLevel().isPresent());
        assertEquals("INFO", secondNotification.rawSeverityLevel().get());
        assertTrue(secondNotification.inputPosition().isEmpty());
        assertNull(secondNotification.position());
        assertEquals(
                Map.of(
                        "OPERATION", Values.value(""),
                        "OPERATION_CODE", Values.value("0"),
                        "CURRENT_SCHEMA", Values.value("/"),
                        "_severity", Values.value("INFO")),
                secondNotification.diagnosticRecord());

        assertEquals(2, summary.gqlStatusObjects().size());
        var gqlStatusObjectsIterator = summary.gqlStatusObjects().iterator();
        var firstGqlStatusObject = (Notification) gqlStatusObjectsIterator.next();
        var secondGqlStatusObject = (Notification) gqlStatusObjectsIterator.next();
        assertEquals(firstNotification, firstGqlStatusObject);
        assertEquals(secondNotification, secondGqlStatusObject);
    }

    @Test
    @SuppressWarnings({"deprecation", "OptionalGetWithoutIsPresent"})
    void shouldBuildResultSummaryWithGqlStatusObjects() {
        var gqlStatusObject1 = parameters(
                "gql_status",
                "gql_status",
                "status_description",
                "status_description",
                "neo4j_code",
                "neo4j_code",
                "title",
                "title",
                "description",
                "notification_description",
                "diagnostic_record",
                parameters(
                        "_severity",
                        "WARNING",
                        "_classification",
                        "SECURITY",
                        "_position",
                        parameters(
                                "offset", 42,
                                "line", 4242,
                                "column", 424242)));
        var gqlStatusObject2 = parameters(
                "gql_status",
                "gql_status",
                "status_description",
                "status_description",
                "diagnostic_record",
                parameters(
                        "_severity", "WARNING",
                        "_classification", "SECURITY"));
        var gqlStatusObjects = value(gqlStatusObject1, gqlStatusObject2);
        var metadata = singletonMap("statuses", gqlStatusObjects);

        var summary = extractor.extractSummary(query(), connectionMock(), 42, metadata, false, null);

        assertEquals(2, summary.gqlStatusObjects().size());
        var gqlStatusObjectsIterator = summary.gqlStatusObjects().iterator();
        var firstGqlStatusObject = (Notification) gqlStatusObjectsIterator.next();
        var secondGqlStatusObject = gqlStatusObjectsIterator.next();

        assertEquals("gql_status", firstGqlStatusObject.gqlStatus());
        assertEquals("status_description", firstGqlStatusObject.statusDescription());
        assertEquals("notification_description", firstGqlStatusObject.description());
        assertEquals("neo4j_code", firstGqlStatusObject.code());
        assertEquals("title", firstGqlStatusObject.title());
        assertEquals(
                NotificationSeverity.WARNING,
                firstGqlStatusObject.severityLevel().get());
        assertEquals("WARNING", firstGqlStatusObject.rawSeverityLevel().get());
        assertEquals(
                NotificationCategory.SECURITY, firstGqlStatusObject.category().get());
        assertEquals("SECURITY", firstGqlStatusObject.rawCategory().get());
        assertEquals(new InternalInputPosition(42, 4242, 424242), firstGqlStatusObject.position());
        assertEquals(
                Map.of(
                        "OPERATION",
                        Values.value(""),
                        "OPERATION_CODE",
                        Values.value("0"),
                        "CURRENT_SCHEMA",
                        Values.value("/"),
                        "_severity",
                        Values.value("WARNING"),
                        "_classification",
                        Values.value("SECURITY"),
                        "_position",
                        parameters(
                                "offset", 42,
                                "line", 4242,
                                "column", 424242)),
                firstGqlStatusObject.diagnosticRecord());

        assertFalse(secondGqlStatusObject instanceof Notification);
        assertEquals("gql_status", secondGqlStatusObject.gqlStatus());
        assertEquals("status_description", secondGqlStatusObject.statusDescription());
        assertEquals(
                Map.of(
                        "OPERATION", Values.value(""),
                        "OPERATION_CODE", Values.value("0"),
                        "CURRENT_SCHEMA", Values.value("/"),
                        "_severity", Values.value("WARNING"),
                        "_classification", Values.value("SECURITY")),
                secondGqlStatusObject.diagnosticRecord());

        assertEquals(1, summary.notifications().size());
        assertEquals(firstGqlStatusObject, summary.notifications().get(0));
    }

    @Test
    void shouldBuildResultSummaryWithoutNotifications() {
        var summary = extractor.extractSummary(query(), connectionMock(), 42, emptyMap(), false, null);
        assertEquals(0, summary.notifications().size());
    }

    @Test
    void shouldBuildResultSummaryWithResultAvailableAfter() {
        var value = 42_000;

        var summary = extractor.extractSummary(query(), connectionMock(), value, emptyMap(), false, null);

        assertEquals(42, summary.resultAvailableAfter(TimeUnit.SECONDS));
        assertEquals(value, summary.resultAvailableAfter(TimeUnit.MILLISECONDS));
    }

    @Test
    void shouldBuildResultSummaryWithResultConsumedAfter() {
        var value = 42_000;
        var metadata = singletonMap(RESULT_CONSUMED_AFTER_KEY, value(value));

        var summary = extractor.extractSummary(query(), connectionMock(), 42, metadata, false, null);

        assertEquals(42, summary.resultConsumedAfter(TimeUnit.SECONDS));
        assertEquals(value, summary.resultConsumedAfter(TimeUnit.MILLISECONDS));
    }

    @Test
    void shouldBuildResultSummaryWithoutResultConsumedAfter() {
        var summary = extractor.extractSummary(query(), connectionMock(), 42, emptyMap(), false, null);
        assertEquals(-1, summary.resultConsumedAfter(TimeUnit.SECONDS));
        assertEquals(-1, summary.resultConsumedAfter(TimeUnit.MILLISECONDS));
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

    private ResultSummary createWithQueryType(Value typeValue) {
        var metadata = singletonMap("type", typeValue);
        return extractor.extractSummary(query(), connectionMock(), 42, metadata, false, null);
    }

    private static Query query() {
        return new Query("RETURN 1");
    }

    private static DriverBoltConnection connectionMock() {
        return connectionMock(BoltServerAddress.LOCAL_DEFAULT);
    }

    private static DriverBoltConnection connectionMock(BoltServerAddress address) {
        var connection = mock(DriverBoltConnection.class);
        when(connection.serverAddress()).thenReturn(address);
        when(connection.protocolVersion()).thenReturn(new BoltProtocolVersion(4, 3));
        when(connection.serverAgent()).thenReturn("Neo4j/4.2.5");
        return connection;
    }
}
