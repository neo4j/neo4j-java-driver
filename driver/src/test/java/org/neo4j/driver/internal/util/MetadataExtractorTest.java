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

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
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
import org.neo4j.driver.summary.GqlNotification;
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
        var rawPlan = value(parameters(
                "operatorType", "Projection",
                "args", parameters("n", 42),
                "identifiers", values("a", "b"),
                "children",
                        values(parameters(
                                "operatorType", "AllNodeScan",
                                "args", parameters("x", 4242),
                                "identifiers", values("n", "t", "f")))));
        var metadata = singletonMap("plan", rawPlan);

        var summary = extractor.extractSummary(query(), connectionMock(), 42, metadata, false, null);

        @SuppressWarnings("deprecation")
        var hasPlan = summary.hasPlan();
        @SuppressWarnings("deprecation")
        var plan = summary.plan();

        assertTrue(hasPlan);
        assertEquals("Projection", plan.operatorType());
        assertEquals(singletonMap("n", value(42)), plan.arguments());
        assertEquals(asList("a", "b"), plan.identifiers());

        var children = plan.children();
        assertEquals(1, children.size());
        var child = children.get(0);

        assertEquals("AllNodeScan", child.operatorType());
        assertEquals(singletonMap("x", value(4242)), child.arguments());
        assertEquals(asList("n", "t", "f"), child.identifiers());
        assertEquals(0, child.children().size());
    }


    @Test
    void shouldBuildResultSummaryWithQueryPlan() {
        var rawPlan = value(parameters(
                "operatorType", "Projection",
                "args", parameters("n", 42),
                "identifiers", values("a", "b"),
                "children",
                values(parameters(
                        "operatorType", "AllNodeScan",
                        "args", parameters("x", 4242),
                        "identifiers", values("n", "t", "f")))));
        var metadata = singletonMap("plan", rawPlan);

        var summary = extractor.extractSummary(query(), connectionMock(), 42, metadata, false, null);

        assertTrue(summary.queryPlan().isPresent());
        var plan = summary.queryPlan().get();

        assertEquals("Projection", plan.operatorType());
        assertEquals(singletonMap("n", value(42)), plan.arguments());
        assertEquals(asList("a", "b"), plan.identifiers());

        var children = plan.children();
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

        @SuppressWarnings("deprecation")
        var hasPlan = summary.hasPlan();
        @SuppressWarnings("deprecation")
        var plan = summary.plan();

        assertFalse(hasPlan);
        assertNull(plan);
    }

    @Test
    void shouldBuildResultSummaryWithoutQueryPlan() {
        var summary = extractor.extractSummary(query(), connectionMock(), 42, emptyMap(), false, null);

        assertTrue(summary.queryPlan().isEmpty());
    }

    @Test
    void shouldBuildResultSummaryWithProfile() {
        var rawProfile = value(parameters(
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
        var metadata = singletonMap("profile", rawProfile);

        var summary = extractor.extractSummary(query(), connectionMock(), 42, metadata, false, null);

        @SuppressWarnings("deprecation")
        var hasPlan = summary.hasPlan();
        @SuppressWarnings("deprecation")
        var hasProfile = summary.hasProfile();
        @SuppressWarnings("deprecation")
        var profile = summary.profile();

        assertTrue(hasPlan);
        assertTrue(hasProfile);
        assertEquals("ProduceResult", profile.operatorType());
        assertEquals(singletonMap("a", value(42)), profile.arguments());
        assertEquals(asList("a", "b"), profile.identifiers());
        assertEquals(424242, profile.records());
        assertEquals(242424, profile.dbHits());
        assertEquals(999, profile.time());
        assertFalse(profile.hasPageCacheStats());
        assertEquals(0, profile.pageCacheHitRatio());
        assertEquals(0, profile.pageCacheMisses());
        assertEquals(0, profile.pageCacheHits());

        var children = profile.children();
        assertEquals(1, children.size());
        var child = children.get(0);

        assertEquals("LabelScan", child.operatorType());
        assertEquals(singletonMap("x", value(1)), child.arguments());
        assertEquals(asList("y", "z"), child.identifiers());
        assertEquals(2, child.records());
        assertEquals(4, child.dbHits());
    }

    @Test
    void shouldBuildResultSummaryWithQueryProfile() {
        var rawProfile = value(parameters(
                "operatorType",
                "ProduceResult",
                "args",
                parameters("a", 42),
                "identifiers",
                values("a", "b"),
                "rows",
                value(424242),
                "dbHits",
                value(242424),
                "time",
                value(999),
                "children",
                values(parameters(
                        "operatorType", "LabelScan",
                        "args", parameters("x", 1),
                        "identifiers", values("y", "z"),
                        "rows", value(2),
                        "dbHits", value(4)))));
        var metadata = singletonMap("profile", rawProfile);

        var summary = extractor.extractSummary(query(), connectionMock(), 42, metadata, false, null);

        assertTrue(summary.queryPlan().isPresent());
        assertTrue(summary.queryProfile().isPresent());
        var profile = summary.queryProfile().get();

        assertEquals("ProduceResult", profile.operatorType());
        assertEquals(singletonMap("a", value(42)), profile.arguments());
        assertEquals(asList("a", "b"), profile.identifiers());
        assertEquals(OptionalLong.of(424242), profile.rows());
        assertEquals(OptionalLong.of(242424), profile.dbHits());
        assertEquals(Optional.of(Duration.ofNanos(999)), profile.time());
        assertEquals(OptionalDouble.empty(), profile.pageCacheHitRatio());
        assertEquals(OptionalLong.empty(), profile.pageCacheMisses());
        assertEquals(OptionalLong.empty(), profile.pageCacheHits());

        var children = profile.children();
        assertEquals(1, children.size());
        var child = children.get(0);

        assertEquals("LabelScan", child.operatorType());
        assertEquals(singletonMap("x", value(1)), child.arguments());
        assertEquals(asList("y", "z"), child.identifiers());
        assertEquals(OptionalLong.of(2), child.rows());
        assertEquals(OptionalLong.of(4), child.dbHits());
        assertEquals(Optional.empty(), child.time());
    }

    @Test
    void shouldBuildResultSummaryWithoutProfile() {
        var summary = extractor.extractSummary(query(), connectionMock(), 42, emptyMap(), false, null);

        @SuppressWarnings("deprecation")
        var hasProfile = summary.hasProfile();
        @SuppressWarnings("deprecation")
        var profile = summary.profile();

        assertFalse(hasProfile);
        assertNull(profile);
    }

    @Test
    void shouldBuildResultSummaryWithoutQueryProfile() {
        var summary = extractor.extractSummary(query(), connectionMock(), 42, emptyMap(), false, null);

        assertTrue(summary.queryProfile().isEmpty());
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

        assertEquals(2, summary.gqlStatusObjects().size());
        assertEquals(2, summary.notifications().size());
        var gqlStatusObjectsIterator = summary.gqlStatusObjects().iterator();
        var firstNotification = summary.notifications().get(0);
        var firstGqlStatusObject = (GqlNotification) gqlStatusObjectsIterator.next();
        var secondNotification = summary.notifications().get(1);
        var secondGqlStatusObject = (GqlNotification) gqlStatusObjectsIterator.next();

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

        assertEquals("Almost bad thing", firstGqlStatusObject.statusDescription());
        assertEquals(
                NotificationSeverity.WARNING, firstGqlStatusObject.severity().get());
        assertEquals("WARNING", firstGqlStatusObject.rawSeverity().get());
        assertEquals(
                NotificationCategory.DEPRECATION,
                firstGqlStatusObject.classification().get());
        assertEquals("DEPRECATION", firstGqlStatusObject.rawClassification().get());
        assertEquals(
                new InternalInputPosition(42, 4242, 424242),
                firstGqlStatusObject.position().get());
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
                firstGqlStatusObject.diagnosticRecord());

        assertEquals("Almost good thing", secondNotification.description());
        assertEquals("Neo.GoodNotification", secondNotification.code());
        assertEquals("Good", secondNotification.title());
        assertFalse(secondNotification.severityLevel().isPresent());
        assertEquals("INFO", secondNotification.rawSeverityLevel().get());
        assertNull(secondNotification.position());

        assertEquals("Almost good thing", secondGqlStatusObject.statusDescription());
        assertFalse(secondGqlStatusObject.severity().isPresent());
        assertEquals("INFO", secondGqlStatusObject.rawSeverity().get());
        assertFalse(secondGqlStatusObject.classification().isPresent());
        assertFalse(secondGqlStatusObject.rawClassification().isPresent());
        assertTrue(secondGqlStatusObject.position().isEmpty());
        assertEquals(
                Map.of(
                        "OPERATION", Values.value(""),
                        "OPERATION_CODE", Values.value("0"),
                        "CURRENT_SCHEMA", Values.value("/"),
                        "_severity", Values.value("INFO")),
                secondGqlStatusObject.diagnosticRecord());
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
        assertEquals(1, summary.notifications().size());
        var gqlStatusObjectsIterator = summary.gqlStatusObjects().iterator();
        var firstGqlStatusObject = (GqlNotification) gqlStatusObjectsIterator.next();
        var firstNotification = summary.notifications().get(0);
        var secondGqlStatusObject = gqlStatusObjectsIterator.next();

        assertEquals("gql_status", firstGqlStatusObject.gqlStatus());
        assertEquals("status_description", firstGqlStatusObject.statusDescription());
        assertEquals(
                NotificationSeverity.WARNING, firstGqlStatusObject.severity().get());
        assertEquals("WARNING", firstGqlStatusObject.rawSeverity().get());
        assertEquals(
                NotificationCategory.SECURITY,
                firstGqlStatusObject.classification().get());
        assertEquals("SECURITY", firstGqlStatusObject.rawClassification().get());
        assertEquals(
                new InternalInputPosition(42, 4242, 424242),
                firstGqlStatusObject.position().get());
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

        assertEquals("notification_description", firstNotification.description());
        assertEquals("neo4j_code", firstNotification.code());
        assertEquals("title", firstNotification.title());
        assertEquals(
                NotificationSeverity.WARNING, firstNotification.severityLevel().get());
        assertEquals("WARNING", firstNotification.rawSeverityLevel().get());
        assertEquals(NotificationCategory.SECURITY, firstNotification.category().get());
        assertEquals("SECURITY", firstNotification.rawCategory().get());
        assertEquals(new InternalInputPosition(42, 4242, 424242), firstNotification.position());

        assertFalse(secondGqlStatusObject instanceof GqlNotification);
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
    }

    @SuppressWarnings("deprecation")
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
