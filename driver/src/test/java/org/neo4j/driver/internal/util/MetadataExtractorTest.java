/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Query;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.UntrustedServerException;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.summary.InternalInputPosition;
import org.neo4j.driver.summary.DatabaseInfo;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ProfiledPlan;
import org.neo4j.driver.summary.ResultSummary;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
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
import static org.neo4j.driver.internal.util.MetadataExtractor.extractNeo4jServerVersion;
import static org.neo4j.driver.internal.util.ServerVersion.v4_0_0;
import static org.neo4j.driver.summary.QueryType.READ_ONLY;
import static org.neo4j.driver.summary.QueryType.READ_WRITE;
import static org.neo4j.driver.summary.QueryType.SCHEMA_WRITE;
import static org.neo4j.driver.summary.QueryType.WRITE_ONLY;
import static org.neo4j.driver.util.TestUtil.anyServerVersion;

class MetadataExtractorTest
{
    private static final String RESULT_AVAILABLE_AFTER_KEY = "available_after";
    private static final String RESULT_CONSUMED_AFTER_KEY = "consumed_after";

    private final MetadataExtractor extractor = new MetadataExtractor( RESULT_AVAILABLE_AFTER_KEY, RESULT_CONSUMED_AFTER_KEY );

    @Test
    void shouldExtractQueryKeys()
    {
        List<String> keys = asList( "hello", " ", "world", "!" );
        Map<String, Integer> keyIndex = new HashMap<>();
        keyIndex.put( "hello", 0 );
        keyIndex.put( " ", 1 );
        keyIndex.put( "world", 2 );
        keyIndex.put( "!", 3 );

        QueryKeys extracted = extractor.extractQueryKeys( singletonMap( "fields", value( keys ) ) );
        assertEquals( keys, extracted.keys() );
        assertEquals( keyIndex, extracted.keyIndex() );
    }

    @Test
    void shouldExtractEmptyQueryKeysWhenNoneInMetadata()
    {
        QueryKeys extracted = extractor.extractQueryKeys( emptyMap() );
        assertEquals( emptyList(), extracted.keys() );
        assertEquals( emptyMap(), extracted.keyIndex() );
    }

    @Test
    void shouldExtractResultAvailableAfter()
    {
        Map<String,Value> metadata = singletonMap( RESULT_AVAILABLE_AFTER_KEY, value( 424242 ) );
        long extractedResultAvailableAfter = extractor.extractResultAvailableAfter( metadata );
        assertEquals( 424242L, extractedResultAvailableAfter );
    }

    @Test
    void shouldExtractNoResultAvailableAfterWhenNoneInMetadata()
    {
        long extractedResultAvailableAfter = extractor.extractResultAvailableAfter( emptyMap() );
        assertEquals( -1, extractedResultAvailableAfter );
    }

    @Test
    void shouldBuildResultSummaryWithQuery()
    {
        Query query = new Query( "UNWIND range(10, 100) AS x CREATE (:Node {name: $name, x: x})",
                singletonMap( "name", "Apa" ) );

        ResultSummary summary = extractor.extractSummary(query, connectionMock(), 42, emptyMap() );

        assertEquals(query, summary.query() );
    }

    @Test
    void shouldBuildResultSummaryWithServerInfo()
    {
        Connection connection = connectionMock( new BoltServerAddress( "server:42" ), v4_0_0 );

        ResultSummary summary = extractor.extractSummary( query(), connection, 42, emptyMap() );

        assertEquals( "server:42", summary.server().address() );
        assertEquals( "Neo4j/4.0.0", summary.server().version() );
    }

    @Test
    void shouldBuildResultSummaryWithQueryType()
    {
        assertEquals( READ_ONLY, createWithQueryType( value( "r" ) ).queryType() );
        assertEquals( READ_WRITE, createWithQueryType( value( "rw" ) ).queryType() );
        assertEquals( WRITE_ONLY, createWithQueryType( value( "w" ) ).queryType() );
        assertEquals( SCHEMA_WRITE, createWithQueryType( value( "s" ) ).queryType() );

        assertNull( createWithQueryType( null ).queryType() );
    }

    @Test
    void shouldBuildResultSummaryWithCounters()
    {
        Value stats = parameters(
                "nodes-created", value( 42 ),
                "nodes-deleted", value( 4242 ),
                "relationships-created", value( 24 ),
                "relationships-deleted", value( 24 ),
                "properties-set", null,
                "labels-added", value( 5 ),
                "labels-removed", value( 10 ),
                "indexes-added", null,
                "indexes-removed", value( 0 ),
                "constraints-added", null,
                "constraints-removed", value( 2 )
        );

        Map<String,Value> metadata = singletonMap( "stats", stats );

        ResultSummary summary = extractor.extractSummary( query(), connectionMock(), 42, metadata );

        assertEquals( 42, summary.counters().nodesCreated() );
        assertEquals( 4242, summary.counters().nodesDeleted() );
        assertEquals( 24, summary.counters().relationshipsCreated() );
        assertEquals( 24, summary.counters().relationshipsDeleted() );
        assertEquals( 0, summary.counters().propertiesSet() );
        assertEquals( 5, summary.counters().labelsAdded() );
        assertEquals( 10, summary.counters().labelsRemoved() );
        assertEquals( 0, summary.counters().indexesAdded() );
        assertEquals( 0, summary.counters().indexesRemoved() );
        assertEquals( 0, summary.counters().constraintsAdded() );
        assertEquals( 2, summary.counters().constraintsRemoved() );
    }

    @Test
    void shouldBuildResultSummaryWithoutCounters()
    {
        ResultSummary summary = extractor.extractSummary( query(), connectionMock(), 42, emptyMap() );
        assertEquals( EMPTY_STATS, summary.counters() );
    }

    @Test
    void shouldBuildResultSummaryWithPlan()
    {
        Value plan = value( parameters(
                "operatorType", "Projection",
                "args", parameters( "n", 42 ),
                "identifiers", values( "a", "b" ),
                "children", values(
                        parameters(
                                "operatorType", "AllNodeScan",
                                "args", parameters( "x", 4242 ),
                                "identifiers", values( "n", "t", "f" )
                        )
                )
        ) );
        Map<String,Value> metadata = singletonMap( "plan", plan );

        ResultSummary summary = extractor.extractSummary( query(), connectionMock(), 42, metadata );

        assertTrue( summary.hasPlan() );
        assertEquals( "Projection", summary.plan().operatorType() );
        assertEquals( singletonMap( "n", value( 42 ) ), summary.plan().arguments() );
        assertEquals( asList( "a", "b" ), summary.plan().identifiers() );

        List<? extends Plan> children = summary.plan().children();
        assertEquals( 1, children.size() );
        Plan child = children.get( 0 );

        assertEquals( "AllNodeScan", child.operatorType() );
        assertEquals( singletonMap( "x", value( 4242 ) ), child.arguments() );
        assertEquals( asList( "n", "t", "f" ), child.identifiers() );
        assertEquals( 0, child.children().size() );
    }

    @Test
    void shouldBuildResultSummaryWithoutPlan()
    {
        ResultSummary summary = extractor.extractSummary( query(), connectionMock(), 42, emptyMap() );
        assertFalse( summary.hasPlan() );
        assertNull( summary.plan() );
    }

    @Test
    void shouldBuildResultSummaryWithProfiledPlan()
    {
        Value profile = value( parameters(
                "operatorType", "ProduceResult",
                "args", parameters( "a", 42 ),
                "identifiers", values( "a", "b" ),
                "rows", value( 424242 ),
                "dbHits", value( 242424 ),
                "time", value( 999 ),
                "children", values(
                        parameters(
                                "operatorType", "LabelScan",
                                "args", parameters( "x", 1 ),
                                "identifiers", values( "y", "z" ),
                                "rows", value( 2 ),
                                "dbHits", value( 4 )
                        )
                )
        ) );
        Map<String,Value> metadata = singletonMap( "profile", profile );

        ResultSummary summary = extractor.extractSummary( query(), connectionMock(), 42, metadata );

        assertTrue( summary.hasPlan() );
        assertTrue( summary.hasProfile() );
        assertEquals( "ProduceResult", summary.profile().operatorType() );
        assertEquals( singletonMap( "a", value( 42 ) ), summary.profile().arguments() );
        assertEquals( asList( "a", "b" ), summary.profile().identifiers() );
        assertEquals( 424242, summary.profile().records() );
        assertEquals( 242424, summary.profile().dbHits() );
        assertEquals( 999, summary.profile().time() );
        assertFalse( summary.profile().hasPageCacheStats() );
        assertEquals( 0, summary.profile().pageCacheHitRatio() );
        assertEquals( 0, summary.profile().pageCacheMisses() );
        assertEquals( 0, summary.profile().pageCacheHits() );

        List<ProfiledPlan> children = summary.profile().children();
        assertEquals( 1, children.size() );
        ProfiledPlan child = children.get( 0 );

        assertEquals( "LabelScan", child.operatorType() );
        assertEquals( singletonMap( "x", value( 1 ) ), child.arguments() );
        assertEquals( asList( "y", "z" ), child.identifiers() );
        assertEquals( 2, child.records() );
        assertEquals( 4, child.dbHits() );
    }

    @Test
    void shouldBuildResultSummaryWithoutProfiledPlan()
    {
        ResultSummary summary = extractor.extractSummary( query(), connectionMock(), 42, emptyMap() );
        assertFalse( summary.hasProfile() );
        assertNull( summary.profile() );
    }

    @Test
    void shouldBuildResultSummaryWithNotifications()
    {
        Value notification1 = parameters(
                "description", "Almost bad thing",
                "code", "Neo.DummyNotification",
                "title", "A title",
                "severity", "WARNING",
                "position", parameters(
                        "offset", 42,
                        "line", 4242,
                        "column", 424242
                )
        );
        Value notification2 = parameters(
                "description", "Almost good thing",
                "code", "Neo.GoodNotification",
                "title", "Good",
                "severity", "INFO",
                "position", parameters(
                        "offset", 1,
                        "line", 2,
                        "column", 3
                )
        );
        Value notifications = value( notification1, notification2 );
        Map<String,Value> metadata = singletonMap( "notifications", notifications );

        ResultSummary summary = extractor.extractSummary( query(), connectionMock(), 42, metadata );

        assertEquals( 2, summary.notifications().size() );
        Notification firstNotification = summary.notifications().get( 0 );
        Notification secondNotification = summary.notifications().get( 1 );

        assertEquals( "Almost bad thing", firstNotification.description() );
        assertEquals( "Neo.DummyNotification", firstNotification.code() );
        assertEquals( "A title", firstNotification.title() );
        assertEquals( "WARNING", firstNotification.severity() );
        assertEquals( new InternalInputPosition( 42, 4242, 424242 ), firstNotification.position() );

        assertEquals( "Almost good thing", secondNotification.description() );
        assertEquals( "Neo.GoodNotification", secondNotification.code() );
        assertEquals( "Good", secondNotification.title() );
        assertEquals( "INFO", secondNotification.severity() );
        assertEquals( new InternalInputPosition( 1, 2, 3 ), secondNotification.position() );
    }

    @Test
    void shouldBuildResultSummaryWithoutNotifications()
    {
        ResultSummary summary = extractor.extractSummary( query(), connectionMock(), 42, emptyMap() );
        assertEquals( 0, summary.notifications().size() );
    }

    @Test
    void shouldBuildResultSummaryWithResultAvailableAfter()
    {
        int value = 42_000;

        ResultSummary summary = extractor.extractSummary( query(), connectionMock(), value, emptyMap() );

        assertEquals( 42, summary.resultAvailableAfter( TimeUnit.SECONDS ) );
        assertEquals( value, summary.resultAvailableAfter( TimeUnit.MILLISECONDS ) );
    }

    @Test
    void shouldBuildResultSummaryWithResultConsumedAfter()
    {
        int value = 42_000;
        Map<String,Value> metadata = singletonMap( RESULT_CONSUMED_AFTER_KEY, value( value ) );

        ResultSummary summary = extractor.extractSummary( query(), connectionMock(), 42, metadata );

        assertEquals( 42, summary.resultConsumedAfter( TimeUnit.SECONDS ) );
        assertEquals( value, summary.resultConsumedAfter( TimeUnit.MILLISECONDS ) );
    }

    @Test
    void shouldBuildResultSummaryWithoutResultConsumedAfter()
    {
        ResultSummary summary = extractor.extractSummary( query(), connectionMock(), 42, emptyMap() );
        assertEquals( -1, summary.resultConsumedAfter( TimeUnit.SECONDS ) );
        assertEquals( -1, summary.resultConsumedAfter( TimeUnit.MILLISECONDS ) );
    }

    @Test
    void shouldExtractBookmark()
    {
        String bookmarkValue = "neo4j:bookmark:v1:tx123456";

        Bookmark bookmark = extractor.extractBookmarks( singletonMap( "bookmark", value( bookmarkValue ) ) );

        assertEquals( InternalBookmark.parse( bookmarkValue ), bookmark );
    }

    @Test
    void shouldExtractNoBookmarkWhenMetadataContainsNull()
    {
        Bookmark bookmark = extractor.extractBookmarks( singletonMap( "bookmark", null ) );

        assertEquals( InternalBookmark.empty(), bookmark );
    }

    @Test
    void shouldExtractNoBookmarkWhenMetadataContainsNullValue()
    {
        Bookmark bookmark = extractor.extractBookmarks( singletonMap( "bookmark", Values.NULL ) );

        assertEquals( InternalBookmark.empty(), bookmark );
    }

    @Test
    void shouldExtractNoBookmarkWhenMetadataContainsValueOfIncorrectType()
    {
        Bookmark bookmark = extractor.extractBookmarks( singletonMap( "bookmark", value( 42 ) ) );

        assertEquals( InternalBookmark.empty(), bookmark );
    }

    @Test
    void shouldExtractServerVersion()
    {
        Map<String,Value> metadata = singletonMap( "server", value( "Neo4j/3.5.0" ) );

        ServerVersion version = extractNeo4jServerVersion( metadata );

        assertEquals( ServerVersion.v3_5_0, version );
    }


    @Test
    void shouldExtractDatabase()
    {
        // Given
        Map<String,Value> metadata = singletonMap( "db", value( "MyAwesomeDatabase" ) );

        // When
        DatabaseInfo db = extractDatabaseInfo( metadata );

        // Then
        assertEquals( "MyAwesomeDatabase", db.name() );
    }

    @Test
    void shouldDefaultToNullDatabaseName()
    {
        // Given
        Map<String,Value> metadata = singletonMap( "no_db", value( "no_db" ) );

        // When
        DatabaseInfo db = extractDatabaseInfo( metadata );

        // Then
        assertNull( db.name() );
    }

    @Test
    void shouldErrorWhenTypeIsWrong()
    {
        // Given
        Map<String,Value> metadata = singletonMap( "db", value( 10L ) );

        // When
        Uncoercible error = assertThrows( Uncoercible.class, () -> extractDatabaseInfo( metadata ) );

        // Then
        assertThat( error.getMessage(), startsWith( "Cannot coerce INTEGER to Java String" ) );
    }

    @Test
    void shouldFailToExtractServerVersionWhenMetadataDoesNotContainIt()
    {
        assertThrows( UntrustedServerException.class, () -> extractNeo4jServerVersion( singletonMap( "server", Values.NULL ) ) );
        assertThrows( UntrustedServerException.class, () -> extractNeo4jServerVersion( singletonMap( "server", null ) ) );
    }

    @Test
    void shouldFailToExtractServerVersionFromNonNeo4jProduct()
    {
        assertThrows( UntrustedServerException.class, () -> extractNeo4jServerVersion( singletonMap( "server", value( "NotNeo4j/1.2.3" ) ) ) );
    }

    private ResultSummary createWithQueryType(Value typeValue )
    {
        Map<String,Value> metadata = singletonMap( "type", typeValue );
        return extractor.extractSummary( query(), connectionMock(), 42, metadata );
    }

    private static Query query()
    {
        return new Query( "RETURN 1" );
    }

    private static Connection connectionMock()
    {
        return connectionMock( BoltServerAddress.LOCAL_DEFAULT, anyServerVersion() );
    }

    private static Connection connectionMock( BoltServerAddress address, ServerVersion version )
    {
        Connection connection = mock( Connection.class );
        when( connection.serverAddress() ).thenReturn( address );
        when( connection.serverVersion() ).thenReturn( version );
        return connection;
    }
}
