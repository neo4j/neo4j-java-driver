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
package org.neo4j.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.driver.internal.EagerResultTransformer;
import org.neo4j.driver.testutil.TestUtil;

class QueryConfigTest {
    @Test
    void shouldReturnDefaultValues() {
        var config = QueryConfig.defaultConfig();
        var manager = mock(BookmarkManager.class);

        assertEquals(RoutingControl.WRITERS, config.routing());
        assertTrue(config.resultTransformer() instanceof EagerResultTransformer);
        assertTrue(config.database().isEmpty());
        assertTrue(config.impersonatedUser().isEmpty());
        assertEquals(manager, config.bookmarkManager(manager).get());
    }

    @ParameterizedTest
    @EnumSource(RoutingControl.class)
    void shouldUpdateRouting(RoutingControl routing) {
        var config = QueryConfig.builder().withRouting(routing).build();
        assertEquals(routing, config.routing());
    }

    @Test
    void shouldNotAllowNullRouting() {
        assertThrows(NullPointerException.class, () -> QueryConfig.builder().withRouting(null));
    }

    @Test
    void shouldUpdateResultTransformer() {
        ResultTransformer<String> transformer = ignored -> "result";
        var config = QueryConfig.builder(transformer).build();
        assertEquals(transformer, config.resultTransformer());
    }

    @Test
    void shouldNotAllowNullResultTransformer() {
        assertThrows(NullPointerException.class, () -> QueryConfig.builder(null));
    }

    @Test
    void shouldUpdateDatabaseName() {
        var database = "testing";
        var config = QueryConfig.builder().withDatabase(database).build();
        assertTrue(config.database().isPresent());
        assertEquals(database, config.database().get());
    }

    @Test
    void shouldNotAllowNullDatabaseName() {
        assertThrows(NullPointerException.class, () -> QueryConfig.builder().withDatabase(null));
    }

    @Test
    void shouldUpdateImpersonatedUser() {
        var user = "testing";
        var config = QueryConfig.builder().withImpersonatedUser(user).build();
        assertTrue(config.impersonatedUser().isPresent());
        assertEquals(user, config.impersonatedUser().get());
    }

    @Test
    void shouldAllowNotNullImpersonatedUser() {
        assertThrows(NullPointerException.class, () -> QueryConfig.builder().withImpersonatedUser(null));
    }

    @Test
    void shouldUpdateBookmarkManager() {
        var defaultManager = mock(BookmarkManager.class);
        var manager = mock(BookmarkManager.class);
        var config = QueryConfig.builder().withBookmarkManager(manager).build();
        assertTrue(config.bookmarkManager(defaultManager).isPresent());
        assertEquals(manager, config.bookmarkManager(defaultManager).get());
    }

    @Test
    void shouldAllowNullBookmarkManager() {
        var config = QueryConfig.builder().withBookmarkManager(null).build();
        assertTrue(config.bookmarkManager(mock(BookmarkManager.class)).isEmpty());
    }

    @Test
    void shouldSerialize() throws Exception {
        var originalConfig = QueryConfig.defaultConfig();
        var deserializedConfig = TestUtil.serializeAndReadBack(originalConfig, QueryConfig.class);
        var defaultManager = mock(BookmarkManager.class);

        assertEquals(originalConfig.routing(), deserializedConfig.routing());
        assertEquals(
                originalConfig.resultTransformer().getClass(),
                deserializedConfig.resultTransformer().getClass());
        assertEquals(originalConfig.database(), deserializedConfig.database());
        assertEquals(originalConfig.impersonatedUser(), deserializedConfig.impersonatedUser());
        assertEquals(
                originalConfig.bookmarkManager(defaultManager), deserializedConfig.bookmarkManager(defaultManager));
    }
}
