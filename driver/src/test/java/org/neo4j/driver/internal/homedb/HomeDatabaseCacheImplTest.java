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
package org.neo4j.driver.internal.homedb;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.MockitoAnnotations.openMocks;

import java.time.Clock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.neo4j.bolt.connection.BoltConnection;

class HomeDatabaseCacheImplTest {
    HomeDatabaseCacheImpl cache;

    @Mock
    BoltConnection boltConnection;

    @Mock
    Clock clock;

    @BeforeEach
    @SuppressWarnings("resource")
    void beforeEach() {
        openMocks(this);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 100, 1000, 10000})
    void testPruning(int sizeLimit) {
        cache = new HomeDatabaseCacheImpl(sizeLimit, clock);
        given(boltConnection.serverSideRoutingEnabled()).willReturn(true);
        cache.onOpen(boltConnection);

        for (var i = 0L; i < sizeLimit + 1L; i++) {
            var key = HomeDatabaseCacheKey.of(null, String.valueOf(i));
            cache.put(key, String.valueOf(i));
            given(clock.millis()).willReturn(i);
            cache.get(key);
        }

        assertTrue(cache.size() <= sizeLimit);
    }
}
