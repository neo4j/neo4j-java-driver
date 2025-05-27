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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Config;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@ParallelizableIT
class LoggingIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @SuppressWarnings("deprecation")
    @Test
    void logShouldRecordDebugAndTraceInfo() {
        // Given
        var logging = mock(Logging.class);
        var logger = mock(Logger.class);

        when(logging.getLog(any(Class.class))).thenReturn(logger);
        when(logger.isDebugEnabled()).thenReturn(true);
        when(logger.isTraceEnabled()).thenReturn(true);

        var config = Config.builder().withLogging(logging).build();

        try (var driver = GraphDatabase.driver(neo4j.uri(), neo4j.authTokenManager(), config)) {
            // When
            try (var session = driver.session()) {
                session.run("CREATE (a {name:'Cat'})");
            }
        }

        // Then
        verify(logger, atLeastOnce()).debug(anyString(), any(Object[].class));
        verify(logger, atLeastOnce()).trace(anyString(), any());
    }
}
