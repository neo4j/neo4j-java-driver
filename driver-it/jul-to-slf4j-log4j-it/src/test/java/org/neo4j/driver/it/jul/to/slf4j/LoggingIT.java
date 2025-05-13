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
package org.neo4j.driver.it.jul.to.slf4j;

import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.internal.DriverFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

class LoggingIT {

    private InMemoryAppender appender;

    @BeforeEach
    void setup() {
        java.util.logging.LogManager.getLogManager().reset();
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        var julLogger = java.util.logging.Logger.getLogger(DriverFactory.class.getName());
        julLogger.setLevel(java.util.logging.Level.INFO);

        Configurator.setLevel(DriverFactory.class.getName(), Level.INFO);
        var logger = (Logger) LogManager.getLogger(DriverFactory.class.getName());
        appender = new InMemoryAppender("Appender", null);
        logger.addAppender(appender);
        logger.setAdditive(false);
    }

    @Test
    void shouldLog() {
        var driver = GraphDatabase.driver("bolt://localhost:7687");

        assertFalse(appender.getLogs().isEmpty());
    }
}
