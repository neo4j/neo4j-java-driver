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
package org.neo4j.driver.internal;

import java.util.ResourceBundle;
import org.neo4j.driver.Logger;

public class BoltLogger implements System.Logger {
    @SuppressWarnings("deprecation")
    private final Logger logger;

    public BoltLogger(@SuppressWarnings("deprecation") Logger logger) {
        this.logger = logger;
    }

    @Override
    public String getName() {
        throw new RuntimeException(new UnsupportedOperationException("getName() not supported"));
    }

    @Override
    public boolean isLoggable(Level level) {
        return switch (level) {
            case ALL -> logger.isTraceEnabled() && logger.isDebugEnabled();
            case TRACE -> logger.isTraceEnabled();
            case DEBUG -> logger.isDebugEnabled();
            case INFO, WARNING, ERROR -> true;
            case OFF -> false;
        };
    }

    @Override
    public void log(Level level, ResourceBundle bundle, String msg, Throwable thrown) {
        switch (level) {
            case ALL, OFF -> {}
            case TRACE -> logger.trace(msg, thrown);
            case DEBUG -> logger.debug(msg, thrown);
            case INFO -> logger.info(msg, thrown);
            case WARNING -> logger.warn(msg, thrown);
            case ERROR -> logger.error(msg, thrown);
        }
    }

    @Override
    public void log(Level level, ResourceBundle bundle, String format, Object... params) {
        switch (level) {
            case TRACE -> logger.trace(format, params);
            case DEBUG -> logger.debug(format, params);
            case INFO -> logger.info(format, params);
            case WARNING -> logger.warn(format, params);
            case ALL, OFF, ERROR -> {}
        }
    }
}
