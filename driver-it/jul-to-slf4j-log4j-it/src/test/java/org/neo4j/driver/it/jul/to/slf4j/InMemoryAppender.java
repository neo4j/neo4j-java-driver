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

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;

final class InMemoryAppender extends AbstractAppender {
    private final List<LogEvent> logEvents = new ArrayList<>();

    InMemoryAppender(String name, Filter filter) {
        super(name, filter, PatternLayout.createDefaultLayout(), false, null);
        start();
    }

    @Override
    public void append(LogEvent event) {
        logEvents.add(event);
    }

    List<LogEvent> getLogs() {
        return logEvents;
    }
}
