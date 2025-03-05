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
package org.neo4j.driver.internal.metrics;

import java.time.Clock;
import org.neo4j.bolt.connection.ListenerEvent;

final class TimeRecorderListenerEvent implements ListenerEvent<Long> {
    private final Clock clock;
    private long startTime;

    TimeRecorderListenerEvent(Clock clock) {
        this.clock = clock;
    }

    @Override
    public void start() {
        startTime = clock.millis();
    }

    @Override
    public Long getSample() {
        return clock.millis() - startTime;
    }
}
