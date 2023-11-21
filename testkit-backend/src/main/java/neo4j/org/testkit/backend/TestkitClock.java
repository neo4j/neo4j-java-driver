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
package neo4j.org.testkit.backend;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

public class TestkitClock extends Clock {
    public static final TestkitClock INSTANCE = new TestkitClock(Clock.systemUTC());
    private final Clock clock;
    private long fakeTime = 0L;
    private boolean fakeMode = false;

    private TestkitClock(Clock clock) {
        this.clock = clock;
    }

    public void setFakeTime(boolean fakeMode) {
        this.fakeMode = fakeMode;
    }

    public void tick(long ms) {
        fakeTime += ms;
    }

    public void reset() {
        fakeTime = 0;
    }

    @Override
    public ZoneId getZone() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(fakeMode ? fakeTime : clock.millis());
    }
}
