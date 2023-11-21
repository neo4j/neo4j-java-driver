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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MicrometerTimerListenerEventTest {
    MicrometerTimerListenerEvent event;

    @BeforeEach
    void beforeEach() {
        event = new MicrometerTimerListenerEvent(new SimpleMeterRegistry());
    }

    @Test
    void shouldCreateTimerSampleOnStartAndReturnOnGetSample() {
        // GIVEN
        var initialSample = event.getSample();

        // WHEN
        event.start();

        // THEN
        var sample = event.getSample();
        assertNull(initialSample);
        assertNotNull(sample);
    }
}
