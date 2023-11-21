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
package neo4j.org.testkit.backend.messages.requests;

import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitClock;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.FakeTimeAck;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

@Setter
@Getter
public class FakeTimeTick extends AbstractBasicTestkitRequest {
    private FakeTimeTickBody data;

    @Override
    protected TestkitResponse processAndCreateResponse(TestkitState testkitState) {
        TestkitClock.INSTANCE.tick(data.getIncrementMs());
        return FakeTimeAck.builder().build();
    }

    @Setter
    @Getter
    public static class FakeTimeTickBody {
        private long incrementMs;
    }
}
