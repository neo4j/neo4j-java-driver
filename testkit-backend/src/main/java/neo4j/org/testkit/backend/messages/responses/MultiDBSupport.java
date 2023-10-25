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
package neo4j.org.testkit.backend.messages.responses;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class MultiDBSupport implements TestkitResponse {
    private final MultiDBSupportBody data;

    @Override
    public String testkitName() {
        return "MultiDBSupport";
    }

    @Getter
    @Builder
    public static class MultiDBSupportBody {
        private final String id;

        private final boolean available;
    }
}
