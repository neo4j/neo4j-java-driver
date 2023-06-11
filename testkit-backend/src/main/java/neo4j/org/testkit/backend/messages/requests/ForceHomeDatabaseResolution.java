/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.Driver;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

@Setter
@Getter
public class ForceHomeDatabaseResolution extends AbstractBasicTestkitRequest {
    private ForceHomeDatabaseResolutionBody data;

    @Override
    protected TestkitResponse processAndCreateResponse(TestkitState testkitState) {
        var driver = testkitState.getDriverHolder(data.getDriverId()).getDriver();
        driver.forceHomeDatabaseResolution();
        return Driver.builder()
                .data(Driver.DriverBody.builder().id(data.getDriverId()).build())
                .build();
    }

    @Setter
    @Getter
    public static class ForceHomeDatabaseResolutionBody {
        private String driverId;
    }
}
