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
import neo4j.org.testkit.backend.messages.responses.MultiDBSupport;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletionStage;

@Setter
@Getter
public class CheckMultiDBSupport implements TestkitRequest
{
    private CheckMultiDBSupportBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        String driverId = data.getDriverId();
        boolean available = testkitState.getDriverHolder( driverId ).getDriver().supportsMultiDb();
        return createResponse( available );
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync( TestkitState testkitState )
    {
        return testkitState.getDriverHolder( data.getDriverId() )
                           .getDriver()
                           .supportsMultiDbAsync()
                           .thenApply( this::createResponse );
    }

    @Override
    public Mono<TestkitResponse> processRx( TestkitState testkitState )
    {
        return Mono.fromCompletionStage( processAsync( testkitState ) );
    }

    private MultiDBSupport createResponse( boolean available )
    {
        return MultiDBSupport.builder()
                             .data( MultiDBSupport.MultiDBSupportBody.builder()
                                                                     .available( available )
                                                                     .build() )
                             .build();
    }

    @Setter
    @Getter
    public static class CheckMultiDBSupportBody
    {
        private String driverId;
    }
}
