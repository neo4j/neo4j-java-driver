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
import lombok.NoArgsConstructor;
import lombok.Setter;
import neo4j.org.testkit.backend.SessionState;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

@Setter
@Getter
@NoArgsConstructor
public class RetryablePositive implements TestkitRequest
{
    private RetryablePositiveBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        SessionState sessionState = testkitState.getSessionStates().get( data.sessionId );
        if ( sessionState == null )
        {
            throw new RuntimeException( "Could not find session" );
        }
        sessionState.setRetryableState( 1 );
        return null;
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class RetryablePositiveBody
    {
        private String sessionId;
    }
}
