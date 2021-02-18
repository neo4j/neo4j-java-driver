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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.requests.deserializer.TestkitCypherParamDeserializer;
import neo4j.org.testkit.backend.messages.responses.Result;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import org.neo4j.driver.Query;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;

@Setter
@Getter
@NoArgsConstructor
public class SessionRun implements TestkitRequest
{
    private SessionRunBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        Session session = testkitState.getSessionStates().get( data.getSessionId() ).getSession();
        Query query = Optional.ofNullable( data.params )
                              .map( params -> new Query( data.cypher, data.params ) )
                              .orElseGet( () -> new Query( data.cypher ) );
        TransactionConfig.Builder transactionConfig = TransactionConfig.builder();
        Optional.ofNullable( data.getTxMeta() ).ifPresent( transactionConfig::withMetadata );
        Optional.ofNullable( data.getTimeout() ).ifPresent( to -> transactionConfig.withTimeout( Duration.ofMillis( to ) ) );
        org.neo4j.driver.Result result = session.run( query, transactionConfig.build() );
        String newId = testkitState.newId();
        testkitState.getResults().put( newId, result );

        return Result.builder().data( Result.ResultBody.builder().id( newId ).build() ).build();
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class SessionRunBody
    {
        @JsonDeserialize( using = TestkitCypherParamDeserializer.class )
        private Map<String,Object> params;

        private String sessionId;
        private String cypher;
        private Map<String,Object> txMeta;
        private Integer timeout;

    }
}
