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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.AuthTokenUtil;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.requests.deserializer.TestkitCypherParamDeserializer;
import neo4j.org.testkit.backend.messages.responses.EagerResult;
import neo4j.org.testkit.backend.messages.responses.Record;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.RoutingControl;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class ExecuteQuery implements TestkitRequest {
    private ExecuteQueryBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        @SuppressWarnings("resource")
        var driver = testkitState.getDriverHolder(data.getDriverId()).driver();
        var configBuilder = QueryConfig.builder();
        var routing = data.getConfig().getRouting();
        if (data.getConfig().getRouting() != null) {
            switch (routing) {
                case "w" -> configBuilder.withRouting(RoutingControl.WRITE);
                case "r" -> configBuilder.withRouting(RoutingControl.READ);
                default -> throw new IllegalArgumentException();
            }
        }
        var database = data.getConfig().getDatabase();
        if (database != null) {
            configBuilder.withDatabase(database);
        }
        var impersonatedUser = data.getConfig().getImpersonatedUser();
        if (impersonatedUser != null) {
            configBuilder.withImpersonatedUser(impersonatedUser);
        }
        var bookmarkManagerId = data.getConfig().getBookmarkManagerId();
        if (bookmarkManagerId != null) {
            var bookmarkManager =
                    bookmarkManagerId.equals("-1") ? null : testkitState.getBookmarkManager(bookmarkManagerId);
            configBuilder.withBookmarkManager(bookmarkManager);
        }

        Optional.ofNullable(data.getConfig().getTimeout())
                .map(Duration::ofMillis)
                .ifPresent(configBuilder::withTimeout);

        Optional.ofNullable(data.getConfig().getTxMeta()).ifPresent(configBuilder::withMetadata);

        var authToken = data.getConfig().getAuthorizationToken() != null
                ? AuthTokenUtil.parseAuthToken(data.getConfig().getAuthorizationToken())
                : null;

        var params = data.getParams() != null ? data.getParams() : Collections.<String, Object>emptyMap();
        var eagerResult = driver.executableQuery(data.getCypher())
                .withParameters(params)
                .withConfig(configBuilder.build())
                .withAuthToken(authToken)
                .execute();

        return EagerResult.builder()
                .data(EagerResult.EagerResultBody.builder()
                        .keys(eagerResult.keys())
                        .records(eagerResult.records().stream()
                                .map(record -> Record.RecordBody.builder()
                                        .values(record)
                                        .build())
                                .toList())
                        .summary(SummaryUtil.toSummaryBody(eagerResult.summary()))
                        .build())
                .build();
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Setter
    @Getter
    public static class ExecuteQueryBody {
        private String driverId;
        private String cypher;

        @JsonDeserialize(using = TestkitCypherParamDeserializer.class)
        private Map<String, Object> params;

        private QueryConfigData config;
    }

    @Setter
    @Getter
    public static class QueryConfigData {
        private String database;
        private String routing;
        private String impersonatedUser;
        private String bookmarkManagerId;
        private Long timeout;

        @JsonDeserialize(using = TestkitCypherParamDeserializer.class)
        private Map<String, Serializable> txMeta;

        private AuthorizationToken authorizationToken;
    }
}
