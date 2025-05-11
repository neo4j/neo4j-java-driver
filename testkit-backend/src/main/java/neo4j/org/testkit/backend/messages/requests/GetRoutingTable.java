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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.RoutingTable;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.DatabaseNameUtil;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class GetRoutingTable implements TestkitRequest {
    private static final Function<List<BoltServerAddress>, List<String>> ADDRESSES_TO_STRINGS =
            (addresses) -> addresses.stream()
                    .map(address -> String.format("%s:%d", address.host(), address.port()))
                    .collect(Collectors.toList());

    private GetRoutingTableBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        var routingTableRegistry = testkitState.getRoutingTableRegistry().get(data.getDriverId());
        if (routingTableRegistry == null) {
            throw new IllegalStateException(String.format(
                    "There is no routing table registry for '%s' driver. (It might be a direct driver)",
                    data.getDriverId()));
        }

        var databaseName = DatabaseNameUtil.database(data.getDatabase());
        var routingTableHandler = routingTableRegistry
                .getRoutingTableHandler(databaseName)
                .orElseThrow(() -> new IllegalStateException(String.format(
                        "There is no routing table handler for the '%s' database.",
                        databaseName.databaseName().orElse("null"))));

        var routingTable = routingTableHandler.routingTable();

        return RoutingTable.builder()
                .data(RoutingTable.RoutingTableBody.builder()
                        .database(databaseName.databaseName().orElse(null))
                        .routers(ADDRESSES_TO_STRINGS.apply(routingTable.routers()))
                        .readers(ADDRESSES_TO_STRINGS.apply(routingTable.readers()))
                        .writers(ADDRESSES_TO_STRINGS.apply(routingTable.writers()))
                        .build())
                .build();
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return CompletableFuture.completedFuture(process(testkitState));
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return Mono.just(process(testkitState));
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return processReactive(testkitState);
    }

    @Setter
    @Getter
    public static class GetRoutingTableBody {
        private String driverId;
        private String database;
    }
}
