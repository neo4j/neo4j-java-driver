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

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.ConnectionPoolMetrics;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.bolt.connection.BoltServerAddress;
import reactor.core.publisher.Mono;

@Getter
@Setter
public class GetConnectionPoolMetrics implements TestkitRequest {
    private GetConnectionPoolMetricsBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        return getConnectionPoolMetrics(testkitState);
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return CompletableFuture.completedFuture(getConnectionPoolMetrics(testkitState));
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return Mono.just(getConnectionPoolMetrics(testkitState));
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return processReactive(testkitState);
    }

    private ConnectionPoolMetrics getConnectionPoolMetrics(TestkitState testkitState) {
        var driverHolder = testkitState.getDriverHolder(data.getDriverId());
        @SuppressWarnings("resource")
        var metrics = driverHolder.driver().metrics();
        var poolMetrics = metrics.connectionPoolMetrics().stream()
                .filter(pm -> {
                    // Brute forcing the access via reflections avoid having the InternalConnectionPoolMetrics a public
                    // class
                    BoltServerAddress poolAddress;
                    try {
                        var m = pm.getClass().getDeclaredMethod("getAddress");
                        m.setAccessible(true);
                        poolAddress = (BoltServerAddress) m.invoke(pm);
                    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                        return false;
                    }
                    var address = new BoltServerAddress(data.getAddress());
                    return address.host().equals(poolAddress.host()) && address.port() == poolAddress.port();
                })
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        String.format("Pool metrics for %s are not available", data.getAddress())));
        return createResponse(poolMetrics);
    }

    private ConnectionPoolMetrics createResponse(org.neo4j.driver.ConnectionPoolMetrics poolMetrics) {
        return ConnectionPoolMetrics.builder()
                .data(ConnectionPoolMetrics.ConnectionPoolMetricsBody.builder()
                        .inUse(poolMetrics.inUse())
                        .idle(poolMetrics.idle())
                        .build())
                .build();
    }

    @Setter
    @Getter
    public static class GetConnectionPoolMetricsBody {
        private String driverId;
        private String address;
    }
}
