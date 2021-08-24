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
import neo4j.org.testkit.backend.messages.responses.RoutingTable;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.DatabaseNameUtil;
import org.neo4j.driver.internal.cluster.AddressSet;
import org.neo4j.driver.internal.cluster.RoutingTableHandler;
import org.neo4j.driver.internal.cluster.RoutingTableRegistry;

@Setter
@Getter
public class GetRoutingTable implements TestkitRequest
{
    private GetRoutingTableBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        RoutingTableRegistry routingTableRegistry = testkitState.getRoutingTableRegistry().get( data.getDriverId() );
        if ( routingTableRegistry == null )
        {
            throw new IllegalStateException(
                    String.format( "There is no routing table registry for '%s' driver. (It might be a direct driver)", data.getDriverId() ) );
        }

        DatabaseName databaseName = DatabaseNameUtil.database( data.getDatabase() );
        RoutingTableHandler routingTableHandler = routingTableRegistry.getRoutingTableHandler( databaseName ).orElseThrow(
                () -> new IllegalStateException(
                        String.format( "There is no routing table handler for the '%s' database.", databaseName.databaseName().orElse( "null" ) ) ) );

        org.neo4j.driver.internal.cluster.RoutingTable routingTable = routingTableHandler.routingTable();
        Function<AddressSet,List<String>> addressesToStrings = ( addresses ) -> Arrays.stream( addresses.toArray() )
                                                                                      .map( BoltServerAddress::toString ).collect( Collectors.toList() );

        return RoutingTable
                .builder()
                .data( RoutingTable.RoutingTableBody
                               .builder()
                               .database( databaseName.databaseName().orElse( null ) )
                               .routers( addressesToStrings.apply( routingTable.routers() ) )
                               .readers( addressesToStrings.apply( routingTable.readers() ) )
                               .writers( addressesToStrings.apply( routingTable.writers() ) )
                               .build()
                ).build();
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync( TestkitState testkitState )
    {
        return CompletableFuture.completedFuture( process( testkitState ) ) ;
    }

    @Setter
    @Getter
    public static class GetRoutingTableBody
    {
        private String driverId;
        private String database;
    }
}
