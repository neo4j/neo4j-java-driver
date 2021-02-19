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
package org.neo4j.docs.driver;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.summary.ResultSummary;

public class AsyncTransactionFunctionExample extends BaseApplication
{
    public AsyncTransactionFunctionExample( String uri, String user, String password )
    {
        super( uri, user, password );
    }

    public CompletionStage<ResultSummary> printAllProducts()
    {
        // tag::async-transaction-function[]
        String query = "MATCH (p:Product) WHERE p.id = $id RETURN p.title";
        Map<String,Object> parameters = Collections.singletonMap( "id", 0 );

        Session session = driver.session();

        return session.readTransactionAsync( tx ->
                tx.runAsync( query, parameters )
                        .thenCompose( cursor -> cursor.forEachAsync( record ->
                                // asynchronously print every record
                                System.out.println( record.get( 0 ).asString() ) ) )
        );
        // end::async-transaction-function[]
    }
}
