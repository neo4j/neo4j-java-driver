/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.v1.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionConfig;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.driver.v1.util.SessionExtension;

import static java.time.Duration.ofSeconds;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V3;

@EnabledOnNeo4jWith( BOLT_V3 )
class SessionBoltV3IT
{
    @RegisterExtension
    static final SessionExtension session = new SessionExtension();

    @Test
    void shouldSetTransactionMetadata()
    {
        Map<String,Object> metadata = new HashMap<>();
        metadata.put( "a", "hello world" );
        metadata.put( "b", LocalDate.now() );
        metadata.put( "c", asList( true, false, true ) );

        TransactionConfig config = TransactionConfig.builder()
                .withMetadata( metadata )
                .build();

        // call listTransactions procedure that should list itself with the specified metadata
        StatementResult result = session.run( "CALL dbms.listTransactions()", config );
        Map<String,Object> receivedMetadata = result.single().get( "metaData" ).asMap();

        assertEquals( metadata, receivedMetadata );
    }

    @Test
    void shouldSetTransactionTimeout()
    {
        // create a dummy node
        session.run( "CREATE (:Node)" ).consume();

        try ( Session otherSession = session.driver().session() )
        {
            try ( Transaction otherTx = otherSession.beginTransaction() )
            {
                // lock dummy node but keep the transaction open
                otherTx.run( "MATCH (n:Node) SET n.prop = 1" ).consume();

                TransactionConfig config = TransactionConfig.builder()
                        .withTimeout( ofSeconds( 1 ) )
                        .build();

                // run a query in an auto-commit transaction with timeout and try to update the locked dummy node
                TransientException error = assertThrows( TransientException.class,
                        () -> session.run( "MATCH (n:Node) SET n.prop = 2", config ).consume() );

                assertThat( error.getMessage(), containsString( "terminated" ) );
            }
        }
    }
}
