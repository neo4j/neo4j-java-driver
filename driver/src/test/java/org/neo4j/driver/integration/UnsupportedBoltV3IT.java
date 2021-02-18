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
package org.neo4j.driver.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.function.Executable;

import java.util.concurrent.CompletionStage;

import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.util.DisabledOnNeo4jWith;
import org.neo4j.driver.util.DriverExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V3;
import static org.neo4j.driver.util.TestUtil.await;

@DisabledOnNeo4jWith( BOLT_V3 )
@ParallelizableIT
class UnsupportedBoltV3IT
{
    @RegisterExtension
    static final DriverExtension driver = new DriverExtension();

    private final TransactionConfig txConfig = TransactionConfig.builder()
            .withTimeout( ofSeconds( 4 ) )
            .withMetadata( singletonMap( "key", "value" ) )
            .build();

    @Test
    void shouldNotSupportAutoCommitQueriesWithTransactionConfig()
    {
        assertTxConfigNotSupported( () -> driver.session().run( "RETURN 42", txConfig ) );
    }

    @Test
    void shouldNotSupportAsyncAutoCommitQueriesWithTransactionConfig()
    {
        assertTxConfigNotSupported( driver.asyncSession().runAsync( "RETURN 42", txConfig ) );
    }

    @Test
    void shouldNotSupportTransactionFunctionsWithTransactionConfig()
    {
        assertTxConfigNotSupported( () -> driver.session().readTransaction( tx -> tx.run( "RETURN 42" ), txConfig ) );
    }

    @Test
    void shouldNotSupportAsyncTransactionFunctionsWithTransactionConfig()
    {
        assertTxConfigNotSupported( driver.asyncSession().readTransactionAsync( tx -> tx.runAsync( "RETURN 42" ), txConfig ) );
    }

    @Test
    void shouldNotSupportUnmanagedTransactionsWithTransactionConfig()
    {
        assertTxConfigNotSupported( () -> driver.session().beginTransaction( txConfig ) );
    }

    @Test
    void shouldNotSupportAsyncUnmanagedTransactionsWithTransactionConfig()
    {
        assertTxConfigNotSupported( driver.asyncSession().beginTransactionAsync( txConfig ) );
    }

    /**
     * Separate method to verify async APIs. They should return {@link CompletionStage}s completed exceptionally and not throw exceptions directly.
     *
     * @param stage the stage to verify.
     */
    private static void assertTxConfigNotSupported( CompletionStage<?> stage )
    {
        assertTxConfigNotSupported( () -> await( stage ) );
    }

    private static void assertTxConfigNotSupported( Executable executable )
    {
        ClientException error = assertThrows( ClientException.class, executable );
        assertThat( error.getMessage(), startsWith( "Driver is connected to the database that does not support transaction configuration" ) );
    }
}
