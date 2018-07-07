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
package org.neo4j.driver.v1.stress;

import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BlockingWriteQueryUsingReadSessionInTx<C extends AbstractContext> extends AbstractBlockingQuery<C>
{
    public BlockingWriteQueryUsingReadSessionInTx( Driver driver, boolean useBookmark )
    {
        super( driver, useBookmark );
    }

    @Override
    public void execute( C context )
    {
        AtomicReference<StatementResult> resultRef = new AtomicReference<>();
        assertThrows( ClientException.class, () ->
        {
            try ( Session session = newSession( AccessMode.READ, context );
                  Transaction tx = beginTransaction( session, context ) )
            {
                StatementResult result = tx.run( "CREATE ()" );
                resultRef.set( result );
                tx.success();
            }
        } );
        assertNotNull( resultRef.get() );
        assertEquals( 0, resultRef.get().summary().counters().nodesCreated() );
    }
}
