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
package org.neo4j.driver.stress;

import java.util.List;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.types.Node;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.driver.internal.util.Iterables.single;

public class BlockingReadQueryInTx<C extends AbstractContext> extends AbstractBlockingQuery<C>
{
    public BlockingReadQueryInTx( Driver driver, boolean useBookmark )
    {
        super( driver, useBookmark );
    }

    @Override
    public void execute( C context )
    {
        try ( Session session = newSession( AccessMode.READ, context );
              Transaction tx = beginTransaction( session, context ) )
        {
            Result result = tx.run( "MATCH (n) RETURN n LIMIT 1" );
            List<Record> records = result.list();
            if ( !records.isEmpty() )
            {
                Record record = single( records );
                Node node = record.get( 0 ).asNode();
                assertNotNull( node );
            }

            context.readCompleted( result.consume() );
            tx.commit();
        }
    }
}
