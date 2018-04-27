/*
 * Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.util.Matchers.arithmeticError;

public class BlockingFailingQueryInTx<C extends AbstractContext> extends AbstractBlockingQuery<C>
{
    public BlockingFailingQueryInTx( Driver driver )
    {
        super( driver, false );
    }

    @Override
    public void execute( C context )
    {
        try ( Session session = newSession( AccessMode.READ, context ) )
        {
            try ( Transaction tx = beginTransaction( session, context ) )
            {
                StatementResult result = tx.run( "UNWIND [10, 5, 0] AS x RETURN 10 / x" );

                try
                {
                    result.consume();
                    fail( "Exception expected" );
                }
                catch ( Exception e )
                {
                    assertThat( e, is( arithmeticError() ) );
                }
            }
        }
    }
}
