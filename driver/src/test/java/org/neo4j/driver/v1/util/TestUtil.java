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
package org.neo4j.driver.v1.util;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;

public final class TestUtil
{
    private TestUtil()
    {
    }

    public static long countNodes( Driver driver, String bookmark )
    {
        try ( Session session = driver.session( bookmark ) )
        {
            return session.readTransaction( new TransactionWork<Long>()
            {
                @Override
                public Long execute( Transaction tx )
                {
                    return tx.run( "MATCH (n) RETURN count(n)" ).single().get( 0 ).asLong();
                }
            } );
        }
    }

    public static String cleanDb( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            cleanDb( session );
            return session.lastBookmark();
        }
    }

    private static void cleanDb( Session session )
    {
        int nodesDeleted;
        do
        {
            nodesDeleted = deleteBatchOfNodes( session );
        }
        while ( nodesDeleted > 0 );
    }

    private static int deleteBatchOfNodes( Session session )
    {
        return session.writeTransaction( new TransactionWork<Integer>()
        {
            @Override
            public Integer execute( Transaction tx )
            {
                StatementResult result = tx.run( "MATCH (n) WITH n LIMIT 10000 DETACH DELETE n RETURN count(n)" );
                return result.single().get( 0 ).asInt();
            }
        } );
    }
}
