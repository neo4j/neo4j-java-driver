/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.driver.v1.exceptions.ClientException;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BlockingWriteQueryUsingReadSession<C extends AbstractContext> extends AbstractBlockingQuery<C>
{
    public BlockingWriteQueryUsingReadSession( Driver driver, boolean useBookmark )
    {
        super( driver, useBookmark );
    }

    @Override
    public void execute( C context )
    {
        StatementResult result = null;
        try
        {
            try ( Session session = newSession( AccessMode.READ, context ) )
            {
                result = session.run( "CREATE ()" );
            }
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
            assertNotNull( result );
            assertEquals( 0, result.summary().counters().nodesCreated() );
        }
    }
}
