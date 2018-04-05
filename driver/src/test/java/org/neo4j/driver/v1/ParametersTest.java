/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.driver.v1;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.NetworkSession;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.Collections.singletonList;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.ValueFactory.emptyNodeValue;
import static org.neo4j.driver.internal.util.ValueFactory.emptyRelationshipValue;
import static org.neo4j.driver.internal.util.ValueFactory.filledPathValue;
import static org.neo4j.driver.v1.Values.parameters;

@RunWith(Parameterized.class)
public class ParametersTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Parameterized.Parameter
    public Object obj;
    @Parameterized.Parameter( 1 )
    public String expectedMsg;

    @Parameterized.Parameters( name = "{0}" )
    public static Object[][] addressesToParse()
    {
        return new Object[][]{
                // Node
                {emptyNodeValue(), "Nodes can't be used as parameters."},
                {emptyNodeValue().asNode(), "Nodes can't be used as parameters."},

                // Rel
                {emptyRelationshipValue(), "Relationships can't be used as parameters."},
                {emptyRelationshipValue().asRelationship(), "Relationships can't be used as parameters."},

                // Path
                {filledPathValue(), "Paths can't be used as parameters."},
                {filledPathValue().asPath(), "Paths can't be used as parameters."},
        };
    }

    @Test
    public void shouldGiveHelpfulMessageOnMisalignedInput() throws Throwable
    {
        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Parameters function requires an even number of arguments, " +
                                 "alternating key and value." );

        // When
        Values.parameters( "1", obj, "2" );
    }

    @Test
    public void shouldNotBePossibleToUseInvalidParameterTypesViaParameters()
    {
        //Expect
        exception.expect( ClientException.class );
        exception.expectMessage( expectedMsg );

        // WHEN
        Session session = mockedSession();
        session.run( "RETURN {a}", parameters( "a", obj ) );
    }

    @Test
    public void shouldNotBePossibleToUseInvalidParametersViaMap()
    {
        // GIVEN
        Map<String,Object> params = new HashMap<>();
        params.put( "a", obj );

        //Expect
        exception.expect( ClientException.class );
        exception.expectMessage( expectedMsg );

        // WHEN
        Session session = mockedSession();
        session.run( "RETURN {a}", params );
    }

    @Test
    public void shouldNotBePossibleToUseInvalidParametersViaRecord()
    {
        // GIVEN
        assumeTrue(obj instanceof Value);
        Record record = new InternalRecord( singletonList( "a" ), new Value[]{(Value) obj} );

        //Expect
        exception.expect( ClientException.class );
        exception.expectMessage( expectedMsg );

        // WHEN
        Session session = mockedSession();
        session.run( "RETURN {a}", record );
    }

    private Session mockedSession()
    {
        ConnectionProvider provider = mock( ConnectionProvider.class );
        RetryLogic retryLogic = mock( RetryLogic.class );
        return new NetworkSession( provider, AccessMode.WRITE, retryLogic, DEV_NULL_LOGGING );
    }
}
