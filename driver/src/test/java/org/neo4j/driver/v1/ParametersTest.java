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
package org.neo4j.driver.v1;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.NetworkSession;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.ValueFactory.emptyNodeValue;
import static org.neo4j.driver.internal.util.ValueFactory.emptyRelationshipValue;
import static org.neo4j.driver.internal.util.ValueFactory.filledPathValue;
import static org.neo4j.driver.v1.Values.parameters;

class ParametersTest
{
    static Stream<Arguments> addressesToParse()
    {
        return Stream.of(
                // Node
                Arguments.of( emptyNodeValue(), "Nodes can't be used as parameters." ),
                Arguments.of( emptyNodeValue().asNode(), "Nodes can't be used as parameters." ),

                // Relationship
                Arguments.of( emptyRelationshipValue(), "Relationships can't be used as parameters." ),
                Arguments.of( emptyRelationshipValue().asRelationship(), "Relationships can't be used as parameters." ),

                // Path
                Arguments.of( filledPathValue(), "Paths can't be used as parameters." ),
                Arguments.of( filledPathValue().asPath(), "Paths can't be used as parameters." )
        );
    }

    @ParameterizedTest
    @MethodSource( "addressesToParse" )
    void shouldGiveHelpfulMessageOnMisalignedInput( Object obj, String expectedMsg )
    {
        ClientException e = assertThrows( ClientException.class, () -> Values.parameters( "1", obj, "2" ) );
        assertThat( e.getMessage(), startsWith( "Parameters function requires an even number of arguments, alternating key and value." ) );
    }

    @ParameterizedTest
    @MethodSource( "addressesToParse" )
    void shouldNotBePossibleToUseInvalidParameterTypesViaParameters( Object obj, String expectedMsg )
    {
        Session session = mockedSession();
        ClientException e = assertThrows( ClientException.class, () -> session.run( "RETURN {a}", parameters( "a", obj ) ) );
        assertEquals( expectedMsg, e.getMessage() );
    }

    @ParameterizedTest
    @MethodSource( "addressesToParse" )
    void shouldNotBePossibleToUseInvalidParametersViaMap( Object obj, String expectedMsg )
    {
        Session session = mockedSession();
        ClientException e = assertThrows( ClientException.class, () -> session.run( "RETURN {a}", singletonMap( "a", obj ) ) );
        assertEquals( expectedMsg, e.getMessage() );
    }

    @ParameterizedTest
    @MethodSource( "addressesToParse" )
    void shouldNotBePossibleToUseInvalidParametersViaRecord( Object obj, String expectedMsg )
    {
        assumeTrue( obj instanceof Value );
        Record record = new InternalRecord( singletonList( "a" ), new Value[]{(Value) obj} );
        Session session = mockedSession();

        ClientException e = assertThrows( ClientException.class, () -> session.run( "RETURN {a}", record ) );
        assertEquals( expectedMsg, e.getMessage() );
    }

    private Session mockedSession()
    {
        ConnectionProvider provider = mock( ConnectionProvider.class );
        RetryLogic retryLogic = mock( RetryLogic.class );
        return new NetworkSession( provider, AccessMode.WRITE, retryLogic, DEV_NULL_LOGGING );
    }
}
