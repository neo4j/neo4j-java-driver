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
package org.neo4j.driver.internal;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemeTest
{

    @ParameterizedTest
    @ValueSource( strings = {"neo4j", "neo4j+s", "neo4j+ssc", "bolt", "bolt+s", "bolt+ssc"} )
    void shouldAcceptValidSchemes( String input )
    {
        Scheme.validateScheme( input );
    }

    @ParameterizedTest
    @ValueSource( strings = {"bob", "grey", "", " ", "blah"} )
    void shouldRejectInvalidSchemes( String input )
    {
        IllegalArgumentException ex =
                assertThrows( IllegalArgumentException.class, () -> Scheme.validateScheme( input ) );
        assertTrue( ex.getMessage().contains( "Invalid address format " + input ) );
    }

    @ParameterizedTest
    @NullSource
    void shouldRejectNullScheme( String input )
    {
        IllegalArgumentException ex =
                assertThrows( IllegalArgumentException.class, () -> Scheme.validateScheme( input ) );
        assertTrue( ex.getMessage().contains( "Scheme must not be null" ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"neo4j+s", "bolt+s"} )
    void shouldAcceptValidHighTrustSchemes( String scheme )
    {
        assertTrue( Scheme.isHighTrustScheme( scheme ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"neo4j", "neo4j+ssc", "bolt", "bolt+ssc", "blah"} )
    void shouldRejectInvalidHighTrustSchemes( String scheme )
    {
        assertFalse( Scheme.isHighTrustScheme( scheme ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"neo4j+ssc", "bolt+ssc"} )
    void shouldAcceptValidLowTrustSchemes( String scheme )
    {
        assertTrue( Scheme.isLowTrustScheme( scheme ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"neo4j", "neo4j+s", "bolt", "bolt+s", "blah"} )
    void shouldRejectInvalidLowTrustSchemes( String scheme )
    {
        assertFalse( Scheme.isLowTrustScheme( scheme ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"neo4j+s", "neo4j+ssc", "bolt+s", "bolt+ssc"} )
    void shouldAcceptValidSecuritySchemes( String scheme )
    {
        assertTrue( Scheme.isSecurityScheme( scheme ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"neo4j", "bolt", "blah"} )
    void shouldRejectInvalidSecuritySchemes( String scheme )
    {
        assertFalse( Scheme.isSecurityScheme( scheme ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"neo4j", "neo4j+s", "neo4j+ssc"} )
    void shouldAcceptValidRoutingSchemes( String scheme )
    {
        assertTrue( Scheme.isRoutingScheme( scheme ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"bolt", "bolt+s", "bolt+ssc", "blah"} )
    void shouldRejectInvalidRoutingSchemes( String scheme )
    {
        assertFalse( Scheme.isRoutingScheme( scheme ) );
    }
}