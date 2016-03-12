/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.v1.types;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.v1.Value;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;
import static org.neo4j.driver.v1.Values.value;

public class TypeSystemTest
{
    private final InternalNode node = new InternalNode( 42L );
    private final InternalRelationship relationship = new InternalRelationship( 42L, 42L, 43L, "T" );

    private Value integerValue = value( 13 );
    private Value floatValue = value( 13.1 );
    private Value stringValue = value( "Lalala " );
    private Value nodeValue = value( node );
    private Value relationshipValue = value( relationship );
    private Value mapValue = value( Collections.singletonMap( "type", "r" ) );
    private Value pathValue = value( new InternalPath( Arrays.<Entity>asList( node, relationship, node ) ) );
    private Value booleanValue = value( true );
    private Value listValue = value( Arrays.asList( 1, 2, 3 ) );
    private Value nullValue = value( (Object) null );

    private InternalTypeSystem typeSystem = TYPE_SYSTEM;

    TypeVerifier newTypeVerifierFor( Type type )
    {
        HashSet<Value> allValues = new HashSet<>();
        allValues.add( integerValue );
        allValues.add( stringValue );
        allValues.add( floatValue );
        allValues.add( nodeValue );
        allValues.add( relationshipValue );
        allValues.add( mapValue );
        allValues.add( pathValue );
        allValues.add( booleanValue );
        allValues.add( nullValue );
        allValues.add( listValue );
        return new TypeVerifier( type, allValues );
    }

    @Test
    public void shouldNameTypeCorrectly()
    {
        assertThat( TYPE_SYSTEM.ANY().name(), is( "ANY" ) );
        assertThat( TYPE_SYSTEM.BOOLEAN().name(), is( "BOOLEAN" ) );
        assertThat( TYPE_SYSTEM.STRING().name(), is( "STRING" ) );
        assertThat( TYPE_SYSTEM.NUMBER().name(), is( "NUMBER" ) );
        assertThat( TYPE_SYSTEM.INTEGER().name(), is( "INTEGER" ) );
        assertThat( TYPE_SYSTEM.FLOAT().name(), is( "FLOAT" ) );
        assertThat( TYPE_SYSTEM.LIST().name(), is( "LIST OF ANY?" ) );
        assertThat( TYPE_SYSTEM.MAP().name(), is( "MAP" ) );
        assertThat( TYPE_SYSTEM.NODE().name(), is( "NODE" ) );
        assertThat( TYPE_SYSTEM.RELATIONSHIP().name(), is( "RELATIONSHIP" ) );
        assertThat( TYPE_SYSTEM.PATH().name(), is( "PATH" ) );
        assertThat( TYPE_SYSTEM.NULL().name(), is( "NULL" ) );
    }

    @Test
    public void shouldInferAnyTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( TYPE_SYSTEM.ANY() ) )
        {
            verifier.assertIncludes( booleanValue );
            verifier.assertIncludes( stringValue );
            verifier.assertIncludes( integerValue );
            verifier.assertIncludes( floatValue );
            verifier.assertIncludes( listValue );
            verifier.assertIncludes( mapValue );
            verifier.assertIncludes( nodeValue );
            verifier.assertIncludes( relationshipValue );
            verifier.assertIncludes( pathValue );
        }
    }

    @Test
    public void shouldInferNumberTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( TYPE_SYSTEM.NUMBER() ) )
        {
            verifier.assertIncludes( integerValue );
            verifier.assertIncludes( floatValue );
        }
    }

    @Test
    public void shouldInferNodesTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( TYPE_SYSTEM.NODE() ) )
        {
            verifier.assertIncludes( nodeValue );
        }
    }

    @Test
    public void shouldInferRelTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( TYPE_SYSTEM.RELATIONSHIP() ) )
        {
            verifier.assertIncludes( relationshipValue );
        }
    }

    @Test
    public void shouldInferStringTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( TYPE_SYSTEM.STRING() ) )
        {
            verifier.assertIncludes( stringValue );
        }
    }

    @Test
    public void shouldInferMapTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( TYPE_SYSTEM.MAP() ) )
        {
            verifier.assertIncludes( nodeValue );
            verifier.assertIncludes( relationshipValue );
            verifier.assertIncludes( mapValue );
        }
    }

    @Test
    public void shouldInferPathTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( TYPE_SYSTEM.PATH() ) )
        {
            verifier.assertIncludes( pathValue );
        }
    }

    @Test
    public void shouldInferNullCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( TYPE_SYSTEM.NULL() ) )
        {
            verifier.assertIncludes( nullValue );
        }
    }

    @Test
    public void shouldInferBooleanTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( TYPE_SYSTEM.BOOLEAN() ) )
        {
            verifier.assertIncludes( booleanValue );
        }
    }

    @Test
    public void shouldIntegerTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( TYPE_SYSTEM.INTEGER() ) )
        {
            verifier.assertIncludes( integerValue );
        }
    }

    @Test
    public void shouldInferFloatTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( TYPE_SYSTEM.FLOAT() ) )
        {
            verifier.assertIncludes( floatValue );
        }
    }

    @Test
    public void shouldInferListTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( typeSystem.LIST() ) )
        {
            verifier.assertIncludes( listValue );
        }
    }

    @Test
    public void shouldDetermineTypeCorrectly()
    {
        assertThat( integerValue, hasType( TYPE_SYSTEM.INTEGER() ) );
        assertThat( floatValue, hasType( TYPE_SYSTEM.FLOAT() ) );
        assertThat( stringValue, hasType( TYPE_SYSTEM.STRING() ) );
        assertThat( booleanValue, hasType( TYPE_SYSTEM.BOOLEAN() ) );
        assertThat( listValue, hasType( TYPE_SYSTEM.LIST() ) );
        assertThat( mapValue, hasType( TYPE_SYSTEM.MAP() ) );
        assertThat( nodeValue, hasType( TYPE_SYSTEM.NODE() ) );
        assertThat( relationshipValue, hasType( TYPE_SYSTEM.RELATIONSHIP() ) );
        assertThat( pathValue, hasType( TYPE_SYSTEM.PATH() ) );
        assertThat( nullValue, hasType( TYPE_SYSTEM.NULL() ) );
    }

    private class TypeVerifier implements AutoCloseable
    {
        private final Type type;
        private final Set<Value> values;

        TypeVerifier( Type type, Set<Value> values )
        {
            this.type = type;
            this.values = values;
        }

        public void assertIncludes( Value value )
        {
            assertThat( value, hasType( type ) );
            values.remove( value );
        }

        @Override
        public void close()
        {
            for ( Value value : values )
            {
                assertThat( value, not( hasType( type )) );
            }
        }
    }

    private Matcher<? super Value> hasType( final Type type )
    {
        return new BaseMatcher<Value>()
        {
            @Override
            public boolean matches( Object o )
            {
                return (o instanceof Value || o == null) && type.isTypeOf( (Value) o );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( type.name() );
            }
        };
    }

}
