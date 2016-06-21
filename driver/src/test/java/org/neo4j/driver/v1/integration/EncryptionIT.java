/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.driver.v1.integration;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.util.TestNeo4j;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.driver.internal.util.AddressUtil.isLocalhost;
import static org.neo4j.driver.v1.Config.EncryptionLevel.NONE;
import static org.neo4j.driver.v1.Config.EncryptionLevel.REQUIRED;
import static org.neo4j.driver.v1.Config.EncryptionLevel.REQUIRED_NON_LOCAL;

public class EncryptionIT
{
    @Rule
    public TestNeo4j neo4j = new TestNeo4j();

    @Test
    public void shouldOperateWithNoEncryption() throws Exception
    {
        // Given
        Driver driver = GraphDatabase.driver( neo4j.address(), Config.build().withEncryptionLevel( NONE ).toConfig() );

        // Then
        assertThat( driver.encrypted(), equalTo( false ) );

        // When
        Session session = driver.session();
        StatementResult result = session.run( "RETURN 1" );

        // Then
        Record record = result.next();
        int value = record.get( 0 ).asInt();
        assertThat( value, equalTo( 1 ) );

        // Finally
        session.close();
        driver.close();
    }

    @Test
    public void shouldOperateWithRequiredNonLocalEncryption() throws Exception
    {
        // Given
        Driver driver = GraphDatabase.driver( neo4j.address(), Config.build().withEncryptionLevel( REQUIRED_NON_LOCAL ).toConfig() );

        // Then
        assertThat( driver.encrypted(), equalTo( !isLocalhost( neo4j.host() ) ) );

        // When
        Session session = driver.session();
        StatementResult result = session.run( "RETURN 1" );

        // Then
        Record record = result.next();
        int value = record.get( 0 ).asInt();
        assertThat( value, equalTo( 1 ) );

        // Finally
        session.close();
        driver.close();
    }

    @Test
    public void shouldOperateWithRequiredEncryption() throws Exception
    {
        // Given
        Driver driver = GraphDatabase.driver( neo4j.address(), Config.build().withEncryptionLevel( REQUIRED ).toConfig() );

        // Then
        assertThat( driver.encrypted(), equalTo( true ) );

        // When
        Session session = driver.session();
        StatementResult result = session.run( "RETURN 1" );

        // Then
        Record record = result.next();
        int value = record.get( 0 ).asInt();
        assertThat( value, equalTo( 1 ) );

        // Finally
        session.close();
        driver.close();
    }

}
