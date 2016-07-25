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
package org.neo4j.driver.v1.integration;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.util.TestNeo4j;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
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
        Driver driver = GraphDatabase.driver( neo4j.uri(), Config.build().withEncryptionLevel( NONE ).toConfig() );

        // Then
        assertThat( driver.isEncrypted(), equalTo( false ) );

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
        Driver driver = GraphDatabase.driver( neo4j.uri(), Config.build().withEncryptionLevel( REQUIRED_NON_LOCAL ).toConfig() );

        // Then
        assertThat( driver.isEncrypted(), equalTo( !neo4j.address().isLocal() ) );

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
        Driver driver = GraphDatabase.driver( neo4j.uri(), Config.build().withEncryptionLevel( REQUIRED ).toConfig() );

        // Then
        assertThat( driver.isEncrypted(), equalTo( true ) );

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
