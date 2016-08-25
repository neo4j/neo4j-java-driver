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
package org.neo4j.docs.driver;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.summary.Notification;
import org.neo4j.driver.v1.summary.ResultSummary;

public class Examples
{

    public static Driver constructDriver() throws Exception
    {
        // tag::construct-driver[]
        Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic("neo4j", "neo4j") );
        // end::construct-driver[]

        return driver;
    }

    public static Driver configuration() throws Exception
    {
        // tag::configuration[]
        Driver driver = GraphDatabase.driver(
                "bolt://localhost",
                AuthTokens.basic("neo4j", "neo4j"),
                Config.build().withMaxSessions( 10 ).toConfig() );
        // end::configuration[]

        return driver;
    }

    public static void statement( Session session ) throws Exception
    {
        // tag::statement[]
        StatementResult result =
                session.run( "CREATE (person:Person {name: {name}})", Values.parameters( "name", "Arthur" ) );
        // end::statement[]
        int theOnesCreated = result.consume().counters().nodesCreated();
        System.out.println( "There were " + theOnesCreated + " the ones created." );
    }

    public static void statementWithoutParameters( Session session ) throws Exception
    {
        // tag::statement-without-parameters[]
        StatementResult result = session.run( "CREATE (p:Person { name: 'Arthur' })" );
        // end::statement-without-parameters[]
        int theOnesCreated = result.consume().counters().nodesCreated();
        System.out.println( "There were " + theOnesCreated + " the ones created." );
    }

    public static void resultTraversal( Session session ) throws Exception
    {
        // tag::result-traversal[]
        String searchTerm = "Sword";
        StatementResult result = session.run( "MATCH (weapon:Weapon) WHERE weapon.name CONTAINS {term} RETURN weapon.name",
                Values.parameters( "term", searchTerm ) );

        System.out.println("List of weapons called " + searchTerm + ":");
        while ( result.hasNext() )
        {
            Record record = result.next();
            System.out.println(record.get("weapon.name").asString());
        }
        // end::result-traversal[]
    }

    public static void accessRecord( Session session ) throws Exception
    {
        // tag::access-record[]
        String searchTerm = "Arthur";
        StatementResult result = session.run( "MATCH (weapon:Weapon) WHERE weapon.owner CONTAINS {term} RETURN weapon.name, weapon.material, weapon.size",
                Values.parameters( "term", searchTerm ) );

        System.out.println("List of weapons owned by " + searchTerm + ":");
        while ( result.hasNext() )
        {
            Record record = result.next();
            List<String> sword = new ArrayList<>();
            for ( String key : record.keys() )
            {
                sword.add( key + ": " + record.get(key) );
            }
            System.out.println(sword);
        }
        // end::access-record[]
    }

    public static void retainResultsForNestedQuerying( Session session ) throws Exception
    {
        // tag::nested-statements[]
        StatementResult result = session.run( "MATCH (knight:Person:Knight) WHERE knight.castle = {castle} RETURN id(knight) AS knight_id",
                Values.parameters( "castle", "Camelot" ) );

        for ( Record record : result.list() )
        {
            session.run( "MATCH (knight) WHERE id(knight) = {id} " +
                    "MATCH (king:Person) WHERE king.name = {king} " +
                    "CREATE (knight)-[:DEFENDS]->(king)",
                    Values.parameters( "id", record.get( "knight_id" ), "king", "Arthur" ) );
        }
        // end::nested-statements[]
    }

    public static void retainResultsForLaterProcessing( Driver driver ) throws Exception
    {
        // tag::retain-result[]
        List<Record> records;
        try ( Session session = driver.session() )
        {
            StatementResult result = session.run(
                    "MATCH (knight:Person:Knight) WHERE knight.castle = {castle} RETURN knight.name AS name",
                    Values.parameters( "castle", "Camelot" ) );

            records = result.list();
        }

        for ( Record record : records )
        {
            System.out.println( record.get( "name" ).asString() + " is a knight of Camelot" );
        }
        // end::retain-result[]
    }

    public static void handleCypherError( Session session ) throws Exception
    {
        // tag::handle-cypher-error[]
        try
        {
            session.run( "This will cause a syntax error" ).consume();
        }
        catch ( ClientException e )
        {
            throw new RuntimeException("Something really bad has happened!");
        }
        // end::handle-cypher-error[]
    }

    public static void transactionCommit( Session session ) throws Exception
    {
        // tag::transaction-commit[]
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE (:Person {name: 'Guinevere'})" );
            tx.success();
        }
        // end::transaction-commit[]
    }

    public static void transactionRollback( Session session ) throws Exception
    {
        // tag::transaction-rollback[]
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE (:Person {name: 'Merlin'})" );
            tx.failure();
        }
        // end::transaction-rollback[]
    }

    public static void resultSummary( Session session ) throws Exception
    {
        // tag::result-summary-query-profile[]
        StatementResult result = session.run( "PROFILE MATCH (p:Person { name: {name} }) RETURN id(p)",
                Values.parameters( "name", "Arthur" ) );

        ResultSummary summary = result.consume();

        System.out.println( summary.statementType() );
        System.out.println( summary.profile() );
        // end::result-summary-query-profile[]
    }

    public static void notifications( Session session ) throws Exception
    {
        // tag::result-summary-notifications[]
        ResultSummary summary = session.run( "EXPLAIN MATCH (king), (queen) RETURN king, queen" ).consume();

        for ( Notification notification : summary.notifications() )
        {
            System.out.println( notification );
        }
        // end::result-summary-notifications[]
    }

    public static Driver requireEncryption() throws Exception
    {
        // tag::tls-require-encryption[]
        Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic("neo4j", "neo4j"),
                Config.build().withEncryptionLevel( Config.EncryptionLevel.REQUIRED ).toConfig() );
        // end::tls-require-encryption[]

        return driver;
    }

    public static Driver trustOnFirstUse() throws Exception
    {
        // tag::tls-trust-on-first-use[]
        Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic("neo4j", "neo4j"), Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.REQUIRED )
                .withTrustStrategy( Config.TrustStrategy.trustOnFirstUse( new File( "/path/to/neo4j_known_hosts" ) ) )
                .toConfig() );
        // end::tls-trust-on-first-use[]

        return driver;
    }

    public static Driver trustSignedCertificates() throws Exception
    {
        // tag::tls-signed[]
        Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic("neo4j", "neo4j"), Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.REQUIRED )
                .withTrustStrategy( Config.TrustStrategy.trustCustomCertificateSignedBy( new File( "/path/to/ca-certificate.pem") ) )
                .toConfig() );
        // end::tls-signed[]

        return driver;
    }

    public static Driver connectWithAuthDisabled() throws Exception
    {
        // tag::connect-with-auth-disabled[]
        Driver driver = GraphDatabase.driver( "bolt://localhost",
                Config.build().withEncryptionLevel( Config.EncryptionLevel.REQUIRED ).toConfig() );
        // end::connect-with-auth-disabled[]

        return driver;
    }
}
