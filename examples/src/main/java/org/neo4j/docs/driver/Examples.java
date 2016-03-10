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
import java.util.List;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.summary.Notification;
import org.neo4j.driver.v1.util.Pair;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;

public class Examples
{

    public static Driver constructDriver() throws Exception
    {
        // tag::construct-driver[]
        Driver driver = GraphDatabase.driver( "bolt://localhost" );
        // end::construct-driver[]

        return driver;
    }

    public static Driver configuration() throws Exception
    {
        // tag::configuration[]
        Driver driver =
                GraphDatabase.driver( "bolt://localhost", Config.build().withMaxSessions( 10 ).toConfig() );
        // end::configuration[]

        return driver;
    }

    public static void statement( Session session ) throws Exception
    {
        // tag::statement[]
        StatementResult result =
                session.run( "CREATE (p:Person { name: {name} })", Values.parameters( "name", "The One" ) );

        int theOnesCreated = result.summarize().updateStatistics().nodesCreated();
        System.out.println( "There were " + theOnesCreated + " the ones created." );
        // end::statement[]
    }

    public static void statementWithoutParameters( Session session ) throws Exception
    {
        // tag::statement-without-parameters[]
        StatementResult result = session.run( "CREATE (p:Person { name: 'The One' })" );

        int theOnesCreated = result.summarize().updateStatistics().nodesCreated();
        System.out.println( "There were " + theOnesCreated + " the ones created." );
        // end::statement-without-parameters[]
    }

    public static void resultCursor( Session session ) throws Exception
    {
        // tag::result-cursor[]
        StatementResult result = session.run( "MATCH (p:Person { name: {name} }) RETURN p.age",
                Values.parameters( "name", "The One" ) );

        while ( result.hasNext() )
        {
            Record record = result.next();
            for ( Pair<String,Value> fieldInRecord : record.fields() )
            {
                System.out.println( fieldInRecord.key() + " = " + fieldInRecord.value() );
            }
        }
        // end::result-cursor[]
    }

    public static void retainResultsForNestedQuerying( Session session ) throws Exception
    {
        // tag::retain-result-query[]
        StatementResult result = session.run( "MATCH (p:Person { name: {name} }) RETURN id(p)",
                Values.parameters( "name", "The One" ) );

        for ( Record record : result.list() )
        {
            session.run( "MATCH (p) WHERE id(p) = {id} " + "CREATE (p)-[:HAS_TRAIT]->(:Trait {type:'Immortal'})",
                    Values.parameters( "id", record.get( "id(p)" ) ) );
        }
        // end::retain-result-query[]
    }

    public static void retainResultsForLaterProcessing( Driver driver ) throws Exception
    {
        // tag::retain-result-process[]
        Session session = driver.session();

        StatementResult result = session.run( "MATCH (p:Person { name: {name} }) RETURN p.age",
                Values.parameters( "name", "The One" ) );

        List<Record> records = result.list();

        session.close();

        for ( Record record : records )
        {
            for ( Pair<String,Value> fieldInRecord : record.fields() )
            {
                System.out.println( fieldInRecord.key() + " = " + fieldInRecord.value() );
            }
        }
        // end::retain-result-process[]
    }

    public static void transactionCommit( Session session ) throws Exception
    {
        // tag::transaction-commit[]
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE (p:Person { name: 'The One' })" );
            tx.success();
        }
        // end::transaction-commit[]
    }

    public static void transactionRollback( Session session ) throws Exception
    {
        // tag::transaction-rollback[]
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE (p:Person { name: 'The One' })" );
            tx.failure();
        }
        // end::transaction-rollback[]
    }

    public static void resultSummary( Session session ) throws Exception
    {
        // tag::result-summary-query-profile[]
        StatementResult result = session.run( "PROFILE MATCH (p:Person { name: {name} }) RETURN id(p)",
                Values.parameters( "name", "The One" ) );

        ResultSummary summary = result.summarize();

        System.out.println( summary.statementType() );
        System.out.println( summary.profile() );
        // end::result-summary-query-profile[]
    }

    public static void notifications( Session session ) throws Exception
    {
        // tag::result-summary-notifications[]
        ResultSummary summary = session.run( "EXPLAIN MATCH (a), (b) RETURN a,b" ).summarize();

        for ( Notification notification : summary.notifications() )
        {
            System.out.println( notification );
        }
        // end::result-summary-notifications[]
    }

    public static Driver requireEncryption() throws Exception
    {
        // tag::tls-require-encryption[]
        Driver driver = GraphDatabase.driver( "bolt://localhost",
                Config.build().withEncryptionLevel( Config.EncryptionLevel.REQUIRED ).toConfig() );
        // end::tls-require-encryption[]

        return driver;
    }

    public static Driver trustOnFirstUse() throws Exception
    {
        // tag::tls-trust-on-first-use[]
        Driver driver = GraphDatabase.driver( "bolt://localhost", Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE )
                .withTrustStrategy( Config.TrustStrategy.trustOnFirstUse( new File( "/path/to/neo4j_known_hosts" ) ) )
                .toConfig() );
        // end::tls-trust-on-first-use[]

        return driver;
    }

    public static Driver trustSignedCertificates() throws Exception
    {
        // tag::tls-signed[]
        Driver driver = GraphDatabase.driver( "bolt://localhost", Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE )
                .withTrustStrategy( Config.TrustStrategy.trustSignedBy( new File( "/path/to/ca-certificate.pem") ) )
                .toConfig() );
        // end::tls-signed[]

        return driver;
    }
}
