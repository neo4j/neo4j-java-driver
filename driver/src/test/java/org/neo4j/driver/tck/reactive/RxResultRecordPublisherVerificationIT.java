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
package org.neo4j.driver.tck.reactive;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;

import java.time.Duration;

import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;

import static org.neo4j.driver.Values.parameters;

@Testcontainers( disabledWithoutDocker = true )
public class RxResultRecordPublisherVerificationIT extends PublisherVerification<Record>
{
    private static final Neo4jContainer<?> NEO4J_CONTAINER = new Neo4jContainer<>( "neo4j:4.4" )
            .withAdminPassword( null );

    private final static long MAX_NUMBER_OF_RECORDS = 30000;

    private static final Duration TIMEOUT = Duration.ofSeconds( 10 );
    private static final Duration TIMEOUT_FOR_NO_SIGNALS = Duration.ofSeconds( 1 );
    private static final Duration PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS = Duration.ofSeconds( 1 );

    private final static String QUERY = "UNWIND RANGE(1, $numberOfRecords) AS n RETURN 'String Number' + n";

    private Driver driver;

    public RxResultRecordPublisherVerificationIT()
    {
        super( new TestEnvironment( TIMEOUT.toMillis(), TIMEOUT_FOR_NO_SIGNALS.toMillis() ),
               PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS.toMillis() );
    }

    @BeforeClass
    public void beforeClass()
    {
        if ( !isDockerAvailable() )
        {
            throw new SkipException( "Docker is unavailable" );
        }
        NEO4J_CONTAINER.start();
        driver = GraphDatabase.driver( NEO4J_CONTAINER.getBoltUrl() );
    }

    public void afterClass()
    {
        NEO4J_CONTAINER.stop();
    }

    @Override
    public long maxElementsFromPublisher()
    {
        return MAX_NUMBER_OF_RECORDS;
    }

    @Override
    public Publisher<Record> createPublisher( long elements )
    {
        RxSession session = driver.rxSession();
        RxResult result = session.run( QUERY, parameters( "numberOfRecords", elements ) );
        return result.records();
    }

    @Override
    public Publisher<Record> createFailedPublisher()
    {
        RxSession session = driver.rxSession();
        RxResult result = session.run( "INVALID" );
        return result.records();
    }

    boolean isDockerAvailable()
    {
        try
        {
            DockerClientFactory.instance().client();
            return true;
        }
        catch ( Throwable ex )
        {
            return false;
        }
    }
}
