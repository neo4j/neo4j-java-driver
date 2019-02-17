/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.v1.integration;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.BeforeClass;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.react.RxResult;
import org.neo4j.driver.react.RxSession;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.DatabaseExtension;

import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V4;
import static org.neo4j.driver.v1.Values.parameters;

public class RxSessionRecordPublisherVerificationIT extends PublisherVerification<Record>
{
    private static final DatabaseExtension neo4j = new DatabaseExtension();

    private final static long MAX_NUMBER_OF_RECORDS = 30000;

    private static final Duration TIMEOUT = Duration.ofSeconds( 10 );
    private static final Duration TIMEOUT_FOR_NO_SIGNALS = Duration.ofSeconds( 1 );
    private static final Duration PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS = Duration.ofSeconds( 1 );

    private final static String QUERY = "UNWIND RANGE(1, $numberOfRecords) AS n RETURN 'String Number' + n";

    public RxSessionRecordPublisherVerificationIT()
    {
        super( new TestEnvironment( TIMEOUT.toMillis(), TIMEOUT_FOR_NO_SIGNALS.toMillis() ),
                PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS.toMillis() );
    }

    @Override
    public long maxElementsFromPublisher()
    {
        return MAX_NUMBER_OF_RECORDS;
    }

    @Override
    public Publisher<Record> createPublisher( long elements )
    {
        if( !isBoltV4Available() )
        {
            // we skip the tests by creating a faked publisher
            return fakeRecordPublisher( elements );
        }
        RxSession session = neo4j.driver().rxSession();
        RxResult result = session.run( QUERY, parameters( "numberOfRecords", elements ) );
        return result.records();
    }

    @Override
    public Publisher<Record> createFailedPublisher()
    {
        if( !isBoltV4Available() )
        {
            return null; // skip tests
        }
        RxSession session = neo4j.driver().rxSession();
        RxResult result = session.run( "INVALID" );
        return result.records();
    }

    @BeforeClass
    public static void before()
    {
        ensureDriver();
    }

    private static void ensureDriver()
    {
        try
        {
            neo4j.beforeEach( null );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    private static boolean isBoltV4Available()
    {
        ResultSummary summary = neo4j.driver().session().run( "RETURN 1" ).consume();
        ServerVersion serverVersion = ServerVersion.version( summary.server().version() );
        return BOLT_V4.availableIn( serverVersion );
    }

    private Publisher<Record> fakeRecordPublisher( long elements )
    {
        List<Record> source = new ArrayList<>();
        Value[] emptyArray = new Value[0];
        for ( int i =0; i< elements; i ++  )
        {
            source.add( new InternalRecord( Collections.emptyList(), emptyArray ) );
        }
        return Flux.fromIterable( source );
    }
}
