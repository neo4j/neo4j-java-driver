/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import static org.neo4j.driver.Values.parameters;

import java.time.Duration;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.reactivestreams.ReactiveResult;
import org.neo4j.driver.reactivestreams.ReactiveSession;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import reactor.core.publisher.Mono;

public class RxResultRecordPublisherVerificationIT extends PublisherVerification<Record> {
    private final Neo4jManager NEO4J = new Neo4jManager();
    private static final long MAX_NUMBER_OF_RECORDS = 30000;

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final Duration TIMEOUT_FOR_NO_SIGNALS = Duration.ofSeconds(1);
    private static final Duration PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS = Duration.ofSeconds(1);

    private static final String QUERY = "UNWIND RANGE(1, $numberOfRecords) AS n RETURN 'String Number' + n";

    private Driver driver;

    public RxResultRecordPublisherVerificationIT() {
        super(
                new TestEnvironment(TIMEOUT.toMillis(), TIMEOUT_FOR_NO_SIGNALS.toMillis()),
                PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS.toMillis());
    }

    @BeforeClass
    public void beforeClass() {
        NEO4J.skipIfDockerTestsSkipped();
        NEO4J.start();
        driver = NEO4J.getDriver();
    }

    @AfterClass
    public void afterClass() {
        NEO4J.stop();
    }

    @Override
    public long maxElementsFromPublisher() {
        return MAX_NUMBER_OF_RECORDS;
    }

    @Override
    public Publisher<Record> createPublisher(long elements) {
        var session = driver.session(ReactiveSession.class);
        var result = Mono.fromDirect(session.run(QUERY, parameters("numberOfRecords", elements)));
        return result.flatMapMany(ReactiveResult::records);
    }

    @Override
    public Publisher<Record> createFailedPublisher() {
        var session = driver.session(ReactiveSession.class);
        var result = Mono.fromDirect(session.run("INVALID"));
        return result.flatMapMany(ReactiveResult::records);
    }
}
