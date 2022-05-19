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

import java.time.Duration;
import org.neo4j.driver.Driver;
import org.neo4j.driver.reactive.ReactiveResult;
import org.neo4j.driver.reactive.ReactiveSession;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import reactor.core.publisher.Mono;

@Testcontainers(disabledWithoutDocker = true)
public class ReactiveResultPublisherVerificationIT extends PublisherVerification<ReactiveResult> {
    private final Neo4jManager NEO4J = new Neo4jManager();
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final Duration TIMEOUT_FOR_NO_SIGNALS = Duration.ofSeconds(1);
    private static final Duration PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS = Duration.ofSeconds(1);

    private Driver driver;

    public ReactiveResultPublisherVerificationIT() {
        super(
                new TestEnvironment(TIMEOUT.toMillis(), TIMEOUT_FOR_NO_SIGNALS.toMillis()),
                PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS.toMillis());
    }

    @BeforeClass
    public void beforeClass() {
        NEO4J.skipIfDockerUnavailable();
        NEO4J.start();
        driver = NEO4J.getDriver();
    }

    @AfterClass
    public void afterClass() {
        NEO4J.stop();
    }

    @Override
    public long maxElementsFromPublisher() {
        return 1;
    }

    @Override
    public Publisher<ReactiveResult> createPublisher(long elements) {
        ReactiveSession session = driver.reactiveSession();
        return Mono.fromDirect(session.run("RETURN 1"));
    }

    @Override
    public Publisher<ReactiveResult> createFailedPublisher() {
        ReactiveSession session = driver.reactiveSession();
        // Please note that this publisher fails on run stage.
        return Mono.fromDirect(session.run("RETURN 5/0"));
    }
}
