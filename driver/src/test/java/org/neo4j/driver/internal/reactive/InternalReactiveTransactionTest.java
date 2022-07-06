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
package org.neo4j.driver.internal.reactive;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import reactor.test.StepVerifier;

public class InternalReactiveTransactionTest {
    private InternalReactiveTransaction tx;

    @Test
    void shouldDelegateInterrupt() {
        // Given
        UnmanagedTransaction utx = mock(UnmanagedTransaction.class);
        given(utx.interruptAsync()).willReturn(completedFuture(null));
        tx = new InternalReactiveTransaction(utx);

        // When
        StepVerifier.create(tx.interrupt()).expectComplete().verify();

        // Then
        then(utx).should().interruptAsync();
    }

    @Test
    void shouldDelegateInterruptAndReportError() {
        // Given
        UnmanagedTransaction utx = mock(UnmanagedTransaction.class);
        RuntimeException e = mock(RuntimeException.class);
        given(utx.interruptAsync()).willReturn(failedFuture(e));
        tx = new InternalReactiveTransaction(utx);

        // When
        StepVerifier.create(tx.interrupt()).expectErrorMatches(ar -> ar == e).verify();

        // Then
        then(utx).should().interruptAsync();
    }
}
