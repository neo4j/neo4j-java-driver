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
package org.neo4j.driver.internal.blockhound;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.util.LockUtil;
import org.neo4j.driver.testutil.BlockHoundTest;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.scheduler.Schedulers;

@BlockHoundTest
class LockUtilBlockHoundTest {
    @BeforeAll
    static void setUp() {
        BlockHound.install();
    }

    @Test
    @SuppressWarnings("StatementWithEmptyBody")
    void shouldAllowBlockingOnLock() {
        var lock = new ReentrantLock();
        var lockUnlockFuture = new CompletableFuture<Void>();
        CompletableFuture.runAsync(() -> {
            lock.lock();
            lockUnlockFuture.join();
            lock.unlock();
        });
        var testFuture = new CompletableFuture<Void>();
        Schedulers.parallel().schedule(() -> {
            try {
                LockUtil.executeWithLock(lock, () -> {});
            } catch (Throwable t) {
                testFuture.completeExceptionally(t);
            }
            testFuture.complete(null);
        });

        while (lock.getQueueLength() != 1) {}
        lockUnlockFuture.complete(null);
        testFuture.join();
    }

    @Test
    void shouldFailInternalBlockingCalls() {
        var lock = new ReentrantLock();
        var testFuture = new CompletableFuture<Void>();
        Schedulers.parallel()
                .schedule(() -> LockUtil.executeWithLock(lock, () -> {
                    try {
                        Thread.sleep(1);
                    } catch (Throwable e) {
                        testFuture.completeExceptionally(e);
                        return;
                    }
                    testFuture.complete(null);
                }));

        var exception =
                assertThrows(CompletionException.class, testFuture::join).getCause();
        assertEquals(BlockingOperationError.class, exception.getClass());
        assertTrue(exception.getMessage().contains("java.lang.Thread.sleep"));
    }
}
