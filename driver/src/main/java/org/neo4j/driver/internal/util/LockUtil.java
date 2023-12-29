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
package org.neo4j.driver.internal.util;

import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

public class LockUtil {
    public static void executeWithLock(Lock lock, Runnable runnable) {
        lock(lock);
        try {
            runnable.run();
        } finally {
            unlock(lock);
        }
    }

    public static <T> T executeWithLock(Lock lock, Supplier<T> supplier) {
        lock(lock);
        try {
            return supplier.get();
        } finally {
            unlock(lock);
        }
    }

    /**
     * Invokes {@link Lock#lock()} on the supplied {@link Lock}.
     * <p>
     * This method is marked as allowed in the {@link org.neo4j.driver.internal.blockhound.Neo4jDriverBlockHoundIntegration}.
     * @param lock the lock
     * @since 5.11
     */
    private static void lock(Lock lock) {
        lock.lock();
    }

    /**
     * Invokes {@link Lock#unlock()} on the supplied {@link Lock}.
     * <p>
     * This method is marked as allowed in the {@link org.neo4j.driver.internal.blockhound.Neo4jDriverBlockHoundIntegration}.
     * @param lock the lock
     * @since 5.11
     */
    private static void unlock(Lock lock) {
        lock.unlock();
    }
}
