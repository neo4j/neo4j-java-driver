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
package org.neo4j.driver.stress;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;

public class BlockingWriteQueryInTx<C extends AbstractContext> extends AbstractBlockingQuery<C> {
    private final AbstractStressTestBase<C> stressTest;

    BlockingWriteQueryInTx(AbstractStressTestBase<C> stressTest, Driver driver, boolean useBookmark) {
        super(driver, useBookmark);
        this.stressTest = stressTest;
    }

    @Override
    public void execute(C context) {
        Result result = null;
        Throwable txError = null;

        try (var session = newSession(AccessMode.WRITE, context)) {
            try (var tx = beginTransaction(session, context)) {
                result = tx.run("CREATE ()");
                tx.commit();
            }

            context.setBookmark(session.lastBookmarks());
        } catch (Throwable error) {
            txError = error;
            if (!stressTest.handleWriteFailure(error, context)) {
                throw error;
            }
        }

        if (txError == null && result != null) {
            assertEquals(1, result.consume().counters().nodesCreated());
            context.nodeCreated();
        }
    }
}
