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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.driver.internal.util.Iterables.single;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;

public class BlockingReadQueryInTx<C extends AbstractContext> extends AbstractBlockingQuery<C> {
    public BlockingReadQueryInTx(Driver driver, boolean useBookmark) {
        super(driver, useBookmark);
    }

    @Override
    public void execute(C context) {
        try (var session = newSession(AccessMode.READ, context);
                var tx = beginTransaction(session, context)) {
            var result = tx.run("MATCH (n) RETURN n LIMIT 1");
            var records = result.list();
            if (!records.isEmpty()) {
                var record = single(records);
                var node = record.get(0).asNode();
                assertNotNull(node);
            }

            result.consume();
            context.readCompleted();
            tx.commit();
        }
    }
}
