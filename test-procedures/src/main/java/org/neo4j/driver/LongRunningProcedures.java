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
package org.neo4j.driver;

import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class LongRunningProcedures {
    @Context
    public Log log;

    @Context
    public Transaction tx;

    public LongRunningProcedures() {}

    @Procedure("test.driver.longRunningStatement")
    public void longRunningStatement(@Name("seconds") long seconds) {
        final long start = System.currentTimeMillis();

        while (System.currentTimeMillis() <= start + seconds * 1000L) {
            long count = 0;
            try {
                Thread.sleep(100L);
                count = nodeCount(); // Fails if transaction is terminated
            } catch (InterruptedException e) {
                this.log.error(e.getMessage() + " (last node count " + count + ")", e);
            }
        }
    }

    @Procedure("test.driver.longStreamingResult")
    public Stream<Output> longStreamingResult(@Name("seconds") long seconds) {
        return LongStream.range(0L, seconds * 100L)
                .map((x) -> {
                    if (x == 0L) {
                        return x;
                    } else {
                        try {
                            Thread.sleep(10L);
                        } catch (InterruptedException var4) {
                            this.log.error(var4.getMessage(), var4);
                        }

                        nodeCount(); // Fails if transaction is terminated
                        return x;
                    }
                })
                .mapToObj(l -> new Output(l));
    }

    private long nodeCount() {
        return tx.getAllNodes().stream().count();
    }

    public static class Output {
        public final Long out;

        public Output(long value) {
            this.out = value;
        }
    }
}
