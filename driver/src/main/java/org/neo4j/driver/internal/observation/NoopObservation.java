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
package org.neo4j.driver.internal.observation;

public class NoopObservation implements Observation {
    private static final NoopObservation INSTANCE = new NoopObservation();

    public static NoopObservation getInstance() {
        return INSTANCE;
    }

    public NoopObservation() {}

    @Override
    public Observation start() {
        return this;
    }

    @Override
    public Observation error(Throwable error) {
        return this;
    }

    @Override
    public void stop() {}

    @Override
    public Scope openScope() {
        return NoopScope.INSTANCE;
    }

    public static final class NoopScope implements Scope {
        private static final NoopScope INSTANCE = new NoopScope();

        public static NoopScope getInstance() {
            return INSTANCE;
        }

        @Override
        public void close() {}
    }
}
