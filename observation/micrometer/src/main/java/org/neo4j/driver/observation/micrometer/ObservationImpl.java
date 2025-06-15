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
package org.neo4j.driver.observation.micrometer;

import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import java.util.Objects;
import org.neo4j.driver.internal.observation.Observation;
import reactor.util.context.Context;

class ObservationImpl implements Observation {
    final io.micrometer.observation.Observation delegate;

    ObservationImpl(io.micrometer.observation.Observation delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public Observation start() {
        delegate.start();
        return this;
    }

    @Override
    public Observation error(Throwable error) {
        delegate.error(error);
        return this;
    }

    @Override
    public void stop() {
        delegate.stop();
    }

    @Override
    public Context writeReactiveContext(Context context) {
        return context.put(ObservationThreadLocalAccessor.KEY, delegate);
    }

    @Override
    public Scope openScope() {
        return new ScopeImpl(delegate.openScope());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        var that = (ObservationImpl) o;
        return Objects.equals(delegate, that.delegate);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(delegate);
    }

    private record ScopeImpl(io.micrometer.observation.Observation.Scope delegate) implements Scope {
        private ScopeImpl {
            Objects.requireNonNull(delegate);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
