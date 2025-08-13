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

import io.micrometer.observation.Observation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.neo4j.driver.internal.observation.BoltExchangeObservation;

final class BoltExchangeObservationImpl extends ObservationImpl implements BoltExchangeObservation {
    private final BoltExchangeContext context;
    private final List<String> messageNames = Collections.synchronizedList(new ArrayList<>());

    BoltExchangeObservationImpl(Observation delegate) {
        super(delegate);
        this.context = (BoltExchangeContext) Objects.requireNonNull(delegate.getContext());
    }

    @Override
    public BoltExchangeObservation start() {
        super.start();
        return this;
    }

    @Override
    public BoltExchangeObservation onWrite(String messageName) {
        messageNames.add(messageName);
        return this;
    }

    @Override
    public BoltExchangeObservation onRecord() {
        return this;
    }

    @Override
    public BoltExchangeObservation onSummary(String messageName) {
        delegate.event(Observation.Event.of(
                "summary.%s".formatted(messageName.toLowerCase()), "%s summary".formatted(messageName)));
        return this;
    }

    @Override
    public BoltExchangeObservation error(Throwable error) {
        super.error(error);
        return this;
    }

    @Override
    public void stop() {
        context.setMessageNames(messageNames);
        super.stop();
    }
}
