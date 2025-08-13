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
package org.neo4j.driver.internal.adaptedbolt;

import org.neo4j.bolt.connection.observation.BoltExchangeObservation;

final class BoltExchangeObservationImpl extends BoltObservation implements BoltExchangeObservation {
    private final org.neo4j.driver.internal.observation.BoltExchangeObservation delegate;

    BoltExchangeObservationImpl(org.neo4j.driver.internal.observation.BoltExchangeObservation delegate) {
        super(delegate);
        this.delegate = delegate;
    }

    @Override
    public BoltExchangeObservation onWrite(String messageName) {
        delegate.onWrite(messageName);
        return this;
    }

    @Override
    public BoltExchangeObservation onRecord() {
        delegate.onRecord();
        return this;
    }

    @Override
    public BoltExchangeObservation onSummary(String messageName) {
        delegate.onSummary(messageName);
        return this;
    }

    @Override
    public BoltExchangeObservation error(Throwable error) {
        delegate.error(error);
        return this;
    }
}
