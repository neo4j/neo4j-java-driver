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

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.summary.BeginSummary;
import org.neo4j.bolt.connection.summary.CommitSummary;
import org.neo4j.bolt.connection.summary.DiscardSummary;
import org.neo4j.bolt.connection.summary.LogoffSummary;
import org.neo4j.bolt.connection.summary.LogonSummary;
import org.neo4j.bolt.connection.summary.PullSummary;
import org.neo4j.bolt.connection.summary.ResetSummary;
import org.neo4j.bolt.connection.summary.RollbackSummary;
import org.neo4j.bolt.connection.summary.RouteSummary;
import org.neo4j.bolt.connection.summary.RunSummary;
import org.neo4j.bolt.connection.summary.TelemetrySummary;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.BoltValue;
import org.neo4j.driver.internal.value.BoltValueFactory;

final class AdaptingDriverResponseHandler implements ResponseHandler {
    private final DriverResponseHandler delegate;
    private final ErrorMapper errorMapper;
    private final BoltValueFactory boltValueFactory;

    AdaptingDriverResponseHandler(
            DriverResponseHandler delegate, ErrorMapper errorMapper, BoltValueFactory boltValueFactory) {
        this.delegate = Objects.requireNonNull(delegate);
        this.errorMapper = Objects.requireNonNull(errorMapper);
        this.boltValueFactory = Objects.requireNonNull(boltValueFactory);
    }

    @Override
    public void onError(Throwable throwable) {
        delegate.onError(errorMapper.map(throwable));
    }

    @Override
    public void onBeginSummary(BeginSummary summary) {
        delegate.onBeginSummary(summary);
    }

    @Override
    public void onRunSummary(RunSummary summary) {
        delegate.onRunSummary(summary);
    }

    @Override
    public void onRecord(org.neo4j.bolt.connection.values.Value[] fields) {
        var mappedFields = Arrays.stream(fields)
                .map(field -> ((BoltValue) field).asDriverValue())
                .toArray(Value[]::new);
        delegate.onRecord(mappedFields);
    }

    @Override
    public void onPullSummary(PullSummary summary) {
        delegate.onPullSummary(new org.neo4j.driver.internal.adaptedbolt.summary.PullSummary() {
            @Override
            public boolean hasMore() {
                return summary.hasMore();
            }

            @Override
            public Map<String, Value> metadata() {
                return boltValueFactory.toDriverMap(summary.metadata());
            }
        });
    }

    @Override
    public void onDiscardSummary(DiscardSummary summary) {
        delegate.onDiscardSummary(() -> boltValueFactory.toDriverMap(summary.metadata()));
    }

    @Override
    public void onCommitSummary(CommitSummary summary) {
        delegate.onCommitSummary(summary);
    }

    @Override
    public void onRollbackSummary(RollbackSummary summary) {
        delegate.onRollbackSummary(summary);
    }

    @Override
    public void onResetSummary(ResetSummary summary) {
        delegate.onResetSummary(summary);
    }

    @Override
    public void onRouteSummary(RouteSummary summary) {
        delegate.onRouteSummary(summary);
    }

    @Override
    public void onLogoffSummary(LogoffSummary summary) {
        delegate.onLogoffSummary(summary);
    }

    @Override
    public void onLogonSummary(LogonSummary summary) {
        delegate.onLogonSummary(summary);
    }

    @Override
    public void onTelemetrySummary(TelemetrySummary summary) {
        delegate.onTelemetrySummary(summary);
    }

    @Override
    public void onIgnored() {
        delegate.onIgnored();
    }

    @Override
    public void onComplete() {
        delegate.onComplete();
    }
}
