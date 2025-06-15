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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.message.BeginMessage;
import org.neo4j.bolt.connection.message.CommitMessage;
import org.neo4j.bolt.connection.message.DiscardMessage;
import org.neo4j.bolt.connection.message.LogoffMessage;
import org.neo4j.bolt.connection.message.LogonMessage;
import org.neo4j.bolt.connection.message.PullMessage;
import org.neo4j.bolt.connection.message.ResetMessage;
import org.neo4j.bolt.connection.message.RollbackMessage;
import org.neo4j.bolt.connection.message.RouteMessage;
import org.neo4j.bolt.connection.message.RunMessage;
import org.neo4j.bolt.connection.message.TelemetryMessage;
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
import org.neo4j.driver.internal.observation.BoltHandleObservation;
import org.neo4j.driver.internal.value.BoltValueFactory;

final class AdaptingDriverResponseHandler implements ResponseHandler {
    private final DriverResponseHandler delegate;
    private final ErrorMapper errorMapper;
    private final BoltValueFactory boltValueFactory;
    private final BoltHandleObservation observation;

    AdaptingDriverResponseHandler(
            DriverResponseHandler delegate,
            ErrorMapper errorMapper,
            BoltValueFactory boltValueFactory,
            BoltHandleObservation observation) {
        this.delegate = Objects.requireNonNull(delegate);
        this.errorMapper = Objects.requireNonNull(errorMapper);
        this.boltValueFactory = Objects.requireNonNull(boltValueFactory);
        this.observation = Objects.requireNonNull(observation);
    }

    @Override
    public void onError(Throwable throwable) {
        throwable = errorMapper.map(throwable);
        observation.error(throwable);
        delegate.onError(throwable);
    }

    @Override
    public void onBeginSummary(BeginSummary summary) {
        observation.onSummary(BeginMessage.NAME);
        delegate.onBeginSummary(summary);
    }

    @Override
    public void onRunSummary(RunSummary summary) {
        observation.onSummary(RunMessage.NAME);
        delegate.onRunSummary(summary);
    }

    @Override
    public void onRecord(List<org.neo4j.bolt.connection.values.Value> fields) {
        delegate.onRecord(boltValueFactory.toDriverList(fields));
    }

    @Override
    public void onPullSummary(PullSummary summary) {
        observation.onSummary(PullMessage.NAME);
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
        observation.onSummary(DiscardMessage.NAME);
        delegate.onDiscardSummary(() -> boltValueFactory.toDriverMap(summary.metadata()));
    }

    @Override
    public void onCommitSummary(CommitSummary summary) {
        observation.onSummary(CommitMessage.NAME);
        delegate.onCommitSummary(summary);
    }

    @Override
    public void onRollbackSummary(RollbackSummary summary) {
        observation.onSummary(RollbackMessage.NAME);
        delegate.onRollbackSummary(summary);
    }

    @Override
    public void onResetSummary(ResetSummary summary) {
        observation.onSummary(ResetMessage.NAME);
        delegate.onResetSummary(summary);
    }

    @Override
    public void onRouteSummary(RouteSummary summary) {
        observation.onSummary(RouteMessage.NAME);
        delegate.onRouteSummary(summary);
    }

    @Override
    public void onLogoffSummary(LogoffSummary summary) {
        observation.onSummary(LogoffMessage.NAME);
        delegate.onLogoffSummary(summary);
    }

    @Override
    public void onLogonSummary(LogonSummary summary) {
        observation.onSummary(LogonMessage.NAME);
        delegate.onLogonSummary(summary);
    }

    @Override
    public void onTelemetrySummary(TelemetrySummary summary) {
        observation.onSummary(TelemetryMessage.NAME);
        delegate.onTelemetrySummary(summary);
    }

    @Override
    public void onIgnored() {
        observation.onIgnored();
        delegate.onIgnored();
    }

    @Override
    public void onComplete() {
        observation.stop();
        delegate.onComplete();
    }
}
