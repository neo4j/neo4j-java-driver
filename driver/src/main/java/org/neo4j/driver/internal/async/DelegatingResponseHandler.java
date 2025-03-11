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
package org.neo4j.driver.internal.async;

import java.util.Objects;
import org.neo4j.bolt.connection.summary.BeginSummary;
import org.neo4j.bolt.connection.summary.CommitSummary;
import org.neo4j.bolt.connection.summary.LogoffSummary;
import org.neo4j.bolt.connection.summary.LogonSummary;
import org.neo4j.bolt.connection.summary.ResetSummary;
import org.neo4j.bolt.connection.summary.RollbackSummary;
import org.neo4j.bolt.connection.summary.RouteSummary;
import org.neo4j.bolt.connection.summary.RunSummary;
import org.neo4j.bolt.connection.summary.TelemetrySummary;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.adaptedbolt.DriverResponseHandler;
import org.neo4j.driver.internal.adaptedbolt.summary.DiscardSummary;
import org.neo4j.driver.internal.adaptedbolt.summary.PullSummary;

abstract class DelegatingResponseHandler implements DriverResponseHandler {
    protected final DriverResponseHandler delegate;

    DelegatingResponseHandler(DriverResponseHandler delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public void onError(Throwable throwable) {
        delegate.onError(throwable);
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
    public void onRecord(Value[] fields) {
        delegate.onRecord(fields);
    }

    @Override
    public void onPullSummary(PullSummary summary) {
        delegate.onPullSummary(summary);
    }

    @Override
    public void onDiscardSummary(DiscardSummary summary) {
        delegate.onDiscardSummary(summary);
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
