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
import org.neo4j.driver.internal.adaptedbolt.summary.DiscardSummary;
import org.neo4j.driver.internal.adaptedbolt.summary.PullSummary;

public interface DriverResponseHandler {

    void onError(Throwable throwable);

    default void onBeginSummary(BeginSummary summary) {
        // ignored
    }

    default void onRunSummary(RunSummary summary) {
        // ignored
    }

    default void onRecord(Value[] fields) {
        // ignored
    }

    default void onPullSummary(PullSummary summary) {
        // ignored
    }

    default void onDiscardSummary(DiscardSummary summary) {
        // ignored
    }

    default void onCommitSummary(CommitSummary summary) {
        // ignored
    }

    default void onRollbackSummary(RollbackSummary summary) {
        // ignored
    }

    default void onResetSummary(ResetSummary summary) {
        // ignored
    }

    default void onRouteSummary(RouteSummary summary) {
        // ignored
    }

    default void onLogoffSummary(LogoffSummary summary) {
        // ignored
    }

    default void onLogonSummary(LogonSummary summary) {
        // ignored
    }

    default void onTelemetrySummary(TelemetrySummary summary) {
        // ignored
    }

    default void onIgnored() {
        // ignored
    }

    default void onComplete() {
        // ignored
    }
}
