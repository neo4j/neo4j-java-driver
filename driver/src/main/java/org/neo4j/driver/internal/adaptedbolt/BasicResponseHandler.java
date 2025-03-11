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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
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
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.adaptedbolt.summary.DiscardSummary;
import org.neo4j.driver.internal.adaptedbolt.summary.PullSummary;

public class BasicResponseHandler implements DriverResponseHandler {
    private final CompletableFuture<Summaries> summariesFuture = new CompletableFuture<>();
    private final List<Value[]> valuesList = new ArrayList<>();

    private BeginSummary beginSummary;
    private RunSummary runSummary;
    private PullSummary pullSummary;
    private DiscardSummary discardSummary;
    private CommitSummary commitSummary;
    private RollbackSummary rollbackSummary;
    private ResetSummary resetSummary;
    private RouteSummary routeSummary;
    private LogoffSummary logoffSummary;
    private LogonSummary logonSummary;
    private TelemetrySummary telemetrySummary;
    private int ignored;
    private Throwable error;

    public CompletionStage<Summaries> summaries() {
        return summariesFuture;
    }

    @Override
    public void onError(Throwable throwable) {
        if (throwable instanceof CompletionException) {
            throwable = throwable.getCause();
        }
        if (error == null) {
            error = throwable;
        } else {
            if (error instanceof Neo4jException && !(throwable instanceof Neo4jException)) {
                // higher order error has occurred
                error = throwable;
            }
        }
    }

    @Override
    public void onBeginSummary(BeginSummary summary) {
        beginSummary = summary;
    }

    @Override
    public void onRunSummary(RunSummary summary) {
        runSummary = summary;
    }

    @Override
    public void onRecord(Value[] fields) {
        valuesList.add(fields);
    }

    @Override
    public void onPullSummary(PullSummary summary) {
        pullSummary = summary;
    }

    @Override
    public void onDiscardSummary(DiscardSummary summary) {
        discardSummary = summary;
    }

    @Override
    public void onCommitSummary(CommitSummary summary) {
        commitSummary = summary;
    }

    @Override
    public void onRollbackSummary(RollbackSummary summary) {
        rollbackSummary = summary;
    }

    @Override
    public void onResetSummary(ResetSummary summary) {
        resetSummary = summary;
    }

    @Override
    public void onRouteSummary(RouteSummary summary) {
        routeSummary = summary;
    }

    @Override
    public void onLogoffSummary(LogoffSummary summary) {
        logoffSummary = summary;
    }

    @Override
    public void onLogonSummary(LogonSummary summary) {
        logonSummary = summary;
    }

    @Override
    public void onTelemetrySummary(TelemetrySummary summary) {
        telemetrySummary = summary;
    }

    @Override
    public void onIgnored() {
        ignored++;
    }

    @Override
    public void onComplete() {
        if (error != null) {
            summariesFuture.completeExceptionally(error);
        } else {
            summariesFuture.complete(new Summaries(
                    beginSummary,
                    runSummary,
                    valuesList,
                    pullSummary,
                    discardSummary,
                    commitSummary,
                    rollbackSummary,
                    resetSummary,
                    routeSummary,
                    logoffSummary,
                    logonSummary,
                    telemetrySummary,
                    ignored));
        }
    }

    public record Summaries(
            BeginSummary beginSummary,
            RunSummary runSummary,
            List<Value[]> valuesList,
            PullSummary pullSummary,
            DiscardSummary discardSummary,
            CommitSummary commitSummary,
            RollbackSummary rollbackSummary,
            ResetSummary resetSummary,
            RouteSummary routeSummary,
            LogoffSummary logoffSummary,
            LogonSummary logonSummary,
            TelemetrySummary telemetrySummary,
            int ignored) {}
}
