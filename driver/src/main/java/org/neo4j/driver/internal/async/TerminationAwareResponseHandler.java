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
import java.util.function.Consumer;
import java.util.function.Function;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.adaptedbolt.DriverResponseHandler;
import org.neo4j.driver.internal.util.Futures;

final class TerminationAwareResponseHandler extends DelegatingResponseHandler {
    @SuppressWarnings("deprecation")
    private final Logger log;

    private final TerminationAwareStateLockingExecutor executor;
    private final Consumer<Throwable> throwableConsumer;

    TerminationAwareResponseHandler(
            @SuppressWarnings("deprecation") Logging logging,
            DriverResponseHandler delegate,
            TerminationAwareStateLockingExecutor executor,
            Consumer<Throwable> throwableConsumer) {
        super(delegate);
        this.log = logging.getLog(getClass());
        this.executor = Objects.requireNonNull(executor);
        this.throwableConsumer = Objects.requireNonNull(throwableConsumer);
    }

    @Override
    public void onError(Throwable throwable) {
        throwableConsumer.accept(Futures.completionExceptionCause(throwable));
        super.onError(throwable);
    }

    @Override
    public void onComplete() {
        var throwable = executor.execute(Function.identity());
        if (throwable != null) {
            log.trace(
                    "Reporting an existing %s error to delegate",
                    throwable.getClass().getCanonicalName());
            delegate.onError(throwable);
        }
        log.trace("Completing delegate");
        delegate.onComplete();
    }
}
