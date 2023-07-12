/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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

import static java.lang.System.lineSeparator;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.BookmarkManager;
import org.neo4j.driver.Logging;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.util.Futures;

public class LeakLoggingNetworkSession extends NetworkSession {
    private final String stackTrace;

    public LeakLoggingNetworkSession(
            ConnectionProvider connectionProvider,
            RetryLogic retryLogic,
            DatabaseName databaseName,
            AccessMode mode,
            Set<Bookmark> bookmarks,
            String impersonatedUser,
            long fetchSize,
            Logging logging,
            BookmarkManager bookmarkManager,
            NotificationConfig notificationConfig,
            AuthToken overrideAuthToken) {
        super(
                connectionProvider,
                retryLogic,
                databaseName,
                mode,
                bookmarks,
                impersonatedUser,
                fetchSize,
                logging,
                bookmarkManager,
                notificationConfig,
                overrideAuthToken);
        this.stackTrace = captureStackTrace();
    }

    @Override
    @SuppressWarnings("deprecation")
    protected void finalize() throws Throwable {
        logLeakIfNeeded();
        super.finalize();
    }

    private void logLeakIfNeeded() {
        var isOpen = Futures.blockingGet(currentConnectionIsOpen());
        if (isOpen) {
            log.error(
                    "Neo4j Session object leaked, please ensure that your application "
                            + "fully consumes results in Sessions or explicitly calls `close` on Sessions before disposing of the objects.\n"
                            + "Session was create at:\n"
                            + stackTrace,
                    null);
        }
    }

    private static String captureStackTrace() {
        var elements = Thread.currentThread().getStackTrace();
        return Arrays.stream(elements)
                .map(element -> "\t" + element + lineSeparator())
                .collect(Collectors.joining());
    }
}
