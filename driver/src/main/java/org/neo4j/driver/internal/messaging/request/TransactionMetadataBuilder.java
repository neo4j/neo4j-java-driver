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
package org.neo4j.driver.internal.messaging.request;

import static java.util.Collections.emptyMap;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.util.Iterables;

public class TransactionMetadataBuilder {
    private static final String BOOKMARKS_METADATA_KEY = "bookmarks";
    private static final String DATABASE_NAME_KEY = "db";
    private static final String TX_TIMEOUT_METADATA_KEY = "tx_timeout";
    private static final String TX_METADATA_METADATA_KEY = "tx_metadata";
    private static final String MODE_KEY = "mode";
    private static final String MODE_READ_VALUE = "r";
    private static final String IMPERSONATED_USER_KEY = "imp_user";
    private static final String TX_TYPE_KEY = "tx_type";
    private static final String NOTIFICATIONS_KEY = "notifications";

    public static Map<String, Value> buildMetadata(
            Duration txTimeout,
            Map<String, Value> txMetadata,
            AccessMode mode,
            Set<Bookmark> bookmarks,
            String impersonatedUser,
            String txType,
            Value notifications) {
        return buildMetadata(
                txTimeout, txMetadata, defaultDatabase(), mode, bookmarks, impersonatedUser, txType, notifications);
    }

    public static Map<String, Value> buildMetadata(
            Duration txTimeout,
            Map<String, Value> txMetadata,
            DatabaseName databaseName,
            AccessMode mode,
            Set<Bookmark> bookmarks,
            String impersonatedUser,
            String txType,
            Value notifications) {
        boolean bookmarksPresent = !bookmarks.isEmpty();
        boolean txTimeoutPresent = txTimeout != null;
        boolean txMetadataPresent = txMetadata != null && !txMetadata.isEmpty();
        boolean accessModePresent = mode == AccessMode.READ;
        boolean databaseNamePresent = databaseName.databaseName().isPresent();
        boolean impersonatedUserPresent = impersonatedUser != null;
        boolean txTypePresent = txType != null;

        if (!bookmarksPresent
                && !txTimeoutPresent
                && !txMetadataPresent
                && !accessModePresent
                && !databaseNamePresent
                && !impersonatedUserPresent
                && !txTypePresent) {
            return emptyMap();
        }

        Map<String, Value> result = Iterables.newHashMapWithSize(5);

        if (bookmarksPresent) {
            result.put(BOOKMARKS_METADATA_KEY, value(bookmarks.stream().map(Bookmark::value)));
        }
        if (txTimeoutPresent) {
            result.put(TX_TIMEOUT_METADATA_KEY, value(txTimeout.toMillis()));
        }
        if (txMetadataPresent) {
            result.put(TX_METADATA_METADATA_KEY, value(txMetadata));
        }
        if (accessModePresent) {
            result.put(MODE_KEY, value(MODE_READ_VALUE));
        }
        if (impersonatedUserPresent) {
            result.put(IMPERSONATED_USER_KEY, value(impersonatedUser));
        }
        if (txTypePresent) {
            result.put(TX_TYPE_KEY, value(txType));
        }
        if (notifications != null) {
            result.put(NOTIFICATIONS_KEY, notifications);
        }

        databaseName.databaseName().ifPresent(name -> result.put(DATABASE_NAME_KEY, value(name)));

        return result;
    }
}
