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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.request;

import static java.util.Collections.emptyMap;
import static org.neo4j.driver.Values.value;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;

public class TransactionMetadataBuilder {
    private static final String BOOKMARKS_METADATA_KEY = "bookmarks";
    private static final String DATABASE_NAME_KEY = "db";
    private static final String TX_TIMEOUT_METADATA_KEY = "tx_timeout";
    private static final String TX_METADATA_METADATA_KEY = "tx_metadata";
    private static final String MODE_KEY = "mode";
    private static final String MODE_READ_VALUE = "r";
    private static final String IMPERSONATED_USER_KEY = "imp_user";
    private static final String TX_TYPE_KEY = "tx_type";

    public static Map<String, Value> buildMetadata(
            Duration txTimeout,
            Map<String, Value> txMetadata,
            DatabaseName databaseName,
            AccessMode mode,
            Set<String> bookmarks,
            String impersonatedUser,
            String txType,
            NotificationConfig notificationConfig,
            boolean legacyNotifications,
            LoggingProvider logging) {
        var bookmarksPresent = !bookmarks.isEmpty();
        var txTimeoutPresent = txTimeout != null;
        var txMetadataPresent = txMetadata != null && !txMetadata.isEmpty();
        var accessModePresent = mode == AccessMode.READ;
        var databaseNamePresent = databaseName.databaseName().isPresent();
        var impersonatedUserPresent = impersonatedUser != null;
        var txTypePresent = txType != null;
        var notificationConfigPresent = notificationConfig != null;

        if (!bookmarksPresent
                && !txTimeoutPresent
                && !txMetadataPresent
                && !accessModePresent
                && !databaseNamePresent
                && !impersonatedUserPresent
                && !txTypePresent
                && !notificationConfigPresent) {
            return emptyMap();
        }

        Map<String, Value> result = new HashMap<>(5);

        if (bookmarksPresent) {
            result.put(BOOKMARKS_METADATA_KEY, value(bookmarks));
        }
        if (txTimeoutPresent) {
            var millis = txTimeout.toMillis();
            if (txTimeout.toNanosPart() % 1_000_000 > 0) {
                var log = logging.getLog(TransactionMetadataBuilder.class);
                millis++;
                log.log(
                        System.Logger.Level.INFO,
                        "The transaction timeout has been rounded up to next millisecond value since the config had a fractional millisecond value");
            }
            result.put(TX_TIMEOUT_METADATA_KEY, value(millis));
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
        MessageWithMetadata.appendNotificationConfig(result, notificationConfig, legacyNotifications);

        databaseName.databaseName().ifPresent(name -> result.put(DATABASE_NAME_KEY, value(name)));

        return result;
    }
}
