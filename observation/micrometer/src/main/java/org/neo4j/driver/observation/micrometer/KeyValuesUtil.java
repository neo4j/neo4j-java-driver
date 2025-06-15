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
package org.neo4j.driver.observation.micrometer;

import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import io.micrometer.common.docs.KeyName;
import java.util.ArrayList;
import org.neo4j.driver.types.MapAccessor;

final class KeyValuesUtil {
    static final String DB_SYSTEM_NAME = "neo4j";

    static KeyValues queryAndParameters(
            KeyName queryTextKeyName,
            String query,
            MapAccessor parameters,
            String keyNameFormat,
            boolean alwaysAddQuery,
            boolean addParameters) {
        var keyAndValues = new ArrayList<KeyValue>();
        if (parameters.size() > 0) {
            keyAndValues.add(queryTextKeyName.withValue(query));
            if (addParameters) {
                keyAndValues.add(queryTextKeyName.withValue(query));
                for (var key : parameters.keys()) {
                    var value = parameters.get(key).toString();
                    var keyName = keyNameFormat.replace("<key>", key);
                    keyAndValues.add(KeyValue.of(keyName, value));
                }
            }
        } else if (alwaysAddQuery) {
            keyAndValues.add(queryTextKeyName.withValue(query));
        }
        return KeyValues.of(keyAndValues);
    }
}
