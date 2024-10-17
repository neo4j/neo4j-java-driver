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
package org.neo4j.driver.internal.bolt.basicimpl.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.UntrustedServerException;

public class MetadataExtractor {
    public static final int ABSENT_QUERY_ID = -1;
    private final String resultAvailableAfterMetadataKey;

    public MetadataExtractor(String resultAvailableAfterMetadataKey) {
        this.resultAvailableAfterMetadataKey = resultAvailableAfterMetadataKey;
    }

    public List<String> extractQueryKeys(Map<String, Value> metadata) {
        var keysValue = metadata.get("fields");
        if (keysValue != null) {
            if (!keysValue.isEmpty()) {
                List<String> keys = new ArrayList<>(keysValue.size());
                for (var value : keysValue.values()) {
                    keys.add(value.asString());
                }

                return keys;
            }
        }
        return Collections.emptyList();
    }

    public long extractQueryId(Map<String, Value> metadata) {
        var queryId = metadata.get("qid");
        if (queryId != null) {
            return queryId.asLong();
        }
        return ABSENT_QUERY_ID;
    }

    public long extractResultAvailableAfter(Map<String, Value> metadata) {
        var resultAvailableAfterValue = metadata.get(resultAvailableAfterMetadataKey);
        if (resultAvailableAfterValue != null) {
            return resultAvailableAfterValue.asLong();
        }
        return -1;
    }

    public static Value extractServer(Map<String, Value> metadata) {
        var versionValue = metadata.get("server");
        if (versionValue == null || versionValue.isNull()) {
            throw new UntrustedServerException("Server provides no product identifier");
        }
        var serverAgent = versionValue.asString();
        if (!serverAgent.startsWith("Neo4j/")) {
            throw new UntrustedServerException(
                    "Server does not identify as a genuine Neo4j instance: '" + serverAgent + "'");
        }
        return versionValue;
    }

    public static Set<String> extractBoltPatches(Map<String, Value> metadata) {
        var boltPatch = metadata.get("patch_bolt");
        if (boltPatch != null && !boltPatch.isNull()) {
            return new HashSet<>(boltPatch.asList(Value::asString));
        } else {
            return Collections.emptySet();
        }
    }
}
