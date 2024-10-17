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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.v57;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ProtocolException;
import org.neo4j.driver.internal.bolt.api.GqlError;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.ResponseMessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v5.MessageReaderV5;
import org.neo4j.driver.internal.bolt.basicimpl.packstream.PackInput;
import org.neo4j.driver.types.Type;
import org.neo4j.driver.types.TypeSystem;

public class MessageReaderV57 extends MessageReaderV5 {
    private static final Type MAP_TYPE = TypeSystem.getDefault().MAP();

    public MessageReaderV57(PackInput input) {
        super(input);
    }

    @Override
    protected void unpackFailureMessage(ResponseMessageHandler output) throws IOException {
        var params = unpacker.unpackMap();
        var gqlError = unpackGqlError(params);
        output.handleFailureMessage(gqlError);
    }

    @SuppressWarnings("DuplicatedCode")
    protected GqlError unpackGqlError(Map<String, Value> params) {
        var gqlStatus = params.get("gql_status").asString();
        var statusDescription = params.get("description").asString();
        var code = params.getOrDefault("neo4j_code", Values.value("N/A")).asString();
        var message = params.get("message").asString();
        Map<String, Value> diagnosticRecord;
        var diagnosticRecordValue = params.get("diagnostic_record");
        if (diagnosticRecordValue != null && MAP_TYPE.isTypeOf(diagnosticRecordValue)) {
            var containsOperation = diagnosticRecordValue.containsKey("OPERATION");
            var containsOperationCode = diagnosticRecordValue.containsKey("OPERATION_CODE");
            var containsCurrentSchema = diagnosticRecordValue.containsKey("CURRENT_SCHEMA");
            if (containsOperation && containsOperationCode && containsCurrentSchema) {
                diagnosticRecord = diagnosticRecordValue.asMap(Values::value);
            } else {
                diagnosticRecord = new HashMap<>(diagnosticRecordValue.asMap(Values::value));
                if (!containsOperation) {
                    diagnosticRecord.put("OPERATION", Values.value(""));
                }
                if (!containsOperationCode) {
                    diagnosticRecord.put("OPERATION_CODE", Values.value("0"));
                }
                if (!containsCurrentSchema) {
                    diagnosticRecord.put("CURRENT_SCHEMA", Values.value("/"));
                }
                diagnosticRecord = Collections.unmodifiableMap(diagnosticRecord);
            }
        } else {
            diagnosticRecord = Map.ofEntries(
                    Map.entry("OPERATION", Values.value("")),
                    Map.entry("OPERATION_CODE", Values.value("0")),
                    Map.entry("CURRENT_SCHEMA", Values.value("/")));
        }

        GqlError gqlError = null;
        var cause = params.get("cause");
        if (cause != null) {
            if (!MAP_TYPE.isTypeOf(cause)) {
                throw new ProtocolException("Unexpected type");
            }
            gqlError = unpackGqlError(cause.asMap(Values::value));
        }

        return new GqlError(gqlStatus, statusDescription, code, message, diagnosticRecord, gqlError);
    }
}
