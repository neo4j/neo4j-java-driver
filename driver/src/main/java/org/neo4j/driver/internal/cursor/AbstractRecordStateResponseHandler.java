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
package org.neo4j.driver.internal.cursor;

import java.util.List;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.bolt.api.GqlStatusError;
import org.neo4j.driver.internal.summary.InternalGqlStatusObject;
import org.neo4j.driver.summary.GqlStatusObject;

public abstract class AbstractRecordStateResponseHandler {
    protected RecordState recordState = RecordState.NOT_REQUESTED;

    protected synchronized GqlStatusObject generateGqlStatusObject(List<String> keys) {
        return switch (recordState) {
            case NOT_REQUESTED -> keys.isEmpty()
                    ? InternalGqlStatusObject.OMITTED_RESULT
                    : InternalGqlStatusObject.NO_DATA_UNKNOWN;
            case HAD_RECORD -> InternalGqlStatusObject.SUCCESS;
            case REQUESTED -> {
                var message = "Unexpected state: " + recordState;
                throw new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(message),
                        "N/A",
                        message,
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        null);
            }
            case NO_RECORD -> keys.isEmpty() ? InternalGqlStatusObject.OMITTED_RESULT : InternalGqlStatusObject.NO_DATA;
        };
    }

    protected void updateRecordState(RecordState recordState) {
        if (recordState.ordinal() > this.recordState.ordinal()) {
            this.recordState = recordState;
        }
    }

    protected enum RecordState {
        NOT_REQUESTED,
        REQUESTED,
        NO_RECORD,
        HAD_RECORD
    }
}
