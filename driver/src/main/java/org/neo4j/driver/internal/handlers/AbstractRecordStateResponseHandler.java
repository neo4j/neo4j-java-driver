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
package org.neo4j.driver.internal.handlers;

import java.util.List;
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
            case NO_RECORD -> keys.isEmpty() ? InternalGqlStatusObject.OMITTED_RESULT : InternalGqlStatusObject.NO_DATA;
        };
    }

    protected enum RecordState {
        NOT_REQUESTED,
        HAD_RECORD,
        NO_RECORD
    }
}
