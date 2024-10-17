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
package org.neo4j.driver.exceptions;

import java.io.Serial;
import org.neo4j.driver.QueryRunner;
import org.neo4j.driver.internal.bolt.api.GqlStatusError;

/**
 * A user is trying to access resources that are no longer valid due to
 * the resources have already been consumed or
 * the {@link QueryRunner} where the resources are created has already been closed.
 */
public class ResultConsumedException extends ClientException {
    @Serial
    private static final long serialVersionUID = 944999841543178703L;

    /**
     * Creates a new instance.
     * @param message the message
     */
    public ResultConsumedException(String message) {
        super(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                "N/A",
                message,
                GqlStatusError.DIAGNOSTIC_RECORD,
                null);
    }
}
