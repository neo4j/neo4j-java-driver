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
import org.neo4j.driver.util.Preview;

/**
 * @since 6.0.0
 */
@Preview(name = "Observability")
public class DefaultSessionRunConvention implements SessionRunConvention {
    private static final KeyValue DB_SYSTEM_NAME =
            Neo4jDriverDocumentation.SessionRunLowCardinalityKeyNames.DB_SYSTEM_NAME.withValue(
                    KeyValuesUtil.DB_SYSTEM_NAME);
    private final boolean alwaysAddQuery;
    private final boolean addParameters;

    public DefaultSessionRunConvention(boolean alwaysAddQuery, boolean addParameters) {
        this.alwaysAddQuery = alwaysAddQuery;
        this.addParameters = addParameters;
    }

    @Override
    public String getName() {
        return "neo4j.db.client.session.run.duration";
    }

    @Override
    public String getContextualName(SessionRunContext context) {
        return "session run";
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(SessionRunContext context) {
        return KeyValues.of(DB_SYSTEM_NAME, sessionType(context));
    }

    @Override
    public KeyValues getHighCardinalityKeyValues(SessionRunContext context) {
        return KeyValuesUtil.queryAndParameters(
                Neo4jDriverDocumentation.SessionRunHighCardinalityKeyNames.DB_QUERY_TEXT,
                context.query(),
                context.parameters(),
                Neo4jDriverDocumentation.SessionRunHighCardinalityKeyNames.DB_QUERY_PARAMETER_FORMAT.asString(),
                alwaysAddQuery,
                addParameters);
    }

    private KeyValue sessionType(SessionRunContext context) {
        return Neo4jDriverDocumentation.SessionRunLowCardinalityKeyNames.SESSION_TYPE.withValue(
                context.sessionType().getSimpleName());
    }
}
