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
package org.neo4j.driver.summary;

import java.util.Map;
import org.neo4j.driver.Value;
import org.neo4j.driver.util.Preview;

/**
 * The GQL-status object as defined by the GQL standard.
 * @since 5.22.0
 * @see Notification Notification subtype of the GQL-status object
 */
@Preview(name = "GQL-status object")
public interface GqlStatusObject {
    /**
     * Returns the GQLSTATUS as defined by the GQL standard.
     * @return the GQLSTATUS value
     */
    String gqlStatus();

    /**
     * The GQLSTATUS description.
     * @return the GQLSTATUS description
     */
    String statusDescription();

    /**
     * Returns the diagnostic record.
     * @return the diagnostic record
     */
    Map<String, Value> diagnosticRecord();
}
