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
package org.neo4j.driver.summary;

import java.util.Set;

/**
 * A filter for {@link Notification} objects served by Neo4j server.
 * <p>
 * Neo4j server is responsible for delivering on configured selection. Filtering is not done by driver itself.
 * <p>
 * Use {@link NotificationFilterConfigs#selection(Set)} to create an instance of {@link NotificationFilterConfig}
 * with desired {@link NotificationFilter} selection.
 */
public interface NotificationFilter {
    /**
     * Returns {@link Severity} this filter accepts.
     *
     * @return notification severity
     */
    Severity severity();

    /**
     * Returns {@link Category} this filter accepts.
     *
     * @return notification category
     */
    Category category();
}
