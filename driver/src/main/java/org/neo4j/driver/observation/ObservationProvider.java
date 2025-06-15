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
package org.neo4j.driver.observation;

import org.neo4j.driver.util.Preview;

/**
 * An observation provider that is used by the driver to manage observations.
 * <p>
 * Implementations of this interface are shipped as separate modules that should be used with the driver. This
 * interface MUST NOT be implemented directly.
 *
 * @since 6.0.0
 */
@Preview(name = "Observability")
public interface ObservationProvider {}
