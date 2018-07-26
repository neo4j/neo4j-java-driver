/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.internal.util;

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.util.ServerVersion.v3_1_0;
import static org.neo4j.driver.internal.util.ServerVersion.v3_2_0;
import static org.neo4j.driver.internal.util.ServerVersion.v3_4_0;
import static org.neo4j.driver.internal.util.ServerVersion.v3_5_0;

public enum Neo4jFeature
{
    BOOKMARKS( v3_1_0 ),
    CAUSAL_CLUSTER( v3_1_0 ),
    SPATIAL_TYPES( v3_4_0 ),
    TEMPORAL_TYPES( v3_4_0 ),
    TRANSACTION_TERMINATION_AWARE_LOCKS( v3_1_0 ),
    BYTE_ARRAYS( v3_2_0 ),
    READ_ON_FOLLOWERS_BY_DEFAULT( v3_2_0 ),
    STATEMENT_RESULT_TIMINGS( v3_1_0 ),
    LIST_QUERIES_PROCEDURE( v3_1_0 ),
    CONNECTOR_LISTEN_ADDRESS_CONFIGURATION( v3_1_0 ),
    BOLT_V3( v3_5_0 );

    private final ServerVersion availableFromVersion;

    Neo4jFeature( ServerVersion availableFromVersion )
    {
        this.availableFromVersion = requireNonNull( availableFromVersion );
    }

    public boolean availableIn( ServerVersion version )
    {
        return version.greaterThanOrEqual( availableFromVersion );
    }
}
