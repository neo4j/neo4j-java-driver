/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.internal.exceptions;

import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

/**
 * Only used in {@link org.neo4j.driver.internal.RoutingDriver} regarding {@link org.neo4j.driver.internal.cluster.LoadBalancer}
 * failed to update routing table.
 *
 * The reason of failing could be interrupted during update/connection terminated during update/no router to use for
 * updating and so on.
 *
 * This error will end up in {@link org.neo4j.driver.v1.exceptions.ServiceUnavailableException} as failing to update
 * routing table means the driver failed to connect to the cluster and can no longer server routing
 */
public class FailedToUpdateRoutingException extends InternalException
{
    public FailedToUpdateRoutingException( String msg, Throwable cause )
    {
        super( msg, cause );
    }

    public FailedToUpdateRoutingException( String msg )
    {
        super( msg );
    }

    public FailedToUpdateRoutingException( String msg, InternalException error )
    {
        super( msg, error );
    }

    @Override
    public Neo4jException publicException()
    {
        return new ServiceUnavailableException( getMessage(), getCause() );
    }
}
