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
package org.neo4j.driver.stress;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.reactive.RxSession;

import static org.neo4j.driver.SessionConfig.builder;

public abstract class AbstractRxQuery<C extends AbstractContext> implements RxCommand<C>
{
    protected final Driver driver;
    protected final boolean useBookmark;

    public AbstractRxQuery( Driver driver, boolean useBookmark )
    {
        this.driver = driver;
        this.useBookmark = useBookmark;
    }

    public RxSession newSession( AccessMode mode, C context )
    {
        if ( useBookmark )
        {
            return driver.rxSession( builder().withDefaultAccessMode( mode ).withBookmarks( context.getBookmark() ).build() );
        }
        return driver.rxSession( builder().withDefaultAccessMode( mode ).build() );
    }
}
