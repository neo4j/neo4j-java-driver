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
package org.neo4j.driver.stress;

import static org.neo4j.driver.SessionConfig.builder;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.reactivestreams.ReactiveSession;

public abstract class AbstractRxQuery<C extends AbstractContext> implements RxCommand<C> {
    protected final Driver driver;
    protected final boolean useBookmark;

    public AbstractRxQuery(Driver driver, boolean useBookmark) {
        this.driver = driver;
        this.useBookmark = useBookmark;
    }

    public ReactiveSession newSession(AccessMode mode, C context) {
        if (useBookmark) {
            return driver.session(
                    ReactiveSession.class,
                    builder()
                            .withDefaultAccessMode(mode)
                            .withBookmarks(context.getBookmark())
                            .build());
        }
        return driver.session(
                ReactiveSession.class, builder().withDefaultAccessMode(mode).build());
    }
}
