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
package neo4j.org.testkit.backend;

import lombok.Getter;
import lombok.Setter;

import org.neo4j.driver.Session;

@Getter
@Setter
public class SessionState
{
    public Session session;
    public int     retryableState;
    public String  retryableErrorId;

    public SessionState(Session session) {
        this.session = session;
        this.retryableState = 0;
        this.retryableErrorId = "";
    }
}
