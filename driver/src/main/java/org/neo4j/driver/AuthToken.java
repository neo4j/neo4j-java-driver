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
package org.neo4j.driver;

public interface AuthToken {
    // option 1
    // is most likely re-usable for other auth manager implementations that need an additional metadata
    // for now, we only have one temporal implementation that would use something like AuthData<Long>
    <T> AuthData<T> toAuthData(T metadata);

    // option 2
    // this approach will require similar methods and types should we provide other manager implementations requiring
    // another type of metadata
    AuthTokenWithExpiration withExpirationAt(long expirationTimestamp);
}
