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

/**
 * Token for holding authentication details, such as <em>user name</em> and <em>password</em>.
 * Such a token is required by a {@link Driver} to authenticate with a Neo4j
 * instance.
 *
 * @see AuthTokens
 * @see GraphDatabase#driver(String, AuthToken)
 * @since 1.0
 */
public interface AuthToken
{

}
