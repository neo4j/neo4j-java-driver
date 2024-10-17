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
package org.neo4j.driver.internal.bolt.api;

import javax.net.ssl.SSLContext;

/**
 * A SecurityPlan consists of encryption and trust details.
 */
public interface SecurityPlan {
    SecurityPlan INSECURE = new SecurityPlanImpl(false, false, null, false);

    boolean requiresEncryption();

    boolean requiresClientAuth();

    SSLContext sslContext();

    boolean requiresHostnameVerification();
}
