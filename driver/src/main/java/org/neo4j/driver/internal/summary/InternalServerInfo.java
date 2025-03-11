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
package org.neo4j.driver.internal.summary;

import java.util.Objects;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.driver.summary.ServerInfo;

public class InternalServerInfo implements ServerInfo {
    private static final String TO_STRING_FMT = "%s{address='%s'}";

    private final String agent;
    private final String address;
    private final String protocolVersion;

    public InternalServerInfo(String agent, BoltServerAddress address, BoltProtocolVersion protocolVersion) {
        this.agent = agent;
        this.address = address.toString();
        this.protocolVersion = protocolVersion.toString();
    }

    @Override
    public String agent() {
        return agent;
    }

    @Override
    public String address() {
        return address;
    }

    @Override
    public String protocolVersion() {
        return protocolVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (InternalServerInfo) o;
        return Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address);
    }

    @Override
    public String toString() {
        return String.format(TO_STRING_FMT, this.getClass().getSimpleName(), address);
    }
}
