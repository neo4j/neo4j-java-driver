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
package org.neo4j.driver.internal;

import java.io.Serial;
import java.util.Objects;
import org.neo4j.driver.RoutingControl;

public final class InternalRoutingControl implements RoutingControl {
    public static final RoutingControl WRITE = new InternalRoutingControl("WRITE");
    public static final RoutingControl READ = new InternalRoutingControl("READ");

    @Serial
    private static final long serialVersionUID = 6766432177358809940L;

    private final String mode;

    private InternalRoutingControl(String mode) {
        Objects.requireNonNull(mode, "mode must not be null");
        this.mode = mode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var that = (InternalRoutingControl) o;
        return mode.equals(that.mode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode);
    }
}
