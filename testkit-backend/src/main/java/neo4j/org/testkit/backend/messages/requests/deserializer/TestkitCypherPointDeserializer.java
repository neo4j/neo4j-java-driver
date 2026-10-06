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
package neo4j.org.testkit.backend.messages.requests.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.io.Serial;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.Point;

public class TestkitCypherPointDeserializer extends StdDeserializer<Point> {
    @Serial
    private static final long serialVersionUID = -2900194785671874565L;

    @SuppressWarnings("serial")
    private final TestkitCypherTypeMapper mapper;

    public TestkitCypherPointDeserializer() {
        super(Point.class);
        mapper = new TestkitCypherTypeMapper();
    }

    @Override
    public Point deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        var data = mapper.mapData(p, ctxt, new CypherPointData());
        var srid =
                switch (data.system) {
                    case "cartesian" -> data.z == null ? 7203 : 9157;
                    case "wgs84" -> data.z == null ? 4326 : 4979;
                    default -> throw new IllegalArgumentException("Unknown coordinate system: " + data.system);
                };

        return data.z == null
                ? Values.point(srid, data.x, data.y).asPoint()
                : Values.point(srid, data.x, data.y, data.z).asPoint();
    }

    private static final class CypherPointData {
        String system;
        Double x;
        Double y;
        Double z;
    }
}
