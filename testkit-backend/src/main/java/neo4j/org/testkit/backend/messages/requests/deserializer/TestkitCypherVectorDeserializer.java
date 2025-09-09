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
import java.util.Arrays;
import neo4j.org.testkit.backend.messages.VectorType;
import org.neo4j.driver.internal.InternalFloat32Vector;
import org.neo4j.driver.internal.InternalFloat64Vector;
import org.neo4j.driver.internal.InternalInt16Vector;
import org.neo4j.driver.internal.InternalInt32Vector;
import org.neo4j.driver.internal.InternalInt64Vector;
import org.neo4j.driver.internal.InternalInt8Vector;
import org.neo4j.driver.types.Vector;

public class TestkitCypherVectorDeserializer extends StdDeserializer<Vector> {
    @Serial
    private static final long serialVersionUID = 3489940766207129614L;

    @SuppressWarnings("serial")
    private final TestkitCypherTypeMapper mapper;

    public TestkitCypherVectorDeserializer() {
        super(Vector.class);
        mapper = new TestkitCypherTypeMapper();
    }

    @Override
    public Vector deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        var data = mapper.mapData(p, ctxt, new CypherVectorData());
        return switch (VectorType.of(data.dtype)) {
            case BYTE -> new InternalInt8Vector(deserializeToBytes(data.data));
            case SHORT -> new InternalInt16Vector(deserializeToShorts(data.data));
            case INT -> new InternalInt32Vector(deserializeToIntegers(data.data));
            case LONG -> new InternalInt64Vector(deserializeToLongs(data.data));
            case FLOAT -> new InternalFloat32Vector(deserializeToFloats(data.data));
            case DOUBLE -> new InternalFloat64Vector(deserializeToDoubles(data.data));
        };
    }

    private static final class CypherVectorData {
        String dtype;
        String data;
    }

    public static byte[] deserializeToBytes(String hex) {
        if (hex.isEmpty()) {
            return new byte[0];
        }
        var parts = hex.trim().split("\\s+");
        var result = new byte[parts.length];
        for (var i = 0; i < parts.length; i++) {
            result[i] = (byte) Integer.parseInt(parts[i], 16);
        }
        return result;
    }

    public static short[] deserializeToShorts(String hex) {
        if (hex.isEmpty()) {
            return new short[0];
        }
        var parts = hex.trim().split("\\s+");
        if (parts.length % 2 != 0) throw new IllegalArgumentException("Invalid string: " + hex);

        var result = new short[parts.length / 2];
        for (var i = 0; i < result.length; i++) {
            var hi = Integer.parseInt(parts[i * 2], 16);
            var lo = Integer.parseInt(parts[i * 2 + 1], 16);
            result[i] = (short) ((hi << 8) | lo);
        }
        return result;
    }

    public static int[] deserializeToIntegers(String hex) {
        if (hex.isEmpty()) {
            return new int[0];
        }
        var parts = hex.trim().split("\\s+");
        if (parts.length % 4 != 0) throw new IllegalArgumentException("Invalid string: " + hex);

        var result = new int[parts.length / 4];
        for (var i = 0; i < result.length; i++) {
            var b0 = Integer.parseInt(parts[i * 4], 16);
            var b1 = Integer.parseInt(parts[i * 4 + 1], 16);
            var b2 = Integer.parseInt(parts[i * 4 + 2], 16);
            var b3 = Integer.parseInt(parts[i * 4 + 3], 16);
            result[i] = (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
        }
        return result;
    }

    public static long[] deserializeToLongs(String hex) {
        if (hex.isEmpty()) {
            return new long[0];
        }
        var parts = hex.trim().split("\\s+");
        if (parts.length % 8 != 0) throw new IllegalArgumentException("Invalid string: " + hex);

        var result = new long[parts.length / 8];
        for (var i = 0; i < result.length; i++) {
            long val = 0;
            for (var j = 0; j < 8; j++) {
                val = (val << 8) | Integer.parseInt(parts[i * 8 + j], 16);
            }
            result[i] = val;
        }
        return result;
    }

    public static float[] deserializeToFloats(String hex) {
        if (hex.isEmpty()) {
            return new float[0];
        }
        var bits = deserializeToIntegers(hex);
        var result = new float[bits.length];
        for (var i = 0; i < bits.length; i++) {
            result[i] = Float.intBitsToFloat(bits[i]);
        }
        return result;
    }

    public static double[] deserializeToDoubles(String hex) {
        if (hex.isEmpty()) {
            return new double[0];
        }
        var bits = deserializeToLongs(hex);
        return Arrays.stream(bits).mapToDouble(Double::longBitsToDouble).toArray();
    }
}
