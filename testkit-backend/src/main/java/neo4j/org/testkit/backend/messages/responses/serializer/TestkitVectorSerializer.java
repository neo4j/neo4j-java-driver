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
package neo4j.org.testkit.backend.messages.responses.serializer;

import static neo4j.org.testkit.backend.messages.responses.serializer.GenUtils.cypherObject;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.io.Serial;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import neo4j.org.testkit.backend.messages.VectorType;
import org.neo4j.driver.internal.value.VectorValue;
import org.neo4j.driver.types.Float32Vector;
import org.neo4j.driver.types.Float64Vector;
import org.neo4j.driver.types.Int16Vector;
import org.neo4j.driver.types.Int32Vector;
import org.neo4j.driver.types.Int64Vector;
import org.neo4j.driver.types.Int8Vector;
import org.neo4j.driver.types.Vector;

public class TestkitVectorSerializer extends StdSerializer<VectorValue> {
    @Serial
    private static final long serialVersionUID = 5456264010641357998L;

    public TestkitVectorSerializer() {
        super(VectorValue.class);
    }

    @Override
    public void serialize(VectorValue vectorValue, JsonGenerator gen, SerializerProvider serializerProvider)
            throws IOException {
        String dtype;
        String data;
        var vector = vectorValue.as(Vector.class);
        switch (vector) {
            case Int8Vector int8Vector -> {
                dtype = VectorType.BYTE.getName();
                data = toHexString(int8Vector.toArray());
            }
            case Int16Vector int16Vector -> {
                dtype = VectorType.SHORT.getName();
                data = toHexString(int16Vector.toArray());
            }
            case Int32Vector int32Vector -> {
                dtype = VectorType.INT.getName();
                data = toHexString(int32Vector.toArray());
            }
            case Int64Vector int64Vector -> {
                dtype = VectorType.LONG.getName();
                data = toHexString(int64Vector.toArray());
            }
            case Float32Vector float32Vector -> {
                dtype = VectorType.FLOAT.getName();
                data = toHexString(float32Vector.toArray());
            }
            case Float64Vector float64Vector -> {
                dtype = VectorType.DOUBLE.getName();
                data = toHexString(float64Vector.toArray());
            }
            default ->
                throw new IllegalArgumentException(
                        "Unsupported vector type: " + vector.getClass().getName());
        }

        cypherObject(gen, "CypherVector", () -> {
            gen.writeFieldName("dtype");
            gen.writeString(dtype);
            gen.writeFieldName("data");
            gen.writeString(data);
        });
    }

    public static String toHexString(byte[] array) {
        return IntStream.range(0, array.length)
                .mapToObj(i -> String.format("%02x", array[i] & 0xFF))
                .collect(Collectors.joining(" "));
    }

    public static String toHexString(short[] array) {
        return IntStream.range(0, array.length)
                .mapToObj(i -> String.format("%02x %02x", (array[i] >> 8) & 0xFF, array[i] & 0xFF))
                .collect(Collectors.joining(" "));
    }

    public static String toHexString(int[] array) {
        return Arrays.stream(array)
                .mapToObj(val -> String.format(
                        "%02x %02x %02x %02x", (val >> 24) & 0xFF, (val >> 16) & 0xFF, (val >> 8) & 0xFF, val & 0xFF))
                .collect(Collectors.joining(" "));
    }

    public static String toHexString(long[] array) {
        return Arrays.stream(array)
                .mapToObj(val -> String.format(
                        "%02x %02x %02x %02x %02x %02x %02x %02x",
                        (val >> 56) & 0xFF,
                        (val >> 48) & 0xFF,
                        (val >> 40) & 0xFF,
                        (val >> 32) & 0xFF,
                        (val >> 24) & 0xFF,
                        (val >> 16) & 0xFF,
                        (val >> 8) & 0xFF,
                        val & 0xFF))
                .collect(Collectors.joining(" "));
    }

    public static String toHexString(float[] array) {
        return toHexString(IntStream.range(0, array.length)
                .map(i -> Float.floatToRawIntBits(array[i]))
                .toArray());
    }

    public static String toHexString(double[] array) {
        return toHexString(
                Arrays.stream(array).mapToLong(Double::doubleToRawLongBits).toArray());
    }
}
