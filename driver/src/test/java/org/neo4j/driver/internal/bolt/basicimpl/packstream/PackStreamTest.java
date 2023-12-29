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
package org.neo4j.driver.internal.bolt.basicimpl.packstream;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.bolt.basicimpl.util.io.BufferedChannelInput;
import org.neo4j.driver.internal.bolt.basicimpl.util.io.ChannelOutput;
import org.neo4j.driver.internal.util.Iterables;

public class PackStreamTest {
    public static Map<String, Object> asMap(Object... keysAndValues) {
        Map<String, Object> map = Iterables.newLinkedHashMapWithSize(keysAndValues.length / 2);
        String key = null;
        for (var keyOrValue : keysAndValues) {
            if (key == null) {
                key = keyOrValue.toString();
            } else {
                map.put(key, keyOrValue);
                key = null;
            }
        }
        return map;
    }

    private static class Machine {

        private final ByteArrayOutputStream output;
        private final PackStream.Packer packer;

        Machine() {
            this.output = new ByteArrayOutputStream();
            this.packer = new PackStream.Packer(new ChannelOutput(Channels.newChannel(this.output)));
        }

        public void reset() {
            output.reset();
        }

        public byte[] output() {
            return output.toByteArray();
        }

        PackStream.Packer packer() {
            return packer;
        }
    }

    private PackStream.Unpacker newUnpacker(byte[] bytes) {
        var input = new ByteArrayInputStream(bytes);
        return new PackStream.Unpacker(new BufferedChannelInput(Channels.newChannel(input)));
    }

    @Test
    void testCanPackAndUnpackNull() throws Throwable {
        // Given
        var machine = new Machine();

        // When
        machine.packer().packNull();

        // Then
        var bytes = machine.output();
        assertThat(bytes, equalTo(new byte[] {(byte) 0xC0}));

        // When
        var unpacker = newUnpacker(bytes);
        var packType = unpacker.peekNextType();

        // Then
        assertThat(packType, equalTo(PackType.NULL));
    }

    @Test
    void testCanPackAndUnpackTrue() throws Throwable {
        // Given
        var machine = new Machine();

        // When
        machine.packer().pack(true);

        // Then
        var bytes = machine.output();
        assertThat(bytes, equalTo(new byte[] {(byte) 0xC3}));

        // When
        var unpacker = newUnpacker(bytes);
        var packType = unpacker.peekNextType();

        // Then
        assertThat(packType, equalTo(PackType.BOOLEAN));
        assertThat(unpacker.unpackBoolean(), equalTo(true));
    }

    @Test
    void testCanPackAndUnpackFalse() throws Throwable {
        // Given
        var machine = new Machine();

        // When
        machine.packer().pack(false);

        // Then
        var bytes = machine.output();
        assertThat(bytes, equalTo(new byte[] {(byte) 0xC2}));

        // When
        var unpacker = newUnpacker(bytes);
        var packType = unpacker.peekNextType();

        // Then
        assertThat(packType, equalTo(PackType.BOOLEAN));
        assertThat(unpacker.unpackBoolean(), equalTo(false));
    }

    @Test
    void testCanPackAndUnpackTinyIntegers() throws Throwable {
        // Given
        var machine = new Machine();

        for (long i = -16; i < 128; i++) {
            // When
            machine.reset();
            machine.packer().pack(i);

            // Then
            var bytes = machine.output();
            assertThat(bytes.length, equalTo(1));

            // When
            var unpacker = newUnpacker(bytes);
            var packType = unpacker.peekNextType();

            // Then
            assertThat(packType, equalTo(PackType.INTEGER));
            assertThat(unpacker.unpackLong(), equalTo(i));
        }
    }

    @Test
    void testCanPackAndUnpackShortIntegers() throws Throwable {
        // Given
        var machine = new Machine();

        for (long i = -32768; i < 32768; i++) {
            // When
            machine.reset();
            machine.packer().pack(i);

            // Then
            var bytes = machine.output();
            assertThat(bytes.length, lessThanOrEqualTo(3));

            // When
            var unpacker = newUnpacker(bytes);
            var packType = unpacker.peekNextType();

            // Then
            assertThat(packType, equalTo(PackType.INTEGER));
            assertThat(unpacker.unpackLong(), equalTo(i));
        }
    }

    @Test
    void testCanPackAndUnpackPowersOfTwoAsIntegers() throws Throwable {
        // Given
        var machine = new Machine();

        for (var i = 0; i < 32; i++) {
            var n = (long) Math.pow(2, i);

            // When
            machine.reset();
            machine.packer().pack(n);

            // Then
            var unpacker = newUnpacker(machine.output());
            var packType = unpacker.peekNextType();

            // Then
            assertThat(packType, equalTo(PackType.INTEGER));
            assertThat(unpacker.unpackLong(), equalTo(n));
        }
    }

    @Test
    void testCanPackAndUnpackPowersOfTwoPlusABitAsDoubles() throws Throwable {
        // Given
        var machine = new Machine();

        for (var i = 0; i < 32; i++) {
            var n = Math.pow(2, i) + 0.5;

            // When
            machine.reset();
            machine.packer().pack(n);

            // Then
            var unpacker = newUnpacker(machine.output());
            var packType = unpacker.peekNextType();

            // Then
            assertThat(packType, equalTo(PackType.FLOAT));
            assertThat(unpacker.unpackDouble(), equalTo(n));
        }
    }

    @Test
    void testCanPackAndUnpackPowersOfTwoMinusABitAsDoubles() throws Throwable {
        // Given
        var machine = new Machine();

        for (var i = 0; i < 32; i++) {
            var n = Math.pow(2, i) - 0.5;

            // When
            machine.reset();
            machine.packer().pack(n);

            // Then
            var unpacker = newUnpacker(machine.output());
            var packType = unpacker.peekNextType();

            // Then
            assertThat(packType, equalTo(PackType.FLOAT));
            assertThat(unpacker.unpackDouble(), equalTo(n));
        }
    }

    @Test
    void testCanPackAndUnpackByteArrays() throws Throwable {
        // Given
        var machine = new Machine();

        testByteArrayPackingAndUnpacking(machine, 0);
        for (var i = 0; i < 24; i++) {
            testByteArrayPackingAndUnpacking(machine, (int) Math.pow(2, i));
        }
    }

    private void testByteArrayPackingAndUnpacking(Machine machine, int length) throws Throwable {
        var array = new byte[length];

        machine.reset();
        machine.packer().pack(array);

        var unpacker = newUnpacker(machine.output());
        var packType = unpacker.peekNextType();

        assertThat(packType, equalTo(PackType.BYTES));
        assertArrayEquals(array, unpacker.unpackBytes());
    }

    @Test
    void testCanPackAndUnpackStrings() throws Throwable {
        // Given
        var machine = new Machine();

        for (var i = 0; i < 24; i++) {
            var string = new String(new byte[(int) Math.pow(2, i)]);

            // When
            machine.reset();
            machine.packer().pack(string);

            // Then
            var unpacker = newUnpacker(machine.output());
            var packType = unpacker.peekNextType();

            // Then
            assertThat(packType, equalTo(PackType.STRING));
            assertThat(unpacker.unpackString(), equalTo(string));
        }
    }

    @Test
    void testCanPackAndUnpackBytes() throws Throwable {
        // Given
        var machine = new Machine();

        // When
        var packer = machine.packer();
        packer.pack("ABCDEFGHIJ".getBytes());

        // Then
        var unpacker = newUnpacker(machine.output());
        var packType = unpacker.peekNextType();

        // Then
        assertThat(packType, equalTo(PackType.BYTES));
        assertArrayEquals("ABCDEFGHIJ".getBytes(), unpacker.unpackBytes());
    }

    @Test
    void testCanPackAndUnpackString() throws Throwable {
        // Given
        var machine = new Machine();

        // When
        var packer = machine.packer();
        packer.pack("ABCDEFGHIJ");

        // Then
        var unpacker = newUnpacker(machine.output());
        var packType = unpacker.peekNextType();

        // Then
        assertThat(packType, equalTo(PackType.STRING));
        assertThat(unpacker.unpackString(), equalTo("ABCDEFGHIJ"));
    }

    @Test
    void testCanPackAndUnpackSpecialString() throws Throwable {
        // Given
        var machine = new Machine();
        var code = "Mjölnir";

        // When
        var packer = machine.packer();
        packer.pack(code);

        // Then
        var unpacker = newUnpacker(machine.output());
        var packType = unpacker.peekNextType();

        // Then
        assertThat(packType, equalTo(PackType.STRING));
        assertThat(unpacker.unpackString(), equalTo(code));
    }

    @Test
    void testCanPackAndUnpackListOneItemAtATime() throws Throwable {
        // Given
        var machine = new Machine();

        // When
        var packer = machine.packer();
        packer.packListHeader(3);
        packer.pack(12);
        packer.pack(13);
        packer.pack(14);

        // Then
        var unpacker = newUnpacker(machine.output());
        var packType = unpacker.peekNextType();

        assertThat(packType, equalTo(PackType.LIST));
        assertThat(unpacker.unpackListHeader(), equalTo(3L));
        assertThat(unpacker.unpackLong(), equalTo(12L));
        assertThat(unpacker.unpackLong(), equalTo(13L));
        assertThat(unpacker.unpackLong(), equalTo(14L));
    }

    @Test
    void testCanPackAndUnpackListOfString() throws Throwable {
        // Given
        var machine = new Machine();

        // When
        var packer = machine.packer();
        packer.pack(asList("eins", "zwei", "drei"));

        // Then
        var unpacker = newUnpacker(machine.output());
        var packType = unpacker.peekNextType();

        assertThat(packType, equalTo(PackType.LIST));
        assertThat(unpacker.unpackListHeader(), equalTo(3L));
        assertThat(unpacker.unpackString(), equalTo("eins"));
        assertThat(unpacker.unpackString(), equalTo("zwei"));
        assertThat(unpacker.unpackString(), equalTo("drei"));
    }

    @Test
    void testCanPackAndUnpackListOfSpecialStrings() throws Throwable {
        assertPackStringLists(3);
        assertPackStringLists(126);
        assertPackStringLists(3000);
        assertPackStringLists(32768);
    }

    @Test
    void testCanPackAndUnpackListOfStringOneByOne() throws Throwable {
        // Given
        var machine = new Machine();

        // When
        var packer = machine.packer();
        packer.packListHeader(3);
        packer.pack("eins");
        packer.pack("zwei");
        packer.pack("drei");

        // Then
        var unpacker = newUnpacker(machine.output());
        var packType = unpacker.peekNextType();

        assertThat(packType, equalTo(PackType.LIST));
        assertThat(unpacker.unpackListHeader(), equalTo(3L));
        assertThat(unpacker.unpackString(), equalTo("eins"));
        assertThat(unpacker.unpackString(), equalTo("zwei"));
        assertThat(unpacker.unpackString(), equalTo("drei"));
    }

    @Test
    void testCanPackAndUnpackListOfSpecialStringOneByOne() throws Throwable {
        // Given
        var machine = new Machine();

        // When
        var packer = machine.packer();
        packer.packListHeader(3);
        packer.pack("Mjölnir");
        packer.pack("Mjölnir");
        packer.pack("Mjölnir");

        // Then
        var unpacker = newUnpacker(machine.output());
        var packType = unpacker.peekNextType();

        assertThat(packType, equalTo(PackType.LIST));
        assertThat(unpacker.unpackListHeader(), equalTo(3L));
        assertThat(unpacker.unpackString(), equalTo("Mjölnir"));
        assertThat(unpacker.unpackString(), equalTo("Mjölnir"));
        assertThat(unpacker.unpackString(), equalTo("Mjölnir"));
    }

    @Test
    void testCanPackAndUnpackMap() throws Throwable {
        assertMap(2);
        assertMap(126);
        assertMap(2439);
        assertMap(32768);
    }

    @Test
    void testCanPackAndUnpackStruct() throws Throwable {
        // Given
        var machine = new Machine();

        // When
        var packer = machine.packer();
        packer.packStructHeader(3, (byte) 'N');
        packer.pack(12);
        packer.pack(asList("Person", "Employee"));
        packer.pack(asMap("name", "Alice", "age", 33));

        // Then
        var unpacker = newUnpacker(machine.output());
        var packType = unpacker.peekNextType();

        assertThat(packType, equalTo(PackType.STRUCT));
        assertThat(unpacker.unpackStructHeader(), equalTo(3L));
        assertThat(unpacker.unpackStructSignature(), equalTo((byte) 'N'));

        assertThat(unpacker.unpackLong(), equalTo(12L));

        assertThat(unpacker.unpackListHeader(), equalTo(2L));
        assertThat(unpacker.unpackString(), equalTo("Person"));
        assertThat(unpacker.unpackString(), equalTo("Employee"));

        assertThat(unpacker.unpackMapHeader(), equalTo(2L));
        assertThat(unpacker.unpackString(), equalTo("name"));
        assertThat(unpacker.unpackString(), equalTo("Alice"));
        assertThat(unpacker.unpackString(), equalTo("age"));
        assertThat(unpacker.unpackLong(), equalTo(33L));
    }

    @Test
    void testCanPackAndUnpackStructsOfDifferentSizes() throws Throwable {
        assertStruct(2);
        assertStruct(126);
        assertStruct(2439);

        // we cannot have 'too many' fields
        assertThrows(PackStream.Overflow.class, () -> assertStruct(65536));
    }

    @Test
    void testCanDoStreamingListUnpacking() throws Throwable {
        // Given
        var machine = new Machine();
        var packer = machine.packer();
        packer.pack(asList(1, 2, 3, asList(4, 5)));

        // When I unpack this value
        var unpacker = newUnpacker(machine.output());

        // Then I can do streaming unpacking
        var size = unpacker.unpackListHeader();
        var a = unpacker.unpackLong();
        var b = unpacker.unpackLong();
        var c = unpacker.unpackLong();

        var innerSize = unpacker.unpackListHeader();
        var d = unpacker.unpackLong();
        var e = unpacker.unpackLong();

        // And all the values should be sane
        assertEquals(4, size);
        assertEquals(2, innerSize);
        assertEquals(1, a);
        assertEquals(2, b);
        assertEquals(3, c);
        assertEquals(4, d);
        assertEquals(5, e);
    }

    @Test
    void testCanDoStreamingStructUnpacking() throws Throwable {
        // Given
        var machine = new Machine();
        var packer = machine.packer();
        packer.packStructHeader(4, (byte) '~');
        packer.pack(1);
        packer.pack(2);
        packer.pack(3);
        packer.pack(asList(4, 5));

        // When I unpack this value
        var unpacker = newUnpacker(machine.output());

        // Then I can do streaming unpacking
        var size = unpacker.unpackStructHeader();
        var signature = unpacker.unpackStructSignature();
        var a = unpacker.unpackLong();
        var b = unpacker.unpackLong();
        var c = unpacker.unpackLong();

        var innerSize = unpacker.unpackListHeader();
        var d = unpacker.unpackLong();
        var e = unpacker.unpackLong();

        // And all the values should be sane
        assertEquals(4, size);
        assertEquals('~', signature);
        assertEquals(2, innerSize);
        assertEquals(1, a);
        assertEquals(2, b);
        assertEquals(3, c);
        assertEquals(4, d);
        assertEquals(5, e);
    }

    @Test
    void testCanDoStreamingMapUnpacking() throws Throwable {
        // Given
        var machine = new Machine();
        var packer = machine.packer();
        packer.packMapHeader(2);
        packer.pack("name");
        packer.pack("Bob");
        packer.pack("cat_ages");
        packer.pack(asList(4.3, true));

        // When I unpack this value
        var unpacker = newUnpacker(machine.output());

        // Then I can do streaming unpacking
        var size = unpacker.unpackMapHeader();
        var k1 = unpacker.unpackString();
        var v1 = unpacker.unpackString();
        var k2 = unpacker.unpackString();

        var innerSize = unpacker.unpackListHeader();
        var d = unpacker.unpackDouble();
        var e = unpacker.unpackBoolean();

        // And all the values should be sane
        assertEquals(2, size);
        assertEquals(2, innerSize);
        assertEquals("name", k1);
        assertEquals("Bob", v1);
        assertEquals("cat_ages", k2);
        assertEquals(4.3, d, 0.0001);
        assertTrue(e);
    }

    @Test
    void handlesDataCrossingBufferBoundaries() throws Throwable {
        // Given
        var machine = new Machine();
        var packer = machine.packer();
        packer.pack(Long.MAX_VALUE);
        packer.pack(Long.MAX_VALUE);

        var ch = Channels.newChannel(new ByteArrayInputStream(machine.output()));
        var unpacker = new PackStream.Unpacker(new BufferedChannelInput(11, ch));

        // Serialized ch will look like, and misalign with the 11-byte unpack buffer:

        // [XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX]
        //  mkr \___________data______________/ mkr \___________data______________/
        // \____________unpack buffer_________________/

        // When
        var first = unpacker.unpackLong();
        var second = unpacker.unpackLong();

        // Then
        assertEquals(Long.MAX_VALUE, first);
        assertEquals(Long.MAX_VALUE, second);
    }

    @Test
    void testCanPeekOnNextType() throws Throwable {
        // When & Then
        assertPeekType(PackType.STRING, "a string");
        assertPeekType(PackType.INTEGER, 123);
        assertPeekType(PackType.FLOAT, 123.123);
        assertPeekType(PackType.BOOLEAN, true);
        assertPeekType(PackType.LIST, asList(1, 2, 3));
        assertPeekType(PackType.MAP, asMap("l", 3));
    }

    @Test
    void shouldFailForUnknownValue() {
        // Given
        var machine = new Machine();
        var packer = machine.packer();

        // Expect
        assertThrows(PackStream.UnPackable.class, () -> packer.pack(new MyRandomClass()));
    }

    private static class MyRandomClass {}

    private void assertPeekType(PackType type, Object value) throws IOException {
        // Given
        var machine = new Machine();
        var packer = machine.packer();
        packer.pack(value);

        var unpacker = newUnpacker(machine.output());

        // When & Then
        assertEquals(type, unpacker.peekNextType());
    }

    private void assertPackStringLists(int size) throws Throwable {
        // Given
        var machine = new Machine();

        // When
        var packer = machine.packer();
        var strings = new ArrayList<String>(size);
        for (var i = 0; i < size; i++) {
            strings.add(i, "Mjölnir");
        }
        packer.pack(strings);

        // Then
        var unpacker = newUnpacker(machine.output());
        var packType = unpacker.peekNextType();
        assertThat(packType, equalTo(PackType.LIST));

        assertThat(unpacker.unpackListHeader(), equalTo((long) size));
        for (var i = 0; i < size; i++) {
            assertThat(unpacker.unpackString(), equalTo("Mjölnir"));
        }
    }

    private void assertMap(int size) throws Throwable {
        // Given
        var machine = new Machine();

        // When
        var packer = machine.packer();
        var map = IntStream.range(0, size)
                .boxed()
                .collect(Collectors.toMap(i -> Integer.toString(i), Function.identity(), (a, b) -> b, HashMap::new));
        packer.pack(map);

        // Then
        var unpacker = newUnpacker(machine.output());
        var packType = unpacker.peekNextType();

        assertThat(packType, equalTo(PackType.MAP));

        assertThat(unpacker.unpackMapHeader(), equalTo((long) size));

        for (var i = 0; i < size; i++) {
            assertThat(unpacker.unpackString(), equalTo(Long.toString(unpacker.unpackLong())));
        }
    }

    private void assertStruct(int size) throws Throwable {
        // Given
        var machine = new Machine();

        // When
        var packer = machine.packer();
        packer.packStructHeader(size, (byte) 'N');
        for (var i = 0; i < size; i++) {
            packer.pack(i);
        }

        // Then
        var unpacker = newUnpacker(machine.output());
        var packType = unpacker.peekNextType();

        assertThat(packType, equalTo(PackType.STRUCT));
        assertThat(unpacker.unpackStructHeader(), equalTo((long) size));
        assertThat(unpacker.unpackStructSignature(), equalTo((byte) 'N'));

        for (var i = 0; i < size; i++) {
            assertThat(unpacker.unpackLong(), equalTo((long) i));
        }
    }
}
