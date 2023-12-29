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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.common;

import static java.time.ZoneOffset.UTC;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalPoint2D;
import org.neo4j.driver.internal.InternalPoint3D;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.ValuePacker;
import org.neo4j.driver.internal.bolt.basicimpl.packstream.PackOutput;
import org.neo4j.driver.internal.bolt.basicimpl.packstream.PackStream;
import org.neo4j.driver.internal.value.InternalValue;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.Point;

public class CommonValuePacker implements ValuePacker {

    public static final byte DATE = 'D';
    public static final int DATE_STRUCT_SIZE = 1;

    public static final byte TIME = 'T';
    public static final int TIME_STRUCT_SIZE = 2;

    public static final byte LOCAL_TIME = 't';
    public static final int LOCAL_TIME_STRUCT_SIZE = 1;

    public static final byte LOCAL_DATE_TIME = 'd';
    public static final int LOCAL_DATE_TIME_STRUCT_SIZE = 2;

    public static final byte DATE_TIME_WITH_ZONE_OFFSET = 'F';
    public static final byte DATE_TIME_WITH_ZONE_OFFSET_UTC = 'I';
    public static final byte DATE_TIME_WITH_ZONE_ID = 'f';
    public static final byte DATE_TIME_WITH_ZONE_ID_UTC = 'i';
    public static final int DATE_TIME_STRUCT_SIZE = 3;

    public static final byte DURATION = 'E';
    public static final int DURATION_TIME_STRUCT_SIZE = 4;

    public static final byte POINT_2D_STRUCT_TYPE = 'X';
    public static final int POINT_2D_STRUCT_SIZE = 3;

    public static final byte POINT_3D_STRUCT_TYPE = 'Y';
    public static final int POINT_3D_STRUCT_SIZE = 4;

    private final boolean dateTimeUtcEnabled;
    protected final PackStream.Packer packer;

    public CommonValuePacker(PackOutput output, boolean dateTimeUtcEnabled) {
        this.dateTimeUtcEnabled = dateTimeUtcEnabled;
        this.packer = new PackStream.Packer(output);
    }

    @Override
    public final void packStructHeader(int size, byte signature) throws IOException {
        packer.packStructHeader(size, signature);
    }

    @Override
    public final void pack(String string) throws IOException {
        packer.pack(string);
    }

    @Override
    public final void pack(Value value) throws IOException {
        if (value instanceof InternalValue) {
            packInternalValue(((InternalValue) value));
        } else {
            throw new IllegalArgumentException("Unable to pack: " + value);
        }
    }

    @Override
    public final void pack(Map<String, Value> map) throws IOException {
        if (map == null || map.isEmpty()) {
            packer.packMapHeader(0);
            return;
        }
        packer.packMapHeader(map.size());
        for (var entry : map.entrySet()) {
            packer.pack(entry.getKey());
            pack(entry.getValue());
        }
    }

    protected void packInternalValue(InternalValue value) throws IOException {
        switch (value.typeConstructor()) {
            case DATE -> packDate(value.asLocalDate());
            case TIME -> packTime(value.asOffsetTime());
            case LOCAL_TIME -> packLocalTime(value.asLocalTime());
            case LOCAL_DATE_TIME -> packLocalDateTime(value.asLocalDateTime());
            case DATE_TIME -> {
                if (dateTimeUtcEnabled) {
                    packZonedDateTimeUsingUtcBaseline(value.asZonedDateTime());
                } else {
                    packZonedDateTime(value.asZonedDateTime());
                }
            }
            case DURATION -> packDuration(value.asIsoDuration());
            case POINT -> packPoint(value.asPoint());
            case NULL -> packer.packNull();
            case BYTES -> packer.pack(value.asByteArray());
            case STRING -> packer.pack(value.asString());
            case BOOLEAN -> packer.pack(value.asBoolean());
            case INTEGER -> packer.pack(value.asLong());
            case FLOAT -> packer.pack(value.asDouble());
            case MAP -> {
                packer.packMapHeader(value.size());
                for (var s : value.keys()) {
                    packer.pack(s);
                    pack(value.get(s));
                }
            }
            case LIST -> {
                packer.packListHeader(value.size());
                for (var item : value.values()) {
                    pack(item);
                }
            }
            default -> throw new IOException("Unknown type: " + value.type().name());
        }
    }

    private void packDate(LocalDate localDate) throws IOException {
        packer.packStructHeader(DATE_STRUCT_SIZE, DATE);
        packer.pack(localDate.toEpochDay());
    }

    private void packTime(OffsetTime offsetTime) throws IOException {
        var nanoOfDayLocal = offsetTime.toLocalTime().toNanoOfDay();
        var offsetSeconds = offsetTime.getOffset().getTotalSeconds();

        packer.packStructHeader(TIME_STRUCT_SIZE, TIME);
        packer.pack(nanoOfDayLocal);
        packer.pack(offsetSeconds);
    }

    private void packLocalTime(LocalTime localTime) throws IOException {
        packer.packStructHeader(LOCAL_TIME_STRUCT_SIZE, LOCAL_TIME);
        packer.pack(localTime.toNanoOfDay());
    }

    private void packLocalDateTime(LocalDateTime localDateTime) throws IOException {
        var epochSecondUtc = localDateTime.toEpochSecond(UTC);
        var nano = localDateTime.getNano();

        packer.packStructHeader(LOCAL_DATE_TIME_STRUCT_SIZE, LOCAL_DATE_TIME);
        packer.pack(epochSecondUtc);
        packer.pack(nano);
    }

    @SuppressWarnings("DuplicatedCode")
    private void packZonedDateTimeUsingUtcBaseline(ZonedDateTime zonedDateTime) throws IOException {
        var instant = zonedDateTime.toInstant();
        var epochSecondLocal = instant.getEpochSecond();
        var nano = zonedDateTime.getNano();
        var zone = zonedDateTime.getZone();

        if (zone instanceof ZoneOffset) {
            var offsetSeconds = ((ZoneOffset) zone).getTotalSeconds();

            packer.packStructHeader(DATE_TIME_STRUCT_SIZE, DATE_TIME_WITH_ZONE_OFFSET_UTC);
            packer.pack(epochSecondLocal);
            packer.pack(nano);
            packer.pack(offsetSeconds);
        } else {
            var zoneId = zone.getId();

            packer.packStructHeader(DATE_TIME_STRUCT_SIZE, DATE_TIME_WITH_ZONE_ID_UTC);
            packer.pack(epochSecondLocal);
            packer.pack(nano);
            packer.pack(zoneId);
        }
    }

    @SuppressWarnings("DuplicatedCode")
    private void packZonedDateTime(ZonedDateTime zonedDateTime) throws IOException {
        var epochSecondLocal = zonedDateTime.toLocalDateTime().toEpochSecond(UTC);
        var nano = zonedDateTime.getNano();

        var zone = zonedDateTime.getZone();
        if (zone instanceof ZoneOffset) {
            var offsetSeconds = ((ZoneOffset) zone).getTotalSeconds();

            packer.packStructHeader(DATE_TIME_STRUCT_SIZE, DATE_TIME_WITH_ZONE_OFFSET);
            packer.pack(epochSecondLocal);
            packer.pack(nano);
            packer.pack(offsetSeconds);
        } else {
            var zoneId = zone.getId();

            packer.packStructHeader(DATE_TIME_STRUCT_SIZE, DATE_TIME_WITH_ZONE_ID);
            packer.pack(epochSecondLocal);
            packer.pack(nano);
            packer.pack(zoneId);
        }
    }

    private void packDuration(IsoDuration duration) throws IOException {
        packer.packStructHeader(DURATION_TIME_STRUCT_SIZE, DURATION);
        packer.pack(duration.months());
        packer.pack(duration.days());
        packer.pack(duration.seconds());
        packer.pack(duration.nanoseconds());
    }

    private void packPoint(Point point) throws IOException {
        if (point instanceof InternalPoint2D) {
            packPoint2D(point);
        } else if (point instanceof InternalPoint3D) {
            packPoint3D(point);
        } else {
            throw new IOException(String.format("Unknown type: type: %s, value: %s", point.getClass(), point));
        }
    }

    private void packPoint2D(Point point) throws IOException {
        packer.packStructHeader(POINT_2D_STRUCT_SIZE, POINT_2D_STRUCT_TYPE);
        packer.pack(point.srid());
        packer.pack(point.x());
        packer.pack(point.y());
    }

    private void packPoint3D(Point point) throws IOException {
        packer.packStructHeader(POINT_3D_STRUCT_SIZE, POINT_3D_STRUCT_TYPE);
        packer.pack(point.srid());
        packer.pack(point.x());
        packer.pack(point.y());
        packer.pack(point.z());
    }
}
