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
package neo4j.org.testkit.backend.messages.requests.deserializer.types;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.DateTimeValue;
import org.neo4j.driver.internal.value.LocalDateTimeValue;

public class CypherDateTime implements CypherType {
    private final int year, month, day, hour, minute, second, nano;
    private final String zoneId;
    private final Integer offset;

    public CypherDateTime(
            int year,
            int month,
            int day,
            int hour,
            int minute,
            int second,
            int nanosecond,
            String zoneId,
            Integer offset) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.nano = nanosecond;
        this.zoneId = zoneId;
        this.offset = offset;
    }

    @Override
    public Value asValue() {
        if (zoneId != null) {
            var dateTime = getZonedDateTime();
            return new DateTimeValue(dateTime);
        }

        if (offset != null) {
            var zoneOffset = ZoneOffset.ofTotalSeconds(offset);
            return new DateTimeValue(ZonedDateTime.of(year, month, day, hour, minute, second, nano, zoneOffset));
        }
        return new LocalDateTimeValue(LocalDateTime.of(year, month, day, hour, minute, second, nano));
    }

    private ZonedDateTime getZonedDateTime() {
        var dateTime = ZonedDateTime.of(year, month, day, hour, minute, second, nano, ZoneId.of(zoneId));
        if (dateTime.getOffset().getTotalSeconds() != offset) {
            throw new RuntimeException(String.format(
                    "TestKit's and driver's tz info diverge. "
                            + "TestKit assumes %ds offset for %s while the driver assumes %ds for %s.",
                    offset, zoneId, dateTime.getOffset().getTotalSeconds(), dateTime));
        }
        return dateTime;
    }
}
