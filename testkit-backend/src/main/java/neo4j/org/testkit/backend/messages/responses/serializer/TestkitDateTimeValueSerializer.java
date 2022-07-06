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
package neo4j.org.testkit.backend.messages.responses.serializer;

import static neo4j.org.testkit.backend.messages.responses.serializer.GenUtils.cypherObject;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.neo4j.driver.internal.value.DateTimeValue;

public class TestkitDateTimeValueSerializer extends StdSerializer<DateTimeValue> {
    public TestkitDateTimeValueSerializer() {
        super(DateTimeValue.class);
    }

    @Override
    public void serialize(DateTimeValue timeValue, JsonGenerator gen, SerializerProvider provider) throws IOException {
        cypherObject(gen, "CypherDateTime", () -> {
            ZonedDateTime dateTime = timeValue.asZonedDateTime();
            GenUtils.writeDate(gen, dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth());
            GenUtils.writeTime(gen, dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond(), dateTime.getNano());
            ZoneOffset offset = dateTime.getOffset();
            gen.writeFieldName("utc_offset_s");
            gen.writeNumber(offset.getTotalSeconds());
            ZoneId zoneId = dateTime.getZone();
            if (zoneId != offset) {
                // not fixed offset
                gen.writeFieldName("timezone_id");
                gen.writeString(zoneId.toString());
            }
        });
    }
}
