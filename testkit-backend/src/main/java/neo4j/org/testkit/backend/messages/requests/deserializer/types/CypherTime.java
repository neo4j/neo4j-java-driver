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

import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.LocalTimeValue;
import org.neo4j.driver.internal.value.TimeValue;

public class CypherTime implements CypherType {
    private final int hour, minute, second, nano;
    private final Integer offset;

    public CypherTime(int hour, int minute, int second, int nanosecond, Integer offset) {
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.nano = nanosecond;
        this.offset = offset;
    }

    @Override
    public Value asValue() {
        if (offset != null) {
            return new TimeValue(OffsetTime.of(hour, minute, second, nano, ZoneOffset.ofTotalSeconds(offset)));
        }
        return new LocalTimeValue(LocalTime.of(hour, minute, second, nano));
    }
}
