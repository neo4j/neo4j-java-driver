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
package neo4j.org.testkit.backend;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import neo4j.org.testkit.backend.channel.handler.TestkitRequestResponseMapperHandler;
import neo4j.org.testkit.backend.messages.responses.Driver;
import org.junit.jupiter.api.Test;

public class MessageSerializerTest {
    private static final ObjectMapper mapper = TestkitRequestResponseMapperHandler.newObjectMapper();

    @Test
    void shouldSerializerNewDriverResponse() throws JsonProcessingException {
        var response = Driver.builder()
                .data(Driver.DriverBody.builder().id("1").build())
                .build();
        var expectedOutput = "{\"name\":\"Driver\",\"data\":{\"id\":\"1\"}}";

        var serializedResponse = mapper.writeValueAsString(response);
        assertEquals(expectedOutput, serializedResponse);
    }
}
