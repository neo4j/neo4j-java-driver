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
package neo4j.org.testkit.backend;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import neo4j.org.testkit.backend.messages.TestkitModule;
import neo4j.org.testkit.backend.messages.responses.Driver;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class MessageSerializerTest
{
    private static final ObjectMapper mapper = new ObjectMapper();

    @BeforeAll
    static void setUp()
    {
        TestkitModule tkm = new TestkitModule();

        mapper.registerModule( tkm );
        mapper.disable( DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES );
    }

    @Test
    void shouldSerializerNewDriverResponse() throws JsonProcessingException
    {
        Driver response = Driver.builder().data( Driver.DriverBody.builder().id( "1" ).build() ).build();
        String expectedOutput = "{\"name\":\"Driver\",\"data\":{\"id\":\"1\"}}";

        String serializedResponse = mapper.writeValueAsString( response );

        assertThat( serializedResponse, equalTo( expectedOutput ) );
    }
}
