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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import neo4j.org.testkit.backend.channel.handler.TestkitRequestResponseMapperHandler;
import neo4j.org.testkit.backend.messages.requests.NewDriver;
import neo4j.org.testkit.backend.messages.requests.NewSession;
import neo4j.org.testkit.backend.messages.requests.SessionRun;
import neo4j.org.testkit.backend.messages.requests.TestkitRequest;
import org.junit.jupiter.api.Test;

class MessageDeserializerTest {
    private static final ObjectMapper mapper = TestkitRequestResponseMapperHandler.newObjectMapper();

    @Test
    void testDeserializeNewDriver() throws JsonProcessingException {
        Object message = mapper.readValue(
                "{\"name\": \"NewDriver\", \"data\": {\"uri\": \"bolt://localhost:7687\", "
                        + "\"authorizationToken\": {\"name\": \"AuthorizationToken\", \"data\": {\"scheme\": \"basic\", \"principal\": \"neo4j\", "
                        + "\"credentials\": \"pass\", \"realm\": \"\", \"ticket\": \"\"}}, \"userAgent\": null}}",
                TestkitRequest.class);

        assertTrue(message instanceof NewDriver);

        var newDriver = (NewDriver) message;
        assertEquals("bolt://localhost:7687", newDriver.getData().getUri());
        assertEquals(
                "basic", newDriver.getData().getAuthorizationToken().getTokens().getScheme());
        assertEquals(
                "neo4j", newDriver.getData().getAuthorizationToken().getTokens().getPrincipal());
    }

    @Test
    void testDeserializerNewSession() throws JsonProcessingException {
        Object message = mapper.readValue(
                "{\"name\": \"NewSession\", "
                        + "\"data\": {\"driverId\": \"0\", \"accessMode\": \"w\", \"bookmarks\": null, \"database\": null, \"fetchSize\": null}}",
                TestkitRequest.class);

        assertTrue(message instanceof NewSession);

        var sessionRequest = (NewSession) message;
        assertEquals("0", sessionRequest.getData().getDriverId());
        assertEquals("w", sessionRequest.getData().getAccessMode());
    }

    @Test
    void testDeserializerNewSessionRun() throws JsonProcessingException {
        Object message = mapper.readValue(
                "{\"name\": \"SessionRun\", \"data\": {\"sessionId\": \"1\", \"cypher\": \"RETURN $x as y\", "
                        + "\"params\": {\"x\": {\"name\": \"CypherBool\", \"data\": {\"value\": true}}}, \"txMeta\": null, \"timeout\": null}}",
                TestkitRequest.class);

        assertTrue(message instanceof SessionRun);

        var sessionRun = (SessionRun) message;
        assertEquals("1", sessionRun.getData().getSessionId());
        assertEquals("RETURN $x as y", sessionRun.getData().getCypher());
    }
}
