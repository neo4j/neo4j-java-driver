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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

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

        assertThat(message, instanceOf(NewDriver.class));

        var newDriver = (NewDriver) message;
        assertThat(newDriver.getData().getUri(), equalTo("bolt://localhost:7687"));
        assertThat(newDriver.getData().getAuthorizationToken().getTokens().getScheme(), equalTo("basic"));
        assertThat(newDriver.getData().getAuthorizationToken().getTokens().getPrincipal(), equalTo("neo4j"));
    }

    @Test
    void testDeserializerNewSession() throws JsonProcessingException {
        Object message = mapper.readValue(
                "{\"name\": \"NewSession\", "
                        + "\"data\": {\"driverId\": \"0\", \"accessMode\": \"w\", \"bookmarks\": null, \"database\": null, \"fetchSize\": null}}",
                TestkitRequest.class);

        assertThat(message, instanceOf(NewSession.class));

        var sessionRequest = (NewSession) message;
        assertThat(sessionRequest.getData().getDriverId(), equalTo("0"));
        assertThat(sessionRequest.getData().getAccessMode(), equalTo("w"));
    }

    @Test
    void testDeserializerNewSessionRun() throws JsonProcessingException {
        Object message = mapper.readValue(
                "{\"name\": \"SessionRun\", \"data\": {\"sessionId\": \"1\", \"cypher\": \"RETURN $x as y\", "
                        + "\"params\": {\"x\": {\"name\": \"CypherBool\", \"data\": {\"value\": true}}}, \"txMeta\": null, \"timeout\": null}}",
                TestkitRequest.class);

        assertThat(message, instanceOf(SessionRun.class));

        var sessionRun = (SessionRun) message;
        assertThat(sessionRun.getData().getSessionId(), equalTo("1"));
        assertThat(sessionRun.getData().getCypher(), equalTo("RETURN $x as y"));
    }
}
