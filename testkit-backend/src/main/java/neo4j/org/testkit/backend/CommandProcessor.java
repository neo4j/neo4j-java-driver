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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import neo4j.org.testkit.backend.messages.TestkitModule;

import java.util.Collections;

public interface CommandProcessor
{
    /**
     * Used in ObjectMapper's injectable values.
     */
    String COMMAND_PROCESSOR_ID = "commandProcessor";

    /**
     * Reads one request and writes the response. Returns false when not able to read anymore.
     *
     * @return False when there's nothing to read anymore.
     */
    boolean process();

    /**
     * Create a new {@link ObjectMapper} configured with the appropriate testkit module and an injectable {@link CommandProcessor}.
     * @param processor The processor supposed to be injectable
     * @return A reusable object mapper instance
     */
    static ObjectMapper newObjectMapperFor(CommandProcessor processor) {

        final ObjectMapper objectMapper = new ObjectMapper();

        TestkitModule testkitModule = new TestkitModule();
        objectMapper.registerModule( testkitModule );
        objectMapper.disable( DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES );

        InjectableValues injectableValues = new InjectableValues.Std( Collections.singletonMap( COMMAND_PROCESSOR_ID, processor ) );
        objectMapper.setInjectableValues( injectableValues );
        return objectMapper;
    }
}
