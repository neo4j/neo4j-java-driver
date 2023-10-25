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
package org.neo4j.driver.internal.blockhound;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Method;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;

class Neo4jDriverBlockHoundIntegrationTest {
    @Test
    void shouldUseExistingMethodsInAllowBlockingCallsInside() {
        var builder = mock(BlockHound.Builder.class);
        given(builder.allowBlockingCallsInside(any(), any())).willAnswer(invocationOnMock -> {
            var clsName = (String) invocationOnMock.getArgument(0);
            var methodName = (String) invocationOnMock.getArgument(1);
            var cls = Class.forName(clsName);
            Arrays.stream(cls.getDeclaredMethods())
                    .map(Method::getName)
                    .filter(name -> name.equals(methodName))
                    .findAny()
                    .ifPresentOrElse(
                            ignored -> {},
                            () -> fail(String.format("%s.%s method has not been found", clsName, methodName)));
            return builder;
        });
        var integration = new Neo4jDriverBlockHoundIntegration();

        integration.applyTo(builder);
    }
}
