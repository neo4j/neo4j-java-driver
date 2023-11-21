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

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Consumer;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

public class ResponseQueueHanlder {
    private final Consumer<TestkitResponse> responseWriter;
    private final Queue<TestkitResponse> responseQueue = new ArrayDeque<>();
    private int requestCount;

    ResponseQueueHanlder(Consumer<TestkitResponse> responseWriter) {
        this.responseWriter = responseWriter;
    }

    public synchronized void increaseRequestCountAndDispatchFirstResponse() {
        requestCount++;
        dispatchFirstResponse();
    }

    public synchronized void offerAndDispatchFirstResponse(TestkitResponse response) {
        responseQueue.offer(response);
        if (requestCount > 0) {
            dispatchFirstResponse();
        }
    }

    private synchronized void dispatchFirstResponse() {
        var response = responseQueue.poll();
        if (response != null) {
            requestCount--;
            responseWriter.accept(response);
        }
    }
}
