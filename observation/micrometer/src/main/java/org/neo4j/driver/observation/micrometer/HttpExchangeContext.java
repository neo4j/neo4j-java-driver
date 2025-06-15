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
package org.neo4j.driver.observation.micrometer;

import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.SenderContext;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

public class HttpExchangeContext extends SenderContext<Object> {
    private final URI uri;
    private final String method;
    private final String uriTemplate;
    private volatile Map<String, List<String>> headers;
    private volatile Response response;
    private volatile String errorType;

    public HttpExchangeContext(URI uri, String method, String uriTemplate, BiConsumer<String, String> setter) {
        super((carrier, key, value) -> setter.accept(key, value), Kind.CLIENT);
        this.uri = Objects.requireNonNull(uri);
        this.method = Objects.requireNonNull(method);
        this.uriTemplate = Objects.requireNonNull(uriTemplate);
    }

    public URI uri() {
        return uri;
    }

    public String method() {
        return method;
    }

    public String uriTemplate() {
        return uriTemplate;
    }

    public void setHeaders(Map<String, List<String>> headers) {
        this.headers = headers;
    }

    public Optional<Map<String, List<String>>> headers() {
        return Optional.ofNullable(headers);
    }

    public void setResponse(Response response) {
        this.response = response;
    }

    public Optional<Response> response() {
        return Optional.ofNullable(response);
    }

    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }

    public Optional<String> errorType() {
        return Optional.ofNullable(errorType);
    }

    public interface Response {
        int statusCode();

        Map<String, List<String>> headers();

        String httpVersion();
    }
}
