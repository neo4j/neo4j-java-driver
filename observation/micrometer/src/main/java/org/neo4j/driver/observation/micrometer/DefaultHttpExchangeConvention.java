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

import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultHttpExchangeConvention implements HttpExchangeConvention {
    private static final KeyValue DB_SYSTEM_NAME =
            Neo4jDriverDocumentation.HttpExchangeLowCardinalityKeyNames.DB_SYSTEM_NAME.withValue(
                    KeyValuesUtil.DB_SYSTEM_NAME);
    private static final KeyValue NETWORK_PROTOCOL_NAME =
            Neo4jDriverDocumentation.HttpExchangeLowCardinalityKeyNames.NETWORK_PROTOCOL_NAME.withValue("http");

    private final boolean includeUrlScheme;
    private final boolean includeUrlTemplate;
    private final Predicate<String> requestHeaderPredicate;
    private final Predicate<String> responseHeaderPredicate;

    public DefaultHttpExchangeConvention(
            boolean includeUrlScheme,
            boolean includeUrlTemplate,
            Predicate<String> requestHeaderPredicate,
            Predicate<String> responseHeaderPredicate) {
        this.includeUrlScheme = includeUrlScheme;
        this.includeUrlTemplate = includeUrlTemplate;
        this.requestHeaderPredicate = requestHeaderPredicate;
        this.responseHeaderPredicate = responseHeaderPredicate;
    }

    @Override
    public String getName() {
        return "http.client.request.duration";
    }

    @Override
    public String getContextualName(HttpExchangeContext context) {
        return includeUrlTemplate ? "%s %s".formatted(context.method(), context.uriTemplate()) : context.method();
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(HttpExchangeContext context) {
        return requiredLowCardinalityKeyValues(context).and(extraLowCardinalityKeyValues(context));
    }

    @Override
    public KeyValues getHighCardinalityKeyValues(HttpExchangeContext context) {
        return requiredHighCardinalityKeyValues(context).and(extraHighCardinalityKeyValues(context));
    }

    private KeyValues requiredLowCardinalityKeyValues(HttpExchangeContext context) {
        return KeyValues.of(
                DB_SYSTEM_NAME,
                method(context),
                serverAddress(context),
                serverPort(context),
                NETWORK_PROTOCOL_NAME,
                errorType(context));
    }

    private KeyValues extraLowCardinalityKeyValues(HttpExchangeContext context) {
        var list = new ArrayList<KeyValue>();
        if (includeUrlScheme) {
            list.add(scheme(context));
        }
        if (includeUrlTemplate) {
            list.add(urlTemplate(context));
        }
        var response = context.response().orElse(null);
        list.add(protocolVersion(response));
        list.add(responseStatus(response));
        return KeyValues.of(list);
    }

    private KeyValues requiredHighCardinalityKeyValues(HttpExchangeContext context) {
        return KeyValues.of(urlFull(context));
    }

    private KeyValues extraHighCardinalityKeyValues(HttpExchangeContext context) {
        Stream<KeyValue> requestHeaders = requestHeaderPredicate != null
                ? context.headers()
                        .map(headers -> headers(
                                Neo4jDriverDocumentation.HttpExchangeHighCardinalityKeyNames.HTTP_REQUEST_HEADER_FORMAT
                                        .asString(),
                                requestHeaderPredicate,
                                headers))
                        .orElseGet(Stream::empty)
                : Stream.empty();
        Stream<KeyValue> responseHeaders = responseHeaderPredicate != null
                ? context.response()
                        .map(response -> headers(
                                Neo4jDriverDocumentation.HttpExchangeHighCardinalityKeyNames.HTTP_RESPONSE_HEADER_FORMAT
                                        .asString(),
                                responseHeaderPredicate,
                                response.headers()))
                        .orElseGet(Stream::empty)
                : Stream.empty();
        return KeyValues.of(Stream.concat(requestHeaders, responseHeaders).toList());
    }

    private Stream<KeyValue> headers(String format, Predicate<String> predicate, Map<String, List<String>> headers) {
        return headers.entrySet().stream()
                .filter(entry -> predicate.test(entry.getKey()))
                .map(entry -> header(format, entry.getKey(), entry.getValue()));
    }

    private KeyValue header(String format, String name, List<String> values) {
        var keyName = format.replace("<key>", name.toLowerCase());
        var value = values.stream().map("\"%s\""::formatted).collect(Collectors.joining(", ", "[", "]"));
        return KeyValue.of(keyName, value);
    }

    private KeyValue method(HttpExchangeContext context) {
        return Neo4jDriverDocumentation.HttpExchangeLowCardinalityKeyNames.HTTP_REQUEST_METHOD.withValue(
                context.method());
    }

    private KeyValue serverAddress(HttpExchangeContext context) {
        return Neo4jDriverDocumentation.HttpExchangeLowCardinalityKeyNames.SERVER_ADDRESS.withValue(
                context.uri().getHost());
    }

    private KeyValue serverPort(HttpExchangeContext context) {
        var port = context.uri().getPort();
        return Neo4jDriverDocumentation.HttpExchangeLowCardinalityKeyNames.SERVER_PORT.withValue(
                port != -1
                        ? String.valueOf(port)
                        : switch (context.uri().getScheme()) {
                            case "http" -> "80";
                            case "https" -> "443";
                            default -> KeyValue.NONE_VALUE;
                        });
    }

    private KeyValue scheme(HttpExchangeContext context) {
        return Neo4jDriverDocumentation.HttpExchangeLowCardinalityKeyNames.URL_SCHEME.withValue(
                context.uri().getScheme());
    }

    private KeyValue protocolVersion(HttpExchangeContext.Response response) {
        return Neo4jDriverDocumentation.HttpExchangeLowCardinalityKeyNames.NETWORK_PROTOCOL_VERSION.withValue(
                response != null ? response.httpVersion() : KeyValue.NONE_VALUE);
    }

    private KeyValue urlTemplate(HttpExchangeContext context) {
        return Neo4jDriverDocumentation.HttpExchangeLowCardinalityKeyNames.URL_TEMPLATE.withValue(
                context.uriTemplate());
    }

    private KeyValue responseStatus(HttpExchangeContext.Response response) {
        return Neo4jDriverDocumentation.HttpExchangeLowCardinalityKeyNames.HTTP_RESPONSE_STATUS_CODE.withValue(
                response != null ? String.valueOf(response.statusCode()) : KeyValue.NONE_VALUE);
    }

    private KeyValue urlFull(HttpExchangeContext context) {
        return Neo4jDriverDocumentation.HttpExchangeHighCardinalityKeyNames.URL_FULL.withValue(
                context.uri().toString());
    }

    private KeyValue errorType(HttpExchangeContext context) {
        return Neo4jDriverDocumentation.HttpExchangeLowCardinalityKeyNames.ERROR_TYPE.withValue(
                context.errorType().orElse(KeyValue.NONE_VALUE));
    }
}
