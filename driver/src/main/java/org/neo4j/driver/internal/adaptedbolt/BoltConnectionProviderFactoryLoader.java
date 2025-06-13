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
package org.neo4j.driver.internal.adaptedbolt;

import java.net.URI;
import java.util.Comparator;
import java.util.Optional;
import java.util.ServiceLoader;
import org.neo4j.bolt.connection.BoltConnectionProviderFactory;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;

public final class BoltConnectionProviderFactoryLoader {
    private static final String DEFAULT_SCHEMES_FACTORY_NAME =
            "org.neo4j.bolt.connection.netty.NettyBoltConnectionProviderFactory";

    @SuppressWarnings("deprecation")
    private final Logger logger;

    private final String scheme;
    private final BoltConnectionProviderFactory providerFactory;

    public BoltConnectionProviderFactoryLoader(@SuppressWarnings("deprecation") Logging logging, URI uri) {
        this.logger = logging.getLog(getClass());

        scheme = uri.getScheme();
        if (scheme == null) {
            throw new IllegalArgumentException("Scheme must not be null");
        }
        this.providerFactory = findProvider(scheme).orElse(null);
    }

    public String scheme() {
        return scheme;
    }

    public Optional<BoltConnectionProviderFactory> providerFactory() {
        return Optional.ofNullable(providerFactory);
    }

    private Optional<BoltConnectionProviderFactory> findProvider(String scheme) {
        var result = Optional.<BoltConnectionProviderFactory>empty();
        try {
            var serviceLoader = ServiceLoader.load(
                    BoltConnectionProviderFactory.class, this.getClass().getClassLoader());
            result = serviceLoader.stream()
                    .map(ServiceLoader.Provider::get)
                    .filter(factory -> {
                        var supportsScheme = factory.supports(scheme);
                        if (supportsScheme) {
                            logger.trace(
                                    "Loaded '%s' provider, it supports '%s' scheme and has '%d' order value",
                                    factory.getClass(), scheme, factory.getOrder());
                        } else {
                            logger.trace(
                                    "Loaded '%s' provider, it does not support '%s' scheme",
                                    factory.getClass(), scheme, factory.getOrder());
                        }
                        return supportsScheme;
                    })
                    .min(Comparator.comparing(BoltConnectionProviderFactory::getOrder));
        } catch (Exception e) {
            logger.warn("Loading of BoltConnectionProviderFactory service has failed", e);
        }
        if (result.isEmpty()) {
            try {
                // an extra attempt in case the factory is visible
                @SuppressWarnings("Java9ReflectionClassVisibility")
                var factoryCls = Class.forName(DEFAULT_SCHEMES_FACTORY_NAME);
                if (BoltConnectionProviderFactory.class.isAssignableFrom(factoryCls)) {
                    var factory = (BoltConnectionProviderFactory)
                            factoryCls.getConstructor().newInstance();
                    if (factory.supports(scheme)) {
                        result = Optional.of(factory);
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to load default '%s' factory".formatted(DEFAULT_SCHEMES_FACTORY_NAME), e);
            }
        }
        result.ifPresentOrElse(
                factory -> logger.trace("Selected '%s' factory", factory.getClass()),
                () -> logger.warn("No factory has been found"));
        return result;
    }
}
