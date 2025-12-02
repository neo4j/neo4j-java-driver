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
package org.neo4j.driver.internal.property_encryption;

import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import org.neo4j.driver.Logger;

abstract class AbstractFactoryLoader<T> {
    @SuppressWarnings("deprecation")
    private final Logger logger;

    private final T providerFactory;
    private final String defaultFactoryName;
    private final Class<T> factoryType;

    AbstractFactoryLoader(
            @SuppressWarnings("deprecation") Logger logger, String defaultFactoryName, Class<T> factoryType) {
        this.logger = Objects.requireNonNull(logger);
        this.defaultFactoryName = Objects.requireNonNull(defaultFactoryName);
        this.factoryType = Objects.requireNonNull(factoryType);
        this.providerFactory = findFactory().orElseThrow(() -> new IllegalStateException("No factory found"));
    }

    T factory() {
        return providerFactory;
    }

    Optional<T> findFactory() {
        var result = Optional.<T>empty();
        try {
            var serviceLoader = ServiceLoader.load(factoryType, this.getClass().getClassLoader());
            result = serviceLoader.stream().map(ServiceLoader.Provider::get).findFirst();
        } catch (Exception e) {
            logger.warn("Loading of BoltConnectionProviderFactory service has failed", e);
        }
        if (result.isEmpty()) {
            try {
                // an extra attempt in case the factory is visible
                @SuppressWarnings("Java9ReflectionClassVisibility")
                var factoryCls = Class.forName(defaultFactoryName);
                if (factoryType.isAssignableFrom(factoryCls)) {
                    @SuppressWarnings("unchecked")
                    var factory = (T) factoryCls.getConstructor().newInstance();
                    result = Optional.of(factory);
                }
            } catch (Exception e) {
                logger.error("Failed to load default '%s' factory".formatted(defaultFactoryName), e);
            }
        }
        result.ifPresentOrElse(
                factory -> logger.trace("Selected '%s' factory", factory.getClass()),
                () -> logger.warn("No factory has been found"));
        return result;
    }
}
