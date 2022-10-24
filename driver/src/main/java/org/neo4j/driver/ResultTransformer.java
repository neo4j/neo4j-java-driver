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
package org.neo4j.driver;

import java.io.Serializable;

/**
 * A transformer responsible for handling {@link Result} and outputting result of desired type.
 *
 * @param <T> output type
 */
@FunctionalInterface
public interface ResultTransformer<T> extends Serializable {
    /**
     * Transforms {@link Result} to desired output type.
     * <p>
     * This method must never return or proxy calls to the {@link Result} object as it is not guaranteed to be valid after this method supplies the output.
     *
     * @param result result value
     * @return output value
     */
    T transform(Result result);
}
