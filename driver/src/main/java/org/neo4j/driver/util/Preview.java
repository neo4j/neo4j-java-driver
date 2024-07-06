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
package org.neo4j.driver.util;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A marker annotation indicating that the annotated target belongs to a preview feature.
 * <p>
 * The preview feature is a new feature that is a candidate for a future GA status. It enables users to try the feature
 * out and maintainers to refine and update it.
 * <p>
 * The preview features are not considered to be experimental, temporary or unstable. However, they may change more
 * rapidly without the deprecation cycle. Most preview features are expected to be granted the GA status unless some
 * unexpected conditions arise.
 * <p>
 * Due to the increased flexibility of the preview status, user feedback is encouraged so that it can be considered
 * before the GA status. See the driver's README for details on how to provide the feedback.
 *
 * @since 5.7
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.CONSTRUCTOR})
public @interface Preview {
    /**
     * The feature name or a reference.
     *
     * @return the feature name or a reference
     */
    String name();
}
