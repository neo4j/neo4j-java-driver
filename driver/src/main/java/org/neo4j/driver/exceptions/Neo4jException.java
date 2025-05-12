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
package org.neo4j.driver.exceptions;

import java.io.Serial;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.GqlStatusError;
import org.neo4j.driver.types.TypeSystem;
import org.neo4j.driver.util.Preview;

/**
 * This is the base class for Neo4j exceptions.
 *
 * @since 1.0
 */
public class Neo4jException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = -80579062276712567L;

    /**
     * The code value.
     */
    private final String code;
    /**
     * The GQLSTATUS as defined by the GQL standard.
     * @since 5.26.0
     */
    private final String gqlStatus;
    /**
     * The GQLSTATUS description.
     * @since 5.26.0
     */
    private final String statusDescription;
    /**
     * The diagnostic record.
     * @since 5.26.0
     */
    @SuppressWarnings("serial")
    private final Map<String, Value> diagnosticRecord;
    /**
     * The GQLSTATUS error classification.
     * @since 5.26.0
     */
    private final GqlStatusErrorClassification classification;
    /**
     * The GQLSTATUS error classification as raw String.
     * @since 5.26.0
     */
    private final String rawClassification;

    /**
     * Creates a new instance.
     *
     * @param message the message
     */
    // for testing only
    public Neo4jException(String message) {
        this("N/A", message);
    }

    /**
     * Creates a new instance.
     *
     * @param message the message
     * @param cause   the cause
     */
    // for testing only
    public Neo4jException(String message, Throwable cause) {
        this("N/A", message, cause);
    }

    /**
     * Creates a new instance.
     *
     * @param code    the code
     * @param message the message
     */
    // for testing only
    public Neo4jException(String code, String message) {
        this(code, message, null);
    }

    /**
     * Creates a new instance.
     *
     * @param code    the code
     * @param message the message
     * @param cause   the cause
     */
    // for testing only
    public Neo4jException(String code, String message, Throwable cause) {
        this(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                code,
                message,
                GqlStatusError.DIAGNOSTIC_RECORD,
                cause);
    }

    /**
     * Creates a new instance.
     * @param gqlStatus the GQLSTATUS as defined by the GQL standard
     * @param statusDescription the status description
     * @param code the code
     * @param message the message
     * @param diagnosticRecord the diagnostic record
     * @param cause the cause
     * @since 5.26.0
     */
    @Preview(name = "GQL-error")
    public Neo4jException(
            String gqlStatus,
            String statusDescription,
            String code,
            String message,
            Map<String, Value> diagnosticRecord,
            Throwable cause) {
        super(message, cause);
        this.gqlStatus = Objects.requireNonNull(gqlStatus);
        this.statusDescription = Objects.requireNonNull(statusDescription);
        this.code = code;
        this.diagnosticRecord = Objects.requireNonNull(diagnosticRecord);
        var rawClassification = diagnosticRecord.get("_classification");
        if (rawClassification != null && TypeSystem.getDefault().STRING().isTypeOf(rawClassification)) {
            this.rawClassification = rawClassification.asString();
            GqlStatusErrorClassification classification = null;
            try {
                classification = GqlStatusErrorClassification.valueOf(this.rawClassification);
            } catch (Throwable e) {
                // ignored
            }
            this.classification = classification;
        } else {
            this.rawClassification = null;
            this.classification = null;
        }
    }

    /**
     * Access the status code for this exception. The Neo4j manual can
     * provide further details on the available codes and their meanings.
     *
     * @return textual code, such as "Neo.ClientError.Procedure.ProcedureNotFound"
     */
    public String code() {
        return code;
    }

    /**
     * Returns the GQLSTATUS as defined by the GQL standard.
     *
     * @return the GQLSTATUS value
     * @since 5.26.0
     */
    @Preview(name = "GQL-error")
    public String gqlStatus() {
        return gqlStatus;
    }

    /**
     * Returns the GQLSTATUS description.
     *
     * @return the GQLSTATUS description
     * @since 5.26.0
     */
    @Preview(name = "GQL-error")
    public String statusDescription() {
        return statusDescription;
    }

    /**
     * Returns the GQL diagnostic record.
     *
     * @return the GQL diagnostic record
     * @since 5.26.0
     */
    @Preview(name = "GQL-error")
    public Map<String, Value> diagnosticRecord() {
        return diagnosticRecord;
    }

    /**
     * Returns the error classification as a {@link GqlStatusErrorClassification} enum value.
     * <p>
     * The enum value is parsed using the {@link #rawClassification()} value. {@link Optional#empty()} is returned if
     * the {@link String} value is missing or is unknown.
     *
     * @return an {@link Optional} error classification as a {@link GqlStatusErrorClassification} enum value
     * @see #rawClassification()
     * @since 5.26.0
     */
    @Preview(name = "GQL-error")
    public Optional<GqlStatusErrorClassification> classification() {
        return Optional.ofNullable(classification);
    }

    /**
     * Returns the error classification as a {@link String} value.
     * <p>
     * The value is extracted from the {@link #diagnosticRecord()} map using {@code "_classification"} key and is
     * expected to be of {@link TypeSystem#STRING()} type. {@link Optional#empty()} is returned if the value is missing
     * or is of different type.
     *
     * @return an {@link Optional} error classification as a raw {@link String}
     * @see #diagnosticRecord()
     * @since 5.26.0
     */
    @Preview(name = "GQL-error")
    public Optional<String> rawClassification() {
        return Optional.ofNullable(rawClassification);
    }

    /**
     * Returns the GQL error cause.
     *
     * @return an {@link Optional} of GQL error cause
     * @since 5.26.0
     */
    @Preview(name = "GQL-error")
    public Optional<Neo4jException> gqlCause() {
        return findFirstGqlCause(this, Neo4jException.class);
    }

    @SuppressWarnings("DuplicatedCode")
    private static <T extends Throwable> Optional<T> findFirstGqlCause(Throwable throwable, Class<T> targetCls) {
        var cause = throwable.getCause();
        if (cause == null) {
            return Optional.empty();
        }
        if (targetCls.isAssignableFrom(cause.getClass())) {
            return Optional.of(targetCls.cast(cause));
        } else {
            return Optional.empty();
        }
    }
}
