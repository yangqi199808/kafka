/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.errors;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;

import java.util.Map;

/**
 * {@code ProductionExceptionHandler} that always instructs streams to fail when an exception
 * happens while attempting to produce result records.
 */
public class DefaultProductionExceptionHandler implements ProductionExceptionHandler {
    /**
     * @deprecated Since 3.9. Use {@link #handle(ErrorHandlerContext, ProducerRecord, Exception)} instead.
     */
    @SuppressWarnings("deprecation")
    @Deprecated
    @Override
    public ProductionExceptionHandlerResponse handle(final ProducerRecord<byte[], byte[]> record,
                                                     final Exception exception) {
        return exception instanceof RetriableException ?
            ProductionExceptionHandlerResponse.RETRY :
            ProductionExceptionHandlerResponse.FAIL;
    }

    @Override
    public ProductionExceptionHandlerResponse handle(final ErrorHandlerContext context,
                                                     final ProducerRecord<byte[], byte[]> record,
                                                     final Exception exception) {
        return exception instanceof RetriableException ?
            ProductionExceptionHandlerResponse.RETRY :
            ProductionExceptionHandlerResponse.FAIL;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        // ignore
    }
}