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
package org.apache.kafka.coordinator.transaction;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecordSerde;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValue;

public class TransactionCoordinatorRecordSerde extends CoordinatorRecordSerde {

    @Override
    protected ApiMessage apiMessageKeyFor(short recordType) {
        switch (recordType) {
            case 0:
                return new TransactionLogKey();
            default:
                throw new CoordinatorLoader.UnknownRecordTypeException(recordType);
        }
    }

    @Override
    protected ApiMessage apiMessageValueFor(short recordVersion) {
        switch (recordVersion) {
            case 0:
            case 1:
                return new TransactionLogValue();
            default:
                throw new CoordinatorLoader.UnknownRecordTypeException(recordVersion);
        }
    }
}
