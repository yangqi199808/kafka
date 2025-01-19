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

package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecordSerde;
import org.apache.kafka.coordinator.share.generated.CoordinatorRecordType;

public class ShareCoordinatorRecordSerde extends CoordinatorRecordSerde {
    @Override
    protected ApiMessage apiMessageKeyFor(short recordType) {
        try {
            return CoordinatorRecordType.fromId(recordType).newRecordKey();
        } catch (UnsupportedVersionException ex) {
            throw new CoordinatorLoader.UnknownRecordTypeException(recordType);
        }
    }

    @Override
    protected ApiMessage apiMessageValueFor(short recordVersion) {
        try {
            return CoordinatorRecordType.fromId(recordVersion).newRecordValue();
        } catch (UnsupportedVersionException ex) {
            throw new CoordinatorLoader.UnknownRecordTypeException(recordVersion);
        }
    }
}
