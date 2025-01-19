/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import kafka.server.SimpleApiVersionManager
import org.apache.kafka.common.errors.{InvalidRequestException, UnsupportedVersionException}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{RequestHeader, RequestTestUtils}
import org.apache.kafka.server.common.{FinalizedFeatures, MetadataVersion}
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable

import java.util.Collections

class ProcessorTest {

  @Test
  def testParseRequestHeaderWithDisabledApi(): Unit = {
    val requestHeader = RequestTestUtils.serializeRequestHeader(
      new RequestHeader(ApiKeys.INIT_PRODUCER_ID, 0, "clientid", 0))
    val apiVersionManager = new SimpleApiVersionManager(ListenerType.CONTROLLER, true,
      () => new FinalizedFeatures(MetadataVersion.latestTesting(), Collections.emptyMap[String, java.lang.Short], 0, true))
    assertThrows(classOf[InvalidRequestException], (() => Processor.parseRequestHeader(apiVersionManager, requestHeader)): Executable,
      "INIT_PRODUCER_ID with listener type CONTROLLER should throw InvalidRequestException exception")
  }

  @Test
  def testParseRequestHeaderWithUnsupportedApiVersion(): Unit = {
    val requestHeader = RequestTestUtils.serializeRequestHeader(
      new RequestHeader(ApiKeys.PRODUCE, 0, "clientid", 0))
    val apiVersionManager = new SimpleApiVersionManager(ListenerType.BROKER, true,
      () => new FinalizedFeatures(MetadataVersion.latestTesting(), Collections.emptyMap[String, java.lang.Short], 0, true))
    assertThrows(classOf[UnsupportedVersionException], (() => Processor.parseRequestHeader(apiVersionManager, requestHeader)): Executable,
      "PRODUCE v0 should throw UnsupportedVersionException exception")
  }

}
