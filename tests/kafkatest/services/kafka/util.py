# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import namedtuple

from kafkatest.utils.remote_account import java_version
from kafkatest.version import LATEST_4_0, get_version

TopicPartition = namedtuple('TopicPartition', ['topic', 'partition'])

def fix_opts_for_new_jvm(node):
    # Startup scripts for early versions of Kafka contains options
    # that not supported on latest versions of JVM like -XX:+PrintGCDateStamps or -XX:UseParNewGC.
    # When system test run on JVM that doesn't support these options
    # we should setup environment variables with correct options.
    java_ver = java_version(node)
    if java_ver <= 9:
        return ""

    return ""

def get_log4j_config_param(node):
    return '-Dlog4j2.configurationFile=file:' if get_version(node) >= LATEST_4_0 else '-Dlog4j.configuration=file:'

def get_log4j_config(node):
    return 'log4j2.yaml' if get_version(node) >= LATEST_4_0 else 'log4j.properties'

def get_log4j_config_for_connect(node):
    return 'connect_log4j2.yaml' if get_version(node) >= LATEST_4_0 else 'connect_log4j.properties'

def get_log4j_config_for_tools(node):
    return 'tools_log4j2.yaml' if get_version(node) >= LATEST_4_0 else 'tools_log4j.properties'

def get_log4j_config_for_trogdor_coordinator(node):
    return 'trogdor-coordinator-log4j2.yaml' if get_version(node) >= LATEST_4_0 else 'trogdor-coordinator-log4j.properties'

def get_log4j_config_for_trogdor_agent(node):
    return 'trogdor-agent-log4j2.yaml' if get_version(node) >= LATEST_4_0 else 'trogdor-agent-log4j.properties'
