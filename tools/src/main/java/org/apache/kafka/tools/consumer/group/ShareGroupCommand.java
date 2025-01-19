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
package org.apache.kafka.tools.consumer.group;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeShareGroupsOptions;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareMemberAssignment;
import org.apache.kafka.clients.admin.ShareMemberDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import joptsimple.OptionException;

public class ShareGroupCommand {

    public static void main(String[] args) {
        ShareGroupCommandOptions opts = new ShareGroupCommandOptions(args);
        try {
            opts.checkArgs();
            CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to list all share groups, describe a share group, delete share group info, or reset share group offsets.");

            // should have exactly one action
            long actions = Stream.of(opts.listOpt, opts.describeOpt, opts.deleteOpt, opts.resetOffsetsOpt, opts.deleteOffsetsOpt).filter(opts.options::has).count();
            if (actions != 1)
                CommandLineUtils.printUsageAndExit(opts.parser, "Command must include exactly one action: --list, --describe, --delete, --reset-offsets, --delete-offsets.");

            run(opts);
        } catch (OptionException e) {
            CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage());
        }
    }

    public static void run(ShareGroupCommandOptions opts) {
        try (ShareGroupService shareGroupService = new ShareGroupService(opts, Map.of())) {
            if (opts.options.has(opts.listOpt)) {
                shareGroupService.listGroups();
            } else if (opts.options.has(opts.describeOpt)) {
                shareGroupService.describeGroups();
            } else if (opts.options.has(opts.deleteOpt)) {
                throw new UnsupportedOperationException("--delete option is not yet implemented");
            } else if (opts.options.has(opts.resetOffsetsOpt)) {
                throw new UnsupportedOperationException("--reset-offsets option is not yet implemented");
            } else if (opts.options.has(opts.deleteOffsetsOpt)) {
                throw new UnsupportedOperationException("--delete-offsets option is not yet implemented");
            }
        } catch (IllegalArgumentException e) {
            CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage());
        } catch (Throwable e) {
            printError("Executing share group command failed due to " + e.getMessage(), Optional.of(e));
        }
    }

    static Set<GroupState> groupStatesFromString(String input) {
        Set<GroupState> parsedStates =
            Arrays.stream(input.split(",")).map(s -> GroupState.parse(s.trim())).collect(Collectors.toSet());
        Set<GroupState> validStates = GroupState.groupStatesForType(GroupType.SHARE);
        if (!validStates.containsAll(parsedStates)) {
            throw new IllegalArgumentException("Invalid state list '" + input + "'. Valid states are: " +
                    validStates.stream().map(GroupState::toString).collect(Collectors.joining(", ")));
        }
        return parsedStates;
    }

    public static void printError(String msg, Optional<Throwable> e) {
        System.out.println("\nError: " + msg);
        e.ifPresent(Throwable::printStackTrace);
    }

    // Visibility for testing
    static class ShareGroupService implements AutoCloseable {
        final ShareGroupCommandOptions opts;
        private final Admin adminClient;

        public ShareGroupService(ShareGroupCommandOptions opts, Map<String, String> configOverrides) {
            this.opts = opts;
            try {
                this.adminClient = createAdminClient(configOverrides);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public ShareGroupService(ShareGroupCommandOptions opts, Admin adminClient) {
            this.opts = opts;
            this.adminClient = adminClient;
        }

        public void listGroups() throws ExecutionException, InterruptedException {
            if (opts.options.has(opts.stateOpt)) {
                String stateValue = opts.options.valueOf(opts.stateOpt);
                Set<GroupState> states = (stateValue == null || stateValue.isEmpty())
                    ? Set.of()
                    : groupStatesFromString(stateValue);
                List<GroupListing> listings = listShareGroupsInStates(states);

                printGroupInfo(listings);
            } else
                listShareGroups().forEach(System.out::println);
        }

        List<String> listShareGroups() {
            try {
                ListGroupsResult result = adminClient.listGroups(new ListGroupsOptions()
                    .timeoutMs(opts.options.valueOf(opts.timeoutMsOpt).intValue())
                    .withTypes(Set.of(GroupType.SHARE)));
                Collection<GroupListing> listings = result.all().get();
                return listings.stream().map(GroupListing::groupId).collect(Collectors.toList());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        List<GroupListing> listShareGroupsInStates(Set<GroupState> states) throws ExecutionException, InterruptedException {
            ListGroupsResult result = adminClient.listGroups(new ListGroupsOptions()
                .timeoutMs(opts.options.valueOf(opts.timeoutMsOpt).intValue())
                .withTypes(Set.of(GroupType.SHARE))
                .inGroupStates(states));
            return new ArrayList<>(result.all().get());
        }

        private void printGroupInfo(List<GroupListing> groups) {
            // find proper columns width
            int maxGroupLen = 15;
            for (GroupListing group : groups) {
                maxGroupLen = Math.max(maxGroupLen, group.groupId().length());
            }
            System.out.printf("%" + (-maxGroupLen) + "s %s\n", "GROUP", "STATE");
            for (GroupListing group : groups) {
                String groupId = group.groupId();
                String state = group.groupState().orElse(GroupState.UNKNOWN).toString();
                System.out.printf("%" + (-maxGroupLen) + "s %s\n", groupId, state);
            }
        }

        /**
         * Prints a summary of the state for situations where the group is empty or dead.
         *
         * @return Whether the group detail should be printed
         */
        public static boolean maybePrintEmptyGroupState(String group, GroupState state, int numRows) {
            if (state == GroupState.DEAD) {
                printError("Share group '" + group + "' does not exist.", Optional.empty());
            } else if (state == GroupState.EMPTY) {
                System.err.println("\nShare group '" + group + "' has no active members.");
            }

            return !state.equals(GroupState.DEAD) && numRows > 0;
        }

        public void describeGroups() throws ExecutionException, InterruptedException {
            Collection<String> groupIds = opts.options.has(opts.allGroupsOpt)
                ? listShareGroups()
                : opts.options.valuesOf(opts.groupOpt);
            if (opts.options.has(opts.membersOpt)) {
                TreeMap<String, ShareGroupDescription> members = collectGroupsDescription(groupIds);
                printMembers(members, opts.options.has(opts.verboseOpt));
            } else if (opts.options.has(opts.stateOpt)) {
                TreeMap<String, ShareGroupDescription> states = collectGroupsDescription(groupIds);
                printStates(states, opts.options.has(opts.verboseOpt));
            } else {
                TreeMap<String, Entry<ShareGroupDescription, Collection<SharePartitionOffsetInformation>>> offsets
                    = collectGroupsOffsets(groupIds);
                printOffsets(offsets);
            }
        }

        Map<String, ShareGroupDescription> describeShareGroups(Collection<String> groupIds) throws ExecutionException, InterruptedException {
            Map<String, ShareGroupDescription> res = new HashMap<>();
            Map<String, KafkaFuture<ShareGroupDescription>> stringKafkaFutureMap = adminClient.describeShareGroups(
                groupIds,
                new DescribeShareGroupsOptions().timeoutMs(opts.options.valueOf(opts.timeoutMsOpt).intValue())
            ).describedGroups();

            for (Entry<String, KafkaFuture<ShareGroupDescription>> e : stringKafkaFutureMap.entrySet()) {
                res.put(e.getKey(), e.getValue().get());
            }
            return res;
        }

        TreeMap<String, ShareGroupDescription> collectGroupsDescription(Collection<String> groupIds) throws ExecutionException, InterruptedException {
            Map<String, ShareGroupDescription> shareGroups = describeShareGroups(groupIds);
            TreeMap<String, ShareGroupDescription> res = new TreeMap<>();
            shareGroups.forEach(res::put);
            return res;
        }

        TreeMap<String, Entry<ShareGroupDescription, Collection<SharePartitionOffsetInformation>>> collectGroupsOffsets(Collection<String> groupIds) throws ExecutionException, InterruptedException {
            Map<String, ShareGroupDescription> shareGroups = describeShareGroups(groupIds);
            TreeMap<String, Entry<ShareGroupDescription, Collection<SharePartitionOffsetInformation>>> groupOffsets = new TreeMap<>();

            shareGroups.forEach((groupId, shareGroup) -> {
                Set<TopicPartition> allTp = new HashSet<>();
                for (ShareMemberDescription memberDescription : shareGroup.members()) {
                    allTp.addAll(memberDescription.assignment().topicPartitions());
                }

                // Fetch latest and earliest offsets
                Map<TopicPartition, OffsetSpec> earliest = new HashMap<>();
                Map<TopicPartition, OffsetSpec> latest = new HashMap<>();

                for (TopicPartition tp : allTp) {
                    earliest.put(tp, OffsetSpec.earliest());
                    latest.put(tp, OffsetSpec.latest());
                }

                // This call to obtain the earliest offsets will be replaced once adminClient.listShareGroupOffsets is implemented
                try {
                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestResult = adminClient.listOffsets(earliest).all().get();
                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestResult = adminClient.listOffsets(latest).all().get();

                    Set<SharePartitionOffsetInformation> partitionOffsets = new HashSet<>();

                    for (Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> tp : earliestResult.entrySet()) {
                        SharePartitionOffsetInformation partitionOffsetInfo = new SharePartitionOffsetInformation(
                            groupId,
                            tp.getKey().topic(),
                            tp.getKey().partition(),
                            latestResult.get(tp.getKey()).offset() - earliestResult.get(tp.getKey()).offset()
                        );
                        partitionOffsets.add(partitionOffsetInfo);
                    }
                    groupOffsets.put(groupId, new SimpleImmutableEntry<>(shareGroup, partitionOffsets));
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });

            return groupOffsets;
        }

        private void printOffsets(TreeMap<String, Entry<ShareGroupDescription, Collection<SharePartitionOffsetInformation>>> offsets) {
            offsets.forEach((groupId, tuple) -> {
                ShareGroupDescription description = tuple.getKey();
                Collection<SharePartitionOffsetInformation> offsetsInfo = tuple.getValue();
                if (maybePrintEmptyGroupState(groupId, description.groupState(), offsetsInfo.size())) {
                    String fmt = printOffsetFormat(groupId, offsetsInfo);
                    System.out.printf(fmt, "GROUP", "TOPIC", "PARTITION", "START-OFFSET");

                    for (SharePartitionOffsetInformation info : offsetsInfo) {
                        System.out.printf(fmt,
                            groupId,
                            info.topic,
                            info.partition,
                            info.offset
                        );
                    }
                    System.out.println();
                }
            });
        }

        private static String printOffsetFormat(String groupId, Collection<SharePartitionOffsetInformation> offsetsInfo) {
            int groupLen = Math.max(15, groupId.length());
            int maxTopicLen = 15;
            for (SharePartitionOffsetInformation info : offsetsInfo) {
                maxTopicLen = Math.max(maxTopicLen, info.topic.length());
            }
            return "\n%" + (-groupLen) + "s %" + (-maxTopicLen) + "s %-10s %s";
        }

        private void printStates(Map<String, ShareGroupDescription> descriptions, boolean verbose) {
            descriptions.forEach((groupId, description) -> {
                maybePrintEmptyGroupState(groupId, description.groupState(), 1);

                int groupLen = Math.max(15, groupId.length());
                String coordinator = description.coordinator().host() + ":" + description.coordinator().port() + "  (" + description.coordinator().idString() + ")";
                int coordinatorLen = Math.max(25, coordinator.length());

                if (verbose) {
                    String fmt = "\n%" + -groupLen + "s %" + -coordinatorLen + "s %-15s %-12s %-17s %s";
                    System.out.printf(fmt, "GROUP", "COORDINATOR (ID)", "STATE", "GROUP-EPOCH", "ASSIGNMENT-EPOCH", "#MEMBERS");
                    System.out.printf(fmt, groupId, coordinator, description.groupState().toString(),
                        description.groupEpoch(), description.targetAssignmentEpoch(), description.members().size());
                } else {
                    String fmt = "\n%" + -groupLen + "s %" + -coordinatorLen + "s %-15s %s";
                    System.out.printf(fmt, "GROUP", "COORDINATOR (ID)", "STATE", "#MEMBERS");
                    System.out.printf(fmt, groupId, coordinator, description.groupState().toString(), description.members().size());
                }
                System.out.println();
            });
        }

        private void printMembers(TreeMap<String, ShareGroupDescription> descriptions, boolean verbose) {
            descriptions.forEach((groupId, description) -> {
                int groupLen = Math.max(15, groupId.length());
                int maxConsumerIdLen = 15, maxHostLen = 15, maxClientIdLen = 15;
                Collection<ShareMemberDescription> members = description.members();
                if (maybePrintEmptyGroupState(groupId, description.groupState(), description.members().size())) {
                    for (ShareMemberDescription member : members) {
                        maxConsumerIdLen = Math.max(maxConsumerIdLen, member.consumerId().length());
                        maxHostLen = Math.max(maxHostLen, member.host().length());
                        maxClientIdLen = Math.max(maxClientIdLen, member.clientId().length());
                    }

                    if (verbose) {
                        String fmt = "\n%" + -groupLen + "s %" + -maxConsumerIdLen + "s %" + -maxHostLen + "s %" + -maxClientIdLen + "s %-13s %s";
                        System.out.printf(fmt, "GROUP", "CONSUMER-ID", "HOST", "CLIENT-ID", "MEMBER-EPOCH", "ASSIGNMENT");
                        for (ShareMemberDescription member : members) {
                            System.out.printf(fmt, groupId, member.consumerId(), member.host(), member.clientId(), member.memberEpoch(), getAssignmentString(member.assignment()));
                        }
                    } else {
                        String fmt = "\n%" + -groupLen + "s %" + -maxConsumerIdLen + "s %" + -maxHostLen + "s %" + -maxClientIdLen + "s %s";
                        System.out.printf(fmt, "GROUP", "CONSUMER-ID", "HOST", "CLIENT-ID", "ASSIGNMENT");
                        for (ShareMemberDescription member : members) {
                            System.out.printf(fmt, groupId, member.consumerId(), member.host(), member.clientId(), getAssignmentString(member.assignment()));
                        }
                    }
                    System.out.println();
                }
            });
        }

        private String getAssignmentString(ShareMemberAssignment assignment) {
            Map<String, List<TopicPartition>> grouped = new HashMap<>();
            assignment.topicPartitions().forEach(tp ->
                grouped
                   .computeIfAbsent(tp.topic(), key -> new ArrayList<>())
                    .add(tp)
            );
            return grouped.entrySet().stream().map(entry -> {
                String topicName = entry.getKey();
                List<TopicPartition> topicPartitions = entry.getValue();
                return topicPartitions
                    .stream()
                    .map(TopicPartition::partition)
                    .map(Object::toString)
                    .sorted()
                    .collect(Collectors.joining(",", topicName + ":", ""));
            }).sorted().collect(Collectors.joining(";"));
        }

        public void close() {
            adminClient.close();
        }

        protected Admin createAdminClient(Map<String, String> configOverrides) throws IOException {
            Properties props = opts.options.has(opts.commandConfigOpt) ? Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) : new Properties();
            props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt));
            props.putAll(configOverrides);
            return Admin.create(props);
        }
    }

    static class SharePartitionOffsetInformation {
        final String group;
        final String topic;
        final int partition;
        final long offset;

        SharePartitionOffsetInformation(
            String group,
            String topic,
            int partition,
            long offset
        ) {
            this.group = group;
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }
    }
}
