/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service.persistent;

import static org.apache.pulsar.broker.service.persistent.PersistentTopic.MESSAGE_RATE_BACKOFF_MS;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NoMoreEntriesToReadException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.delayed.BucketDelayedDeliveryTrackerFactory;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTracker;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTrackerFactory;
import org.apache.pulsar.broker.delayed.InMemoryDelayedDeliveryTracker;
import org.apache.pulsar.broker.delayed.bucket.BucketDelayedDeliveryTracker;
import org.apache.pulsar.broker.service.AbstractDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.EntryAndMetadata;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.InMemoryRedeliveryTracker;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.RedeliveryTrackerDisabled;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.SharedConsumerAssignor;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter.Type;
import org.apache.pulsar.broker.transaction.exception.buffer.TransactionBufferException;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.stats.TopicMetricBean;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class PersistentDispatcherMultipleConsumers extends AbstractDispatcherMultipleConsumers
        implements Dispatcher, ReadEntriesCallback {

    protected final PersistentTopic topic;
    protected final ManagedCursor cursor;
    protected volatile Range<PositionImpl> lastIndividualDeletedRangeFromCursorRecovery;

    private CompletableFuture<Void> closeFuture = null;
    protected final MessageRedeliveryController redeliveryMessages;
    protected final RedeliveryTracker redeliveryTracker;

    private Optional<DelayedDeliveryTracker> delayedDeliveryTracker = Optional.empty();

    protected volatile boolean havePendingRead = false;
    protected volatile boolean havePendingReplayRead = false;
    protected volatile PositionImpl minReplayedPosition = null;
    protected boolean shouldRewindBeforeReadingOrReplaying = false;
    protected final String name;
    private boolean sendInProgress = false;
    protected static final AtomicIntegerFieldUpdater<PersistentDispatcherMultipleConsumers>
            TOTAL_AVAILABLE_PERMITS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentDispatcherMultipleConsumers.class,
                    "totalAvailablePermits");
    protected volatile int totalAvailablePermits = 0;
    protected volatile int readBatchSize;
    protected final Backoff readFailureBackoff;
    private static final AtomicIntegerFieldUpdater<PersistentDispatcherMultipleConsumers>
            TOTAL_UNACKED_MESSAGES_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentDispatcherMultipleConsumers.class,
                    "totalUnackedMessages");
    protected volatile int totalUnackedMessages = 0;
    private volatile int blockedDispatcherOnUnackedMsgs = FALSE;
    protected static final AtomicIntegerFieldUpdater<PersistentDispatcherMultipleConsumers>
            BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentDispatcherMultipleConsumers.class,
                    "blockedDispatcherOnUnackedMsgs");
    protected Optional<DispatchRateLimiter> dispatchRateLimiter = Optional.empty();
    private AtomicBoolean isRescheduleReadInProgress = new AtomicBoolean(false);
    protected final ExecutorService dispatchMessagesThread;
    private final SharedConsumerAssignor assignor;

    protected enum ReadType {
        Normal, Replay
    }

    public PersistentDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor,
            Subscription subscription) {
        this(topic, cursor, subscription, true);
    }

    public PersistentDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor, Subscription subscription,
            boolean allowOutOfOrderDelivery) {
        super(subscription, topic.getBrokerService().pulsar().getConfiguration());
        this.cursor = cursor;
        this.lastIndividualDeletedRangeFromCursorRecovery = cursor.getLastIndividualDeletedRange();
        this.name = topic.getName() + " / " + Codec.decode(cursor.getName());
        this.topic = topic;
        this.dispatchMessagesThread = topic.getBrokerService().getTopicOrderedExecutor().chooseThread();
        this.redeliveryMessages = new MessageRedeliveryController(allowOutOfOrderDelivery);
        this.redeliveryTracker = this.serviceConfig.isSubscriptionRedeliveryTrackerEnabled()
                ? new InMemoryRedeliveryTracker()
                : RedeliveryTrackerDisabled.REDELIVERY_TRACKER_DISABLED;
        this.readBatchSize = serviceConfig.getDispatcherMaxReadBatchSize();
        this.initializeDispatchRateLimiterIfNeeded();
        this.assignor = new SharedConsumerAssignor(this::getNextConsumer, this::addMessageToReplay);
        this.readFailureBackoff = new Backoff(
                topic.getBrokerService().pulsar().getConfiguration().getDispatcherReadFailureBackoffInitialTimeInMs(),
                TimeUnit.MILLISECONDS,
                1, TimeUnit.MINUTES, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized CompletableFuture<Void> addConsumer(Consumer consumer) {
        if (IS_CLOSED_UPDATER.get(this) == TRUE) {
            log.warn("[{}] Dispatcher is already closed. Closing consumer {}", name, consumer);
            consumer.disconnect();
            return CompletableFuture.completedFuture(null);
        }
        // 如果发现consumerList为空，说明之前的consumer已经全部断开，我们要重置一遍dispatcher的状态
        if (consumerList.isEmpty()) {
            if (havePendingRead || havePendingReplayRead) {
                // 如果有之前的读操作在进行，我们要等待这些读操作结束后，再进行rewind，因此这里先设置一个标志位
                shouldRewindBeforeReadingOrReplaying = true;
            } else {
                // 把cursor的位置调整到markDelete位置
                cursor.rewind();
                shouldRewindBeforeReadingOrReplaying = false;
            }
            // 清除需要重新投递的消息列表
            redeliveryMessages.clear();
            // 清除延迟投递的消息列表
            delayedDeliveryTracker.ifPresent(tracker -> {
                // Don't clean up BucketDelayedDeliveryTracker, otherwise we will lose the bucket snapshot
                if (tracker instanceof InMemoryDelayedDeliveryTracker) {
                    tracker.clear();
                }
            });
        }

        // 判断是否超过consumer上限，若超过则无法添加新的consumer
        if (isConsumersExceededOnSubscription()) {
            log.warn("[{}] Attempting to add consumer to subscription which reached max consumers limit", name);
            return FutureUtil.failedFuture(new ConsumerBusyException("Subscription reached max consumers limit"));
        }

        // 添加consumer到consumerList中
        consumerList.add(consumer);
        // 在consumerList中根据consumer的优先级排序consumer
        if (consumerList.size() > 1
                && consumer.getPriorityLevel() < consumerList.get(consumerList.size() - 2).getPriorityLevel()) {
            consumerList.sort(Comparator.comparingInt(Consumer::getPriorityLevel));
        }
        // 添加consumer到consumerSet中
        consumerSet.add(consumer);

        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected boolean isConsumersExceededOnSubscription() {
        return isConsumersExceededOnSubscription(topic, consumerList.size());
    }

    @Override
    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
        // decrement unack-message count for removed consumer
        // 将要移除的consumer的未ack消息数量从未ack消息总数中减去
        addUnAckedMessages(-consumer.getUnackedMessages());
        // consumer存在consumerSet中并被移除
        if (consumerSet.removeAll(consumer) == 1) {
            // 从consumerList中移除consumer
            consumerList.remove(consumer);
            log.info("Removed consumer {} with pending {} acks", consumer, consumer.getPendingAcks().size());
            if (consumerList.isEmpty()) { // 当前consumer移除后，dispatcher中没有consumer的情况
                // 取消进行中的读操作
                cancelPendingRead();

                // 清除redelivery消息
                redeliveryMessages.clear();
                redeliveryTracker.clear();
                // 如果有关闭前需要执行的操作，那么执行一下
                if (closeFuture != null) {
                    log.info("[{}] All consumers removed. Subscription is disconnected", name);
                    closeFuture.complete(null);
                }
                // 重置availablePermits为0
                totalAvailablePermits = 0;
            } else { // 移除consumer后，dispatcher中还有consumer的情况
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Consumer are left, reading more entries", name);
                }
                // 当前要移除的consumer中没有ack的消息都要重新replay
                consumer.getPendingAcks().forEach((ledgerId, entryId, batchSize, stickyKeyHash) -> {
                    addMessageToReplay(ledgerId, entryId, stickyKeyHash);
                });
                // dispatcher的availablePermits要减去要移除的这个consumer的availablePermits
                totalAvailablePermits -= consumer.getAvailablePermits();
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Decreased totalAvailablePermits by {} in PersistentDispatcherMultipleConsumers. "
                                    + "New dispatcher permit count is {}", name, consumer.getAvailablePermits(),
                            totalAvailablePermits);
                }
                // 主动触发读操作，因为这里可能有需要replay给consumer的消息
                readMoreEntries();
            }
        } else {
            log.info("[{}] Trying to remove a non-connected consumer: {}", name, consumer);
        }
    }

    @Override
    public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {
        topic.getBrokerService().executor().execute(() -> {
            internalConsumerFlow(consumer, additionalNumberOfMessages);
        });
    }

    private synchronized void internalConsumerFlow(Consumer consumer, int additionalNumberOfMessages) {
        // 如果consumer不存在，则直接返回
        if (!consumerSet.contains(consumer)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Ignoring flow control from disconnected consumer {}", name, consumer);
            }
            return;
        }

        // consumer请求可以接收这么多的消息数，这个数量要加到dispatcher的permits上
        totalAvailablePermits += additionalNumberOfMessages;

        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Trigger new read after receiving flow control message with permits {} "
                            + "after adding {} permits", name, consumer,
                    totalAvailablePermits, additionalNumberOfMessages);
        }
        // 触发读操作
        readMoreEntries();
    }

    /**
     * We should not call readMoreEntries() recursively in the same thread as there is a risk of StackOverflowError.
     *
     */
    public void readMoreEntriesAsync() {
        topic.getBrokerService().executor().execute(this::readMoreEntries);
    }

    public synchronized void readMoreEntries() {
        // 如果之前的发送操作正在进行中，我们不应该读新的entries，否则可能读取到相同的entries，导致重复发送
        if (isSendInProgress()) {
            // we cannot read more entries while sending the previous batch
            // otherwise we could re-read the same entries and send duplicates
            return;
        }
        // 是否需要因为延迟投递暂停发送
        if (shouldPauseDeliveryForDelayTracker()) {
            return;
        }

        // totalAvailablePermits may be updated by other threads
        // 找到第一个有permits的consumer的permits数
        int firstAvailableConsumerPermits = getFirstAvailableConsumerPermits();
        // 因为其它线程可能修改totalAvailablePermits，因此这里取firstAvailableConsumerPermits和totalAvailablePermits的最大值
        // 作为当前可用的permits
        int currentTotalAvailablePermits = Math.max(totalAvailablePermits, firstAvailableConsumerPermits);
        if (currentTotalAvailablePermits > 0 && firstAvailableConsumerPermits > 0) {
            // 计算最终当前可以读取的消息条数以及消息大小
            Pair<Integer, Long> calculateResult = calculateToRead(currentTotalAvailablePermits);
            int messagesToRead = calculateResult.getLeft();
            long bytesToRead = calculateResult.getRight();

            // 可能因为限速，或者之前读操作没有完成的原因，现在不能进行读取操作，那么直接返回
            if (messagesToRead == -1 || bytesToRead == -1) {
                // Skip read as topic/dispatcher has exceed the dispatch rate or previous pending read hasn't complete.
                return;
            }

            // 获取需要重新投递的消息
            NavigableSet<PositionImpl> messagesToReplayNow = getMessagesToReplayNow(messagesToRead);

            if (!messagesToReplayNow.isEmpty()) { // 需要重新投递的消息不为空，那么优先投递这部分消息
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Schedule replay of {} messages for {} consumers", name, messagesToReplayNow.size(),
                            consumerList.size());
                }

                // 表示有正在进行中的replay操作
                havePendingReplayRead = true;
                // 获取当前需要replay的消息中最旧消息的Position
                minReplayedPosition = messagesToReplayNow.first();
                // 读取需要replay的消息并发送给consumer，这里返回值是在replay之前发现已经删除的消息
                Set<? extends Position> deletedMessages = topic.isDelayedDeliveryEnabled()
                        ? asyncReplayEntriesInOrder(messagesToReplayNow) : asyncReplayEntries(messagesToReplayNow);

                // 对于已经删除的消息，要从redeliveryMessages中移除
                deletedMessages.forEach(position -> redeliveryMessages.remove(((PositionImpl) position).getLedgerId(),
                        ((PositionImpl) position).getEntryId()));
                // if all the entries are acked-entries and cleared up from redeliveryMessages, try to read
                // next entries as readCompletedEntries-callback was never called
                // 如果需要replay的消息在replay之前就已经全部被确认了，那么这里要主动触发readMoreEntries操作
                if ((messagesToReplayNow.size() - deletedMessages.size()) == 0) {
                    havePendingReplayRead = false;
                    readMoreEntriesAsync();
                }
            } else if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.get(this) == TRUE) {
                // unack的消息数量超过上限时，这里不会读取新的消息
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Dispatcher read is blocked due to unackMessages {} reached to max {}", name,
                            totalUnackedMessages, topic.getMaxUnackedMessagesOnSubscription());
                }
            } else if (!havePendingRead) { // 没有需要replay的消息，没有因为unack消息卡住，没有正在进行中的读操作
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Schedule read of {} messages for {} consumers", name, messagesToRead,
                            consumerList.size());
                }
                // 设置有正在进行中的读操作
                havePendingRead = true;
                // 获取第一条需要replay的消息，也就是最旧的那一条
                NavigableSet<PositionImpl> toReplay = getMessagesToReplayNow(1);
                if (!toReplay.isEmpty()) {
                    // 记录这时候需要replay的最小的Position，因为我们不会发送这条消息，因此把他加回redeliveryMessages
                    minReplayedPosition = toReplay.first();
                    redeliveryMessages.add(minReplayedPosition.getLedgerId(), minReplayedPosition.getEntryId());
                } else {
                    minReplayedPosition = null;
                }

                // Filter out and skip read delayed messages exist in DelayedDeliveryTracker
                // 如果有延迟发送消息的情况，过滤出已经存在于DelayedDeliveryTracker中的消息，发送剩余的消息
                if (delayedDeliveryTracker.isPresent()) {
                    Predicate<PositionImpl> skipCondition = null;
                    final DelayedDeliveryTracker deliveryTracker = delayedDeliveryTracker.get();
                    if (deliveryTracker instanceof BucketDelayedDeliveryTracker) {
                        skipCondition = position -> ((BucketDelayedDeliveryTracker) deliveryTracker)
                                .containsMessage(position.getLedgerId(), position.getEntryId());
                    }
                    cursor.asyncReadEntriesWithSkipOrWait(messagesToRead, bytesToRead, this, ReadType.Normal,
                            topic.getMaxReadPosition(), skipCondition);
                } else {
                    // 调用cursor去读取消息
                    cursor.asyncReadEntriesOrWait(messagesToRead, bytesToRead, this, ReadType.Normal,
                            topic.getMaxReadPosition());
                }
            } else {
                log.debug("[{}] Cannot schedule next read until previous one is done", name);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Consumer buffer is full, pause reading", name);
            }
        }
    }

    @Override
    protected void reScheduleRead() {
        if (isRescheduleReadInProgress.compareAndSet(false, true)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Reschedule message read in {} ms", topic.getName(), name, MESSAGE_RATE_BACKOFF_MS);
            }
            topic.getBrokerService().executor().schedule(
                    () -> {
                        isRescheduleReadInProgress.set(false);
                        readMoreEntries();
                        },
                    MESSAGE_RATE_BACKOFF_MS, TimeUnit.MILLISECONDS);
        }
    }

    // left pair is messagesToRead, right pair is bytesToRead
    protected Pair<Integer, Long> calculateToRead(int currentTotalAvailablePermits) {
        // 可以读取的消息数是当前可用的permits以及最大批量读取消息数量的最小值
        int messagesToRead = Math.min(currentTotalAvailablePermits, readBatchSize);
        // 一次读取最大可读取的消息大小
        long bytesToRead = serviceConfig.getDispatcherMaxReadSizeBytes();

        // 随机获取一个consumer
        Consumer c = getRandomConsumer();
        // if turn on precise dispatcher flow control, adjust the record to read
        // 如果开启精确的分发，则计算出精确的可以读取的消息数
        if (c != null && c.isPreciseDispatcherFlowControl()) {
            int avgMessagesPerEntry = Math.max(1, c.getAvgMessagesPerEntry());
            messagesToRead = Math.min(
                    (int) Math.ceil(currentTotalAvailablePermits * 1.0 / avgMessagesPerEntry),
                    readBatchSize);
        }

        // 如果当前consumer不能写，那么把可读消息设置为1，这样可以应用原来代码的通知机制
        if (!isConsumerWritable()) {
            // If the connection is not currently writable, we issue the read request anyway, but for a single
            // message. The intent here is to keep use the request as a notification mechanism while avoiding to
            // read and dispatch a big batch of messages which will need to wait before getting written to the
            // socket.
            messagesToRead = 1;
        }

        // throttle only if: (1) cursor is not active (or flag for throttle-nonBacklogConsumer is enabled) bcz
        // active-cursor reads message from cache rather from bookkeeper (2) if topic has reached message-rate
        // threshold: then schedule the read after MESSAGE_RATE_BACKOFF_MS
        // 限速过滤
        if (serviceConfig.isDispatchThrottlingOnNonBacklogConsumerEnabled() || !cursor.isActive()) {
            if (topic.getBrokerDispatchRateLimiter().isPresent()) {
                DispatchRateLimiter brokerRateLimiter = topic.getBrokerDispatchRateLimiter().get();
                if (reachDispatchRateLimit(brokerRateLimiter)) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] message-read exceeded broker message-rate {}/{}, schedule after a {}", name,
                                brokerRateLimiter.getDispatchRateOnMsg(), brokerRateLimiter.getDispatchRateOnByte(),
                                MESSAGE_RATE_BACKOFF_MS);
                    }
                    return Pair.of(-1, -1L);
                } else {
                    Pair<Integer, Long> calculateToRead =
                            updateMessagesToRead(brokerRateLimiter, messagesToRead, bytesToRead);
                    messagesToRead = calculateToRead.getLeft();
                    bytesToRead = calculateToRead.getRight();
                }
            }

            if (topic.getDispatchRateLimiter().isPresent()) {
                DispatchRateLimiter topicRateLimiter = topic.getDispatchRateLimiter().get();
                if (reachDispatchRateLimit(topicRateLimiter)) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] message-read exceeded topic message-rate {}/{}, schedule after a {}", name,
                                topicRateLimiter.getDispatchRateOnMsg(), topicRateLimiter.getDispatchRateOnByte(),
                                MESSAGE_RATE_BACKOFF_MS);
                    }
                    return Pair.of(-1, -1L);
                } else {
                    Pair<Integer, Long> calculateToRead =
                            updateMessagesToRead(topicRateLimiter, messagesToRead, bytesToRead);
                    messagesToRead = calculateToRead.getLeft();
                    bytesToRead = calculateToRead.getRight();
                }
            }

            if (dispatchRateLimiter.isPresent()) {
                if (reachDispatchRateLimit(dispatchRateLimiter.get())) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] message-read exceeded subscription message-rate {}/{}, schedule after a {}",
                                name, dispatchRateLimiter.get().getDispatchRateOnMsg(),
                                dispatchRateLimiter.get().getDispatchRateOnByte(),
                                MESSAGE_RATE_BACKOFF_MS);
                    }
                    return Pair.of(-1, -1L);
                } else {
                    Pair<Integer, Long> calculateToRead =
                            updateMessagesToRead(dispatchRateLimiter.get(), messagesToRead, bytesToRead);
                    messagesToRead = calculateToRead.getLeft();
                    bytesToRead = calculateToRead.getRight();
                }
            }
        }

        // 有正在replay的操作，那返回-1，-1
        if (havePendingReplayRead) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Skipping replay while awaiting previous read to complete", name);
            }
            return Pair.of(-1, -1L);
        }

        // If messagesToRead is 0 or less, correct it to 1 to prevent IllegalArgumentException
        messagesToRead = Math.max(messagesToRead, 1);
        bytesToRead = Math.max(bytesToRead, 1);
        return Pair.of(messagesToRead, bytesToRead);
    }

    protected Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions) {
        return cursor.asyncReplayEntries(positions, this, ReadType.Replay);
    }

    protected Set<? extends Position> asyncReplayEntriesInOrder(Set<? extends Position> positions) {
        return cursor.asyncReplayEntries(positions, this, ReadType.Replay, true);
    }

    @Override
    public boolean isConsumerConnected() {
        return !consumerList.isEmpty();
    }

    @Override
    public CopyOnWriteArrayList<Consumer> getConsumers() {
        return consumerList;
    }

    @Override
    public synchronized boolean canUnsubscribe(Consumer consumer) {
        return consumerList.size() == 1 && consumerSet.contains(consumer);
    }

    @Override
    public CompletableFuture<Void> close() {
        IS_CLOSED_UPDATER.set(this, TRUE);

        Optional<DelayedDeliveryTracker> delayedDeliveryTracker;
        synchronized (this) {
            delayedDeliveryTracker = this.delayedDeliveryTracker;
            this.delayedDeliveryTracker = Optional.empty();
        }

        delayedDeliveryTracker.ifPresent(DelayedDeliveryTracker::close);

        dispatchRateLimiter.ifPresent(DispatchRateLimiter::close);

        return disconnectAllConsumers();
    }

    @Override
    public synchronized CompletableFuture<Void> disconnectAllConsumers(boolean isResetCursor) {
        closeFuture = new CompletableFuture<>();
        if (consumerList.isEmpty()) {
            closeFuture.complete(null);
        } else {
            consumerList.forEach(consumer -> consumer.disconnect(isResetCursor));
            cancelPendingRead();
        }
        return closeFuture;
    }

    @Override
    protected void cancelPendingRead() {
        if (havePendingRead && cursor.cancelPendingReadRequest()) {
            havePendingRead = false;
        }
    }

    @Override
    public CompletableFuture<Void> disconnectActiveConsumers(boolean isResetCursor) {
        return disconnectAllConsumers(isResetCursor);
    }

    @Override
    public synchronized void resetCloseFuture() {
        closeFuture = null;
    }

    @Override
    public void reset() {
        resetCloseFuture();
        IS_CLOSED_UPDATER.set(this, FALSE);
    }

    @Override
    public SubType getType() {
        return SubType.Shared;
    }

    @Override
    public final synchronized void readEntriesComplete(List<Entry> entries, Object ctx) {
        // readEntries成功的回调
        ReadType readType = (ReadType) ctx;
        // 更新状态
        if (readType == ReadType.Normal) {
            havePendingRead = false;
        } else {
            havePendingReplayRead = false;
        }

        // 更新readBatchSize
        if (readBatchSize < serviceConfig.getDispatcherMaxReadBatchSize()) {
            int newReadBatchSize = Math.min(readBatchSize * 2, serviceConfig.getDispatcherMaxReadBatchSize());
            if (log.isDebugEnabled()) {
                log.debug("[{}] Increasing read batch size from {} to {}", name, readBatchSize, newReadBatchSize);
            }

            readBatchSize = newReadBatchSize;
        }

        readFailureBackoff.reduceToHalf();

        // 如果是读取新消息并且shouldRewindBeforeReadingOrReplaying，那么这批消息要释放，不能发给consumer
        // 因为所有consumer已经断开连接了，在这个读操作完成之前
        if (shouldRewindBeforeReadingOrReplaying && readType == ReadType.Normal) {
            // All consumers got disconnected before the completion of the read operation
            entries.forEach(Entry::release);
            cursor.rewind();
            shouldRewindBeforeReadingOrReplaying = false;
            readMoreEntries();
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Distributing {} messages to {} consumers", name, entries.size(), consumerList.size());
        }

        // 更新等待dispatch的消息大小
        long size = entries.stream().mapToLong(Entry::getLength).sum();
        updatePendingBytesToDispatch(size);

        // dispatch messages to a separate thread, but still in order for this subscription
        // sendMessagesToConsumers is responsible for running broker-side filters
        // that may be quite expensive
        if (serviceConfig.isDispatcherDispatchMessagesInSubscriptionThread()) {
            // setting sendInProgress here, because sendMessagesToConsumers will be executed
            // in a separate thread, and we want to prevent more reads
            // 因为发送操作在另外一个线程执行，因此这里设置标志位为true，避免在发送完成前进行新的readEntries操作
            acquireSendInProgress();
            dispatchMessagesThread.execute(() -> {
                // 发送消息给consumer
                if (sendMessagesToConsumers(readType, entries, false)) {
                    // 更新等待dispatch的消息大小
                    updatePendingBytesToDispatch(-size);
                    // readMoreEntries
                    readMoreEntries();
                } else {
                    updatePendingBytesToDispatch(-size);
                }
            });
        } else {
            if (sendMessagesToConsumers(readType, entries, true)) {
                updatePendingBytesToDispatch(-size);
                readMoreEntriesAsync();
            } else {
                updatePendingBytesToDispatch(-size);
            }
        }
    }

    protected synchronized void acquireSendInProgress() {
        sendInProgress = true;
    }

    protected synchronized void releaseSendInProgress() {
        sendInProgress = false;
    }

    protected synchronized boolean isSendInProgress() {
        return sendInProgress;
    }

    protected final synchronized boolean sendMessagesToConsumers(ReadType readType, List<Entry> entries,
                                                                 boolean needAcquireSendInProgress) {
        if (needAcquireSendInProgress) {
            acquireSendInProgress();
        }
        try {
            return trySendMessagesToConsumers(readType, entries);
        } finally {
            releaseSendInProgress();
        }
    }

    /**
     * Dispatch the messages to the Consumers.
     * @return true if you want to trigger a new read.
     * This method is overridden by other classes, please take a look to other implementations
     * if you need to change it.
     */
    protected synchronized boolean trySendMessagesToConsumers(ReadType readType, List<Entry> entries) {
        // 过滤已经删除的消息
        if (needTrimAckedMessages()) {
            cursor.trimDeletedEntries(entries);
        }

        int entriesToDispatch = entries.size();
        // Trigger read more messages
        // 没有要dispatch的消息，直接返回
        if (entriesToDispatch == 0) {
            return true;
        }
        final MessageMetadata[] metadataArray = new MessageMetadata[entries.size()];
        int remainingMessages = 0;
        boolean hasChunk = false;
        for (int i = 0; i < metadataArray.length; i++) {
            final MessageMetadata metadata = Commands.peekAndCopyMessageMetadata(
                    entries.get(i).getDataBuffer(), subscription.toString(), -1);
            if (metadata != null) {
                remainingMessages += metadata.getNumMessagesInBatch();
                if (!hasChunk && metadata.hasUuid()) {
                    hasChunk = true;
                }
            }
            metadataArray[i] = metadata;
        }
        // 如果有chunk消息的情况，则调用另外方法发送
        if (hasChunk) {
            return sendChunkedMessagesToConsumers(readType, entries, metadataArray);
        }

        int start = 0;
        long totalMessagesSent = 0;
        long totalBytesSent = 0;
        long totalEntries = 0;
        // 平均每个batch的消息数量
        int avgBatchSizePerMsg = remainingMessages > 0 ? Math.max(remainingMessages / entries.size(), 1) : 1;

        int firstAvailableConsumerPermits, currentTotalAvailablePermits;
        boolean dispatchMessage;
        while (entriesToDispatch > 0) { // 循环发送
            // 第一个有permits的consumer的permits
            firstAvailableConsumerPermits = getFirstAvailableConsumerPermits();
            // 当前可用的permits
            currentTotalAvailablePermits = Math.max(totalAvailablePermits, firstAvailableConsumerPermits);
            // 判断是否可以发送
            dispatchMessage = currentTotalAvailablePermits > 0 && firstAvailableConsumerPermits > 0;
            if (!dispatchMessage) {
                break;
            }
            // 获取消费者
            Consumer c = getNextConsumer();
            if (c == null) {
                // Do nothing, cursor will be rewind at reconnection
                log.info("[{}] rewind because no available consumer found from total {}", name, consumerList.size());
                entries.subList(start, entries.size()).forEach(Entry::release);
                cursor.rewind();
                return false;
            }

            // round-robin dispatch batch size for this consumer
            // 获取consumer的availablePermits
            int availablePermits = c.isWritable() ? c.getAvailablePermits() : 1;
            if (c.getMaxUnackedMessages() > 0) {
                // Avoid negative number
                int remainUnAckedMessages = Math.max(c.getMaxUnackedMessages() - c.getUnackedMessages(), 0);
                availablePermits = Math.min(availablePermits, remainUnAckedMessages);
            }
            if (log.isDebugEnabled() && !c.isWritable()) {
                log.debug("[{}-{}] consumer is not writable. dispatching only 1 message to {}; "
                                + "availablePermits are {}", topic.getName(), name,
                        c, c.getAvailablePermits());
            }

            // 计算可以发送给consumer的消息数
            int messagesForC = Math.min(Math.min(remainingMessages, availablePermits),
                    serviceConfig.getDispatcherMaxRoundRobinBatchSize());
            messagesForC = Math.max(messagesForC / avgBatchSizePerMsg, 1);

            int end = Math.min(start + messagesForC, entries.size());
            List<Entry> entriesForThisConsumer = entries.subList(start, end);

            // remove positions first from replay list first : sendMessages recycles entries
            // 如果是replay，那么先从redeliveryMessages中移除药发送的消息
            if (readType == ReadType.Replay) {
                entriesForThisConsumer.forEach(entry -> {
                    redeliveryMessages.remove(entry.getLedgerId(), entry.getEntryId());
                });
            }

            SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();

            EntryBatchSizes batchSizes = EntryBatchSizes.get(entriesForThisConsumer.size());
            EntryBatchIndexesAcks batchIndexesAcks = EntryBatchIndexesAcks.get(entriesForThisConsumer.size());
            totalEntries += filterEntriesForConsumer(metadataArray, start,
                    entriesForThisConsumer, batchSizes, sendMessageInfo, batchIndexesAcks, cursor,
                    readType == ReadType.Replay, c);

            c.sendMessages(entriesForThisConsumer, batchSizes, batchIndexesAcks, sendMessageInfo.getTotalMessages(),
                    sendMessageInfo.getTotalBytes(), sendMessageInfo.getTotalChunkedMessages(), redeliveryTracker);

            int msgSent = sendMessageInfo.getTotalMessages();
            remainingMessages -= msgSent;
            start += messagesForC;
            entriesToDispatch -= messagesForC;
            TOTAL_AVAILABLE_PERMITS_UPDATER.addAndGet(this,
                    -(msgSent - batchIndexesAcks.getTotalAckedIndexCount()));
            if (log.isDebugEnabled()) {
                log.debug("[{}] Added -({} minus {}) permits to TOTAL_AVAILABLE_PERMITS_UPDATER in "
                                + "PersistentDispatcherMultipleConsumers",
                        name, msgSent, batchIndexesAcks.getTotalAckedIndexCount());
            }
            totalMessagesSent += sendMessageInfo.getTotalMessages();
            totalBytesSent += sendMessageInfo.getTotalBytes();
        }

        // 请求permits
        acquirePermitsForDeliveredMessages(topic, cursor, totalEntries, totalMessagesSent, totalBytesSent);

        // 跳出上面循环后，如果发现这批entries没有全部发送，那么剩余的entries要添加到redeliveryMessages中
        if (entriesToDispatch > 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] No consumers found with available permits, storing {} positions for later replay", name,
                        entries.size() - start);
            }
            entries.subList(start, entries.size()).forEach(entry -> {
                long stickyKeyHash = getStickyKeyHash(entry);
                addMessageToReplay(entry.getLedgerId(), entry.getEntryId(), stickyKeyHash);
                entry.release();
            });
        }
        return true;
    }

    private boolean sendChunkedMessagesToConsumers(ReadType readType,
                                                   List<Entry> entries,
                                                   MessageMetadata[] metadataArray) {
        final List<EntryAndMetadata> originalEntryAndMetadataList = new ArrayList<>(metadataArray.length);
        for (int i = 0; i < metadataArray.length; i++) {
            originalEntryAndMetadataList.add(EntryAndMetadata.create(entries.get(i), metadataArray[i]));
        }

        final Map<Consumer, List<EntryAndMetadata>> assignResult =
                assignor.assign(originalEntryAndMetadataList, consumerList.size());
        long totalMessagesSent = 0;
        long totalBytesSent = 0;
        long totalEntries = 0;
        final AtomicInteger numConsumers = new AtomicInteger(assignResult.size());
        for (Map.Entry<Consumer, List<EntryAndMetadata>> current : assignResult.entrySet()) {
            final Consumer consumer = current.getKey();
            final List<EntryAndMetadata> entryAndMetadataList = current.getValue();
            final int messagesForC = Math.min(consumer.getAvailablePermits(), entryAndMetadataList.size());
            if (log.isDebugEnabled()) {
                log.debug("[{}] select consumer {} with messages num {}, read type is {}",
                        name, consumer.consumerName(), messagesForC, readType);
            }
            if (messagesForC < entryAndMetadataList.size()) {
                for (int i = messagesForC; i < entryAndMetadataList.size(); i++) {
                    final EntryAndMetadata entry = entryAndMetadataList.get(i);
                    addMessageToReplay(entry);
                    entryAndMetadataList.set(i, null);
                }
            }
            if (messagesForC == 0) {
                numConsumers.decrementAndGet();
                continue;
            }
            if (readType == ReadType.Replay) {
                entryAndMetadataList.stream().limit(messagesForC)
                        .forEach(e -> redeliveryMessages.remove(e.getLedgerId(), e.getEntryId()));
            }
            final SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
            final EntryBatchSizes batchSizes = EntryBatchSizes.get(messagesForC);
            final EntryBatchIndexesAcks batchIndexesAcks = EntryBatchIndexesAcks.get(messagesForC);

            totalEntries += filterEntriesForConsumer(entryAndMetadataList, batchSizes, sendMessageInfo,
                    batchIndexesAcks, cursor, readType == ReadType.Replay, consumer);
            consumer.sendMessages(entryAndMetadataList, batchSizes, batchIndexesAcks,
                    sendMessageInfo.getTotalMessages(), sendMessageInfo.getTotalBytes(),
                    sendMessageInfo.getTotalChunkedMessages(), getRedeliveryTracker()
            ).addListener(future -> {
                if (future.isDone() && numConsumers.decrementAndGet() == 0) {
                    readMoreEntries();
                }
            });

            TOTAL_AVAILABLE_PERMITS_UPDATER.getAndAdd(this,
                    -(sendMessageInfo.getTotalMessages() - batchIndexesAcks.getTotalAckedIndexCount()));
            totalMessagesSent += sendMessageInfo.getTotalMessages();
            totalBytesSent += sendMessageInfo.getTotalBytes();
        }

        acquirePermitsForDeliveredMessages(topic, cursor, totalEntries, totalMessagesSent, totalBytesSent);

        return numConsumers.get() == 0; // trigger a new readMoreEntries() call
    }

    @Override
    public synchronized void readEntriesFailed(ManagedLedgerException exception, Object ctx) {

        ReadType readType = (ReadType) ctx;
        long waitTimeMillis = readFailureBackoff.next();

        if (exception instanceof NoMoreEntriesToReadException) {
            if (cursor.getNumberOfEntriesInBacklog(false) == 0) {
                // Topic has been terminated and there are no more entries to read
                // Notify the consumer only if all the messages were already acknowledged
                checkAndApplyReachedEndOfTopicOrTopicMigration(consumerList);
            }
        } else if (exception.getCause() instanceof TransactionBufferException.TransactionNotSealedException
                || exception.getCause() instanceof ManagedLedgerException.OffloadReadHandleClosedException) {
            waitTimeMillis = 1;
            if (log.isDebugEnabled()) {
                log.debug("[{}] Error reading transaction entries : {}, Read Type {} - Retrying to read in {} seconds",
                        name, exception.getMessage(), readType, waitTimeMillis / 1000.0);
            }
        } else if (!(exception instanceof TooManyRequestsException)) {
            log.error("[{}] Error reading entries at {} : {}, Read Type {} - Retrying to read in {} seconds", name,
                    cursor.getReadPosition(), exception.getMessage(), readType, waitTimeMillis / 1000.0);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Error reading entries at {} : {}, Read Type {} - Retrying to read in {} seconds", name,
                        cursor.getReadPosition(), exception.getMessage(), readType, waitTimeMillis / 1000.0);
            }
        }

        if (shouldRewindBeforeReadingOrReplaying) {
            shouldRewindBeforeReadingOrReplaying = false;
            cursor.rewind();
        }

        if (readType == ReadType.Normal) {
            havePendingRead = false;
        } else {
            havePendingReplayRead = false;
            if (exception instanceof ManagedLedgerException.InvalidReplayPositionException) {
                PositionImpl markDeletePosition = (PositionImpl) cursor.getMarkDeletedPosition();
                redeliveryMessages.removeAllUpTo(markDeletePosition.getLedgerId(), markDeletePosition.getEntryId());
            }
        }

        readBatchSize = serviceConfig.getDispatcherMinReadBatchSize();

        topic.getBrokerService().executor().schedule(() -> {
            synchronized (PersistentDispatcherMultipleConsumers.this) {
                // If it's a replay read we need to retry even if there's already
                // another scheduled read, otherwise we'd be stuck until
                // more messages are published.
                if (!havePendingRead || readType == ReadType.Replay) {
                    log.info("[{}] Retrying read operation", name);
                    readMoreEntries();
                } else {
                    log.info("[{}] Skipping read retry: havePendingRead {}", name, havePendingRead, exception);
                }
            }
        }, waitTimeMillis, TimeUnit.MILLISECONDS);

    }

    private boolean needTrimAckedMessages() {
        if (lastIndividualDeletedRangeFromCursorRecovery == null) {
            return false;
        } else {
            return lastIndividualDeletedRangeFromCursorRecovery.upperEndpoint()
                    .compareTo((PositionImpl) cursor.getReadPosition()) > 0;
        }
    }

    /**
     * returns true only if {@link AbstractDispatcherMultipleConsumers#consumerList}
     * has atleast one unblocked consumer and have available permits.
     *
     * @return
     */
    protected boolean isAtleastOneConsumerAvailable() {
        return getFirstAvailableConsumerPermits() > 0;
    }

    protected int getFirstAvailableConsumerPermits() {
        if (consumerList.isEmpty() || IS_CLOSED_UPDATER.get(this) == TRUE) {
            // abort read if no consumers are connected or if disconnect is initiated
            return 0;
        }
        for (Consumer consumer : consumerList) {
            if (consumer != null && !consumer.isBlocked()) {
                int availablePermits = consumer.getAvailablePermits();
                if (availablePermits > 0) {
                    return availablePermits;
                }
            }
        }
        return 0;
    }

    private boolean isConsumerWritable() {
        for (Consumer consumer : consumerList) {
            if (consumer.isWritable()) {
                return true;
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] consumer is not writable", topic.getName(), name);
        }
        return false;
    }

    @Override
    public boolean isConsumerAvailable(Consumer consumer) {
        return consumer != null && !consumer.isBlocked() && consumer.getAvailablePermits() > 0;
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer, long consumerEpoch) {
        // 主动请求把已经发送出去但是没有ack的消息重新投递，这里会放到redeliveryMessages中，然后调用readMoreEntries读取消息
        consumer.getPendingAcks().forEach((ledgerId, entryId, batchSize, stickyKeyHash) -> {
            if (addMessageToReplay(ledgerId, entryId, stickyKeyHash)) {
                redeliveryTracker.incrementAndGetRedeliveryCount((PositionImpl.get(ledgerId, entryId)));
            }
        });
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Redelivering unacknowledged messages for consumer {}", name, consumer,
                    redeliveryMessages);
        }
        readMoreEntries();
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
        positions.forEach(position -> {
            // TODO: We want to pass a sticky key hash as a third argument to guarantee the order of the messages
            // on Key_Shared subscription, but it's difficult to get the sticky key here
            if (addMessageToReplay(position.getLedgerId(), position.getEntryId())) {
                redeliveryTracker.incrementAndGetRedeliveryCount(position);
            }
        });
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Redelivering unacknowledged messages for consumer {}", name, consumer, positions);
        }
        readMoreEntries();
    }

    @Override
    public void addUnAckedMessages(int numberOfMessages) {
        int maxUnackedMessages = topic.getMaxUnackedMessagesOnSubscription();
        // don't block dispatching if maxUnackedMessages = 0
        if (maxUnackedMessages <= 0 && blockedDispatcherOnUnackedMsgs == TRUE
                && BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.compareAndSet(this, TRUE, FALSE)) {
            log.info("[{}] Dispatcher is unblocked, since maxUnackedMessagesPerSubscription=0", name);
            readMoreEntriesAsync();
        }

        int unAckedMessages = TOTAL_UNACKED_MESSAGES_UPDATER.addAndGet(this, numberOfMessages);
        if (unAckedMessages >= maxUnackedMessages && maxUnackedMessages > 0
                && BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.compareAndSet(this, FALSE, TRUE)) {
            // block dispatcher if it reaches maxUnAckMsg limit
            log.debug("[{}] Dispatcher is blocked due to unackMessages {} reached to max {}", name,
                    unAckedMessages, maxUnackedMessages);
        } else if (topic.getBrokerService().isBrokerDispatchingBlocked()
                && blockedDispatcherOnUnackedMsgs == TRUE) {
            // unblock dispatcher: if dispatcher is blocked due to broker-unackMsg limit and if it ack back enough
            // messages
            if (totalUnackedMessages < (topic.getBrokerService().maxUnackedMsgsPerDispatcher / 2)) {
                if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.compareAndSet(this, TRUE, FALSE)) {
                    // it removes dispatcher from blocked list and unblocks dispatcher by scheduling read
                    topic.getBrokerService().unblockDispatchersOnUnAckMessages(Lists.newArrayList(this));
                }
            }
        } else if (blockedDispatcherOnUnackedMsgs == TRUE && unAckedMessages < maxUnackedMessages / 2) {
            // unblock dispatcher if it acks back enough messages
            if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.compareAndSet(this, TRUE, FALSE)) {
                log.debug("[{}] Dispatcher is unblocked", name);
                readMoreEntriesAsync();
            }
        }
        // increment broker-level count
        topic.getBrokerService().addUnAckedMessages(this, numberOfMessages);
    }

    public boolean isBlockedDispatcherOnUnackedMsgs() {
        return blockedDispatcherOnUnackedMsgs == TRUE;
    }

    public void blockDispatcherOnUnackedMsgs() {
        blockedDispatcherOnUnackedMsgs = TRUE;
    }

    public void unBlockDispatcherOnUnackedMsgs() {
        blockedDispatcherOnUnackedMsgs = FALSE;
    }

    public int getTotalUnackedMessages() {
        return totalUnackedMessages;
    }

    public String getName() {
        return name;
    }

    @Override
    public RedeliveryTracker getRedeliveryTracker() {
        return redeliveryTracker;
    }

    @Override
    public Optional<DispatchRateLimiter> getRateLimiter() {
        return dispatchRateLimiter;
    }

    @Override
    public void updateRateLimiter() {
        if (!initializeDispatchRateLimiterIfNeeded()) {
            this.dispatchRateLimiter.ifPresent(DispatchRateLimiter::updateDispatchRate);
        }
    }

    @Override
    public boolean initializeDispatchRateLimiterIfNeeded() {
        if (!dispatchRateLimiter.isPresent() && DispatchRateLimiter.isDispatchRateEnabled(
                topic.getSubscriptionDispatchRate(getSubscriptionName()))) {
            this.dispatchRateLimiter =
                    Optional.of(new DispatchRateLimiter(topic, getSubscriptionName(), Type.SUBSCRIPTION));
            return true;
        }
        return false;
    }

    @Override
    public boolean trackDelayedDelivery(long ledgerId, long entryId, MessageMetadata msgMetadata) {
        if (!topic.isDelayedDeliveryEnabled()) {
            // If broker has the feature disabled, always deliver messages immediately
            return false;
        }

        synchronized (this) {
            if (!delayedDeliveryTracker.isPresent()) {
                if (!msgMetadata.hasDeliverAtTime()) {
                    // No need to initialize the tracker here
                    return false;
                }

                // Initialize the tracker the first time we need to use it
                delayedDeliveryTracker = Optional
                        .of(topic.getBrokerService().getDelayedDeliveryTrackerFactory().newTracker(this));
            }

            delayedDeliveryTracker.get().resetTickTime(topic.getDelayedDeliveryTickTimeMillis());

            long deliverAtTime = msgMetadata.hasDeliverAtTime() ? msgMetadata.getDeliverAtTime() : -1L;
            return delayedDeliveryTracker.get().addMessage(ledgerId, entryId, deliverAtTime);
        }
    }

    protected synchronized NavigableSet<PositionImpl> getMessagesToReplayNow(int maxMessagesToRead) {
        // 找出延迟发送消息中可发送的消息，添加到redeliveryMessages中
        if (delayedDeliveryTracker.isPresent() && delayedDeliveryTracker.get().hasMessageAvailable()) {
            delayedDeliveryTracker.get().resetTickTime(topic.getDelayedDeliveryTickTimeMillis());
            NavigableSet<PositionImpl> messagesAvailableNow =
                    delayedDeliveryTracker.get().getScheduledMessages(maxMessagesToRead);
            messagesAvailableNow.forEach(p -> redeliveryMessages.add(p.getLedgerId(), p.getEntryId()));
        }

        // 从redeliveryMessages中拿出给定条数的消息
        if (!redeliveryMessages.isEmpty()) {
            return redeliveryMessages.getMessagesToReplayNow(maxMessagesToRead);
        } else {
            return Collections.emptyNavigableSet();
        }
    }

    protected synchronized boolean shouldPauseDeliveryForDelayTracker() {
        return delayedDeliveryTracker.isPresent() && delayedDeliveryTracker.get().shouldPauseAllDeliveries();
    }

    @Override
    public long getNumberOfDelayedMessages() {
        return delayedDeliveryTracker.map(DelayedDeliveryTracker::getNumberOfDelayedMessages).orElse(0L);
    }

    @Override
    public CompletableFuture<Void> clearDelayedMessages() {
        if (!topic.isDelayedDeliveryEnabled()) {
            return CompletableFuture.completedFuture(null);
        }

        if (delayedDeliveryTracker.isPresent()) {
            return this.delayedDeliveryTracker.get().clear();
        } else {
            DelayedDeliveryTrackerFactory delayedDeliveryTrackerFactory =
                    topic.getBrokerService().getDelayedDeliveryTrackerFactory();
            if (delayedDeliveryTrackerFactory instanceof BucketDelayedDeliveryTrackerFactory
                    bucketDelayedDeliveryTrackerFactory) {
                return bucketDelayedDeliveryTrackerFactory.cleanResidualSnapshots(cursor);
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public void cursorIsReset() {
        if (this.lastIndividualDeletedRangeFromCursorRecovery != null) {
            this.lastIndividualDeletedRangeFromCursorRecovery = null;
        }
    }

    private void addMessageToReplay(Entry entry) {
        addMessageToReplay(entry.getLedgerId(), entry.getEntryId());
        entry.release();
    }

    protected boolean addMessageToReplay(long ledgerId, long entryId, long stickyKeyHash) {
        if (checkIfMessageIsUnacked(ledgerId, entryId)) {
            redeliveryMessages.add(ledgerId, entryId, stickyKeyHash);
            return true;
        } else {
            return false;
        }
    }

    protected boolean addMessageToReplay(long ledgerId, long entryId) {
        if (checkIfMessageIsUnacked(ledgerId, entryId)) {
            redeliveryMessages.add(ledgerId, entryId);
            return true;
        } else {
            return false;
        }
    }

    private boolean checkIfMessageIsUnacked(long ledgerId, long entryId) {
        Position markDeletePosition = cursor.getMarkDeletedPosition();
        return (markDeletePosition == null || ledgerId > markDeletePosition.getLedgerId()
                || (ledgerId == markDeletePosition.getLedgerId() && entryId > markDeletePosition.getEntryId()));
    }

    @Override
    public boolean checkAndUnblockIfStuck() {
        if (cursor.checkAndUpdateReadPositionChanged()) {
            return false;
        }
        // consider dispatch is stuck if : dispatcher has backlog, available-permits and there is no pending read
        if (totalAvailablePermits > 0 && !havePendingReplayRead && !havePendingRead
                && cursor.getNumberOfEntriesInBacklog(false) > 0) {
            log.warn("{}-{} Dispatcher is stuck and unblocking by issuing reads", topic.getName(), name);
            readMoreEntries();
            return true;
        }
        return false;
    }

    public PersistentTopic getTopic() {
        return topic;
    }


    public long getDelayedTrackerMemoryUsage() {
        return delayedDeliveryTracker.map(DelayedDeliveryTracker::getBufferMemoryUsage).orElse(0L);
    }

    public Map<String, TopicMetricBean> getBucketDelayedIndexStats() {
        if (delayedDeliveryTracker.isEmpty()) {
            return Collections.emptyMap();
        }

        if (delayedDeliveryTracker.get() instanceof BucketDelayedDeliveryTracker) {
            return ((BucketDelayedDeliveryTracker) delayedDeliveryTracker.get()).genTopicMetricMap();
        }

        return Collections.emptyMap();
    }

    public ManagedCursor getCursor() {
        return cursor;
    }

    protected int getStickyKeyHash(Entry entry) {
        return StickyKeyConsumerSelector.makeStickyKeyHash(peekStickyKey(entry.getDataBuffer()));
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentDispatcherMultipleConsumers.class);
}
