package at.ac.fhsalzburg.bde.app;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class DummyConsumer implements Consumer<Long, String> {
    public final Map<TopicPartition, List<ConsumerRecord<Long, String>>>  recordsMap;
    public final ConsumerRecords<Long, String> allRecords;

    public int pollCalledCount = 0;
    public int valueCalledCount = 0;

    public DummyConsumer() {
        recordsMap = new HashMap<>();
        final List<ConsumerRecord<Long, String>> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            records.add(new ConsumerRecord<Long, String>(SpotifyProducer.TOPIC, 0, i, (long) i, String.valueOf((char) (65 + i))) {
                @Override
                public String value() {
                    valueCalledCount++;
                    return super.value();
                }
            });
        }
        recordsMap.put(new TopicPartition(SpotifyProducer.TOPIC, 0), records);
        allRecords = new ConsumerRecords<>(recordsMap);
    }

    @Override
    public Set<TopicPartition> assignment() {
        return null;
    }

    @Override
    public Set<String> subscription() {
        return null;
    }

    @Override
    public void subscribe(Collection<String> topics) {

    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {

    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {

    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {

    }

    @Override
    public void subscribe(Pattern pattern) {

    }

    @Override
    public void unsubscribe() {

    }

    @Override
    public ConsumerRecords<Long, String> poll(long timeout) {
        try {
            pollCalledCount++;
            Thread.sleep(timeout);
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
        return allRecords;
    }

    @Override
    public ConsumerRecords<Long, String> poll(Duration timeout) {
        return poll(timeout.toMillis());
    }

    @Override
    public void commitSync() {

    }

    @Override
    public void commitSync(Duration timeout) {

    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {

    }

    @Override
    public void commitAsync() {

    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {

    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {

    }

    @Override
    public void seek(TopicPartition partition, long offset) {

    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {

    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {

    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {

    }

    @Override
    public long position(TopicPartition partition) {
        return 0;
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        return 0;
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return null;
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return null;
    }

    @Override
    public Set<TopicPartition> paused() {
        return null;
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {

    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {

    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return null;
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void close(long timeout, TimeUnit unit) {

    }

    @Override
    public void close(Duration timeout) {

    }

    @Override
    public void wakeup() {

    }
}
