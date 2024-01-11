package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.ListTransactionsResult;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class XAKafkaProducer<K, V> extends KafkaProducer<K, V> implements XAProducer<K, V> {

    private final Admin adminClient;

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param configs   The producer configs
     *
     */
    public XAKafkaProducer(final Map<String, Object> configs) {
        this(configs, null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * Values can be either strings or Objects of the appropriate type (for example a numeric configuration would accept
     * either the string "42" or the integer 42).
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param configs   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public XAKafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(new ProducerConfig(ProducerConfig.appendSerializerToConfig(configs, keySerializer, valueSerializer)),
                keySerializer, valueSerializer, null, null, null, Time.SYSTEM);
        Properties configMap = new Properties();
        configMap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, configs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        configMap.put(AdminClientConfig.CLIENT_ID_CONFIG, configs.get(ProducerConfig.CLIENT_ID_CONFIG));
        this.adminClient = AdminClient.create(configMap);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     */
    public XAKafkaProducer(Properties properties) {
        this(properties, null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public XAKafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(Utils.propsToMap(properties), keySerializer, valueSerializer);
    }

    @Override
    public Object getTransactionManager() {
        return transactionManager.get();
    }

    @Override
    public Object resetTransactionManager(int txnIdFormat, byte[] globalTxnId, byte[] branchQualifier) {
        return resetTransactionManager(
                virtualProducerId(globalTxnId, branchQualifier), transactionalId(txnIdFormat, globalTxnId, branchQualifier));
    }

    @Override
    public void unlinkTransactionManager() {
        flush(); // Make sure that all records have been sent to the broker before we remove the tm.
        transactionManager.clear();
    }

    @Override
    public void linkTransactionManager(Object tm) {
        transactionManager.set((TransactionManager) tm);
    }

    @Override
    public void prepareTransaction(int txnIdFormat, byte[] globalTxnId, byte[] branchQualifier) {
        super.internalPrepareTransaction(txnIdFormat, globalTxnId, branchQualifier,
                virtualProducerId(globalTxnId, branchQualifier), transactionalId(txnIdFormat, globalTxnId, branchQualifier));
    }

    @Override
    public void commitTransaction(int txnIdFormat, byte[] globalTxnId, byte[] branchQualifier) {
        super.internalCommitTransaction(
                virtualProducerId(globalTxnId, branchQualifier), transactionalId(txnIdFormat, globalTxnId, branchQualifier));
    }

    @Override
    public void rollbackTransaction(int txnIdFormat, byte[] globalTxnId, byte[] branchQualifier) {
        super.internalAbortTransaction(
                virtualProducerId(globalTxnId, branchQualifier), transactionalId(txnIdFormat, globalTxnId, branchQualifier));
    }

    public static String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for(byte b: a)
            sb.append(String.format("%02x", b));
        return sb.toString();
    }

    public static byte[] hexToByteArray(String s) {
        byte[] bb = new byte[s.length()/2];
        StringBuilder sb = new StringBuilder(2);
        int idx = 0;
        for(char c: s.toCharArray()) {
            if (sb.length() == 0) {
                sb.append(c);
            } else {
                sb.append(c);
                bb[idx++] = (byte) Integer.parseInt(sb.toString(), 16);
                sb.delete(0,2);
            }
        }
        return bb;
    }

    private static long virtualProducerId(byte[] globalTxnId, byte[] branchQualifier) {
        int globalTxnIdHash = Arrays.hashCode(globalTxnId);
        int branchQualifierHash = Arrays.hashCode(branchQualifier);
        long virtualProducerId = ((long) globalTxnIdHash) << 32 | ((long) branchQualifierHash);
        return Math.abs(virtualProducerId);
    }

    public static String transactionalId(int formatId, byte[] globalTxnId, byte[] branchQualifier) {
        return Integer.toString(formatId, 16)+":"+byteArrayToHex(globalTxnId)+":"+byteArrayToHex(branchQualifier);
    }

    public static ExternalTxnData transactionDataFromTransactionId(String transactionalId) {
        String[] part = transactionalId.split(":");
        ExternalTxnData rtrn = new ExternalTxnData();
        rtrn.formatId = Integer.parseInt(part[0], 16);
        rtrn.globalTransactionId = hexToByteArray(part[1]);
        rtrn.branchQualifier = hexToByteArray(part[2]);
        return rtrn;
    }

    @Override
    public Collection<ExternalTxnData> recover() throws InterruptedException {
        ListTransactionsOptions options = new ListTransactionsOptions();
        List<TransactionState> statesFilter = new LinkedList<>();
        statesFilter.add(TransactionState.PREPARED);
        options.filterStates(statesFilter);
        ListTransactionsResult result = adminClient.listTransactions(options);
        try {
            return result.all().get().stream()
                    .map(tl -> transactionDataFromTransactionId(tl.transactionalId()))
                    .collect(Collectors.toList());
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
