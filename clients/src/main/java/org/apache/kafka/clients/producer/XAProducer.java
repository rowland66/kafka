package org.apache.kafka.clients.producer;

import java.util.Collection;

public interface XAProducer<K, V> extends Producer<K, V> {

    class ExternalTxnData {
        public int formatId;
        public byte[] globalTransactionId;
        public byte[] branchQualifier;
    }

    Object getTransactionManager();

    Object resetTransactionManager(int txnIdFormat, byte[] globalTxnId, byte[] branchQualifier);

    void unlinkTransactionManager();

    void linkTransactionManager(Object tm);

    void prepareTransaction(int txnIdFormat, byte[] globalTxnId, byte[] branchQualifier);

    void commitTransaction(int txnIdFormat, byte[] globalTxnId, byte[] branchQualifier);

    void rollbackTransaction(int txnIdFormat, byte[] globalTxnId, byte[] branchQualifier);

    Collection<ExternalTxnData> recover() throws InterruptedException;
}
