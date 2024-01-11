package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.RecoverResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RecoverResponse extends AbstractResponse {
    private final RecoverResponseData data;

    public RecoverResponse(RecoverResponseData data) {
        super(ApiKeys.PREPARE_TXN);
        this.data = data;
    }

    @Override
    public int throttleTimeMs() {
        return 0;
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error());
    }

    public List<Xid> transactionsIds() {
        return data.transactions().stream()
                .map(t -> {
                    Xid xid = new Xid();
                    xid.format = t.externalTxnIdFormat();
                    xid.globalTxnId = t.externalTxnId();
                    xid.branchTxnId = t.externalTxnBranchId();
                    return xid;})
                .collect(Collectors.toList());
    }

    @Override
    public RecoverResponseData data() {
        return data;
    }

    public static RecoverResponse parse(ByteBuffer buffer, short version) {
        return new RecoverResponse(new RecoverResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }

    public static class Xid {
        public int format;
        public byte[] globalTxnId;
        public byte[] branchTxnId;
    }
}
