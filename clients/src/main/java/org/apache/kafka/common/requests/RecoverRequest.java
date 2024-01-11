package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.PrepareTxnResponseData;
import org.apache.kafka.common.message.RecoverRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class RecoverRequest extends AbstractRequest{
    private final RecoverRequestData data;

    public static class Builder extends AbstractRequest.Builder<RecoverRequest> {
        public final RecoverRequestData data;

        public Builder(RecoverRequestData data) {
            super(ApiKeys.RECOVER);
            this.data = data;
        }

        @Override
        public RecoverRequest build(short version) {
            return new RecoverRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private RecoverRequest(RecoverRequestData data, short version) {
        super(ApiKeys.RECOVER, version);
        this.data = data;
    }

    @Override
    public RecoverRequestData data() {
        return data;
    }

    @Override
    public PrepareTxnResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new PrepareTxnResponse(new PrepareTxnResponseData()
                .setErrorCode(Errors.forException(e).code())
                .setThrottleTimeMs(throttleTimeMs)
        );
    }

    public static RecoverRequest parse(ByteBuffer buffer, short version) {
        return new RecoverRequest(new RecoverRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
