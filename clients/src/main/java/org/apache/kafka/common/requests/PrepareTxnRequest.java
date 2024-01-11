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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.PrepareTxnRequestData;
import org.apache.kafka.common.message.PrepareTxnResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class PrepareTxnRequest extends AbstractRequest {
    private final PrepareTxnRequestData data;

    public static class Builder extends AbstractRequest.Builder<PrepareTxnRequest> {
        public final PrepareTxnRequestData data;

        public Builder(PrepareTxnRequestData data) {
            super(ApiKeys.PREPARE_TXN);
            this.data = data;
        }

        @Override
        public PrepareTxnRequest build(short version) {
            return new PrepareTxnRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private PrepareTxnRequest(PrepareTxnRequestData data, short version) {
        super(ApiKeys.END_TXN, version);
        this.data = data;
    }

    @Override
    public PrepareTxnRequestData data() {
        return data;
    }

    @Override
    public PrepareTxnResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new PrepareTxnResponse(new PrepareTxnResponseData()
                .setErrorCode(Errors.forException(e).code())
                .setThrottleTimeMs(throttleTimeMs)
        );
    }

    public static PrepareTxnRequest parse(ByteBuffer buffer, short version) {
        return new PrepareTxnRequest(new PrepareTxnRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
