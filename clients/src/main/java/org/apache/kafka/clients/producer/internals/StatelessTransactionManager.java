package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.ListTransactionsResult;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.admin.internals.AdminApiHandler;
import org.apache.kafka.clients.admin.internals.AdminApiLookupStrategy;
import org.apache.kafka.clients.admin.internals.AllBrokersStrategy;
import org.apache.kafka.clients.admin.internals.ListTransactionsHandler;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class StatelessTransactionManager {
    private final Logger log;
    private final int transactionTimeoutMs;
    private final ApiVersions apiVersions;
    private final PriorityQueue<StatelessTransactionManager.TxnRequestHandler> pendingRequests;

    // This is used by the TxnRequestHandlers to control how long to back off before a given request is retried.
    // For instance, this value is lowered by the AddPartitionsToTxnHandler when it receives a CONCURRENT_TRANSACTIONS
    // error for the first AddPartitionsRequest in a transaction.
    private final long retryBackoffMs;

    // A map from an external transaction id to a transaction coordinator node for the transaction.
    // TODO: figure out how to expire items from this map
    private static final Map<String, Node> transactionCoordinatorMap = new HashMap<>();

    // We use the priority to determine the order in which requests need to be sent out. For instance, if we have
    // a pending FindCoordinator request, that must always go first. Next, If we need a producer id, that must go second.
    // The endTxn request must always go last, unless we are bumping the epoch (a special case of InitProducerId) as
    // part of ending the transaction.
    private enum Priority {
        FIND_COORDINATOR(0),
        PREPARE_TXN(1),
        END_TXN(2);
        final int priority;

        Priority(int priority) {
            this.priority = priority;
        }
    }

    public StatelessTransactionManager(final LogContext logContext,
                                       final int transactionTimeoutMs,
                                       final long retryBackoffMs,
                                       final ApiVersions apiVersions) {
        this.log = logContext.logger(this.getClass());
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.apiVersions = apiVersions;
        this.pendingRequests = new PriorityQueue<>(10, Comparator.comparingInt(o -> o.priority().priority));
    }

    static synchronized void addTransactionCoordinatorMapping(String transactionId, Node transactionCoordinator) {
        transactionCoordinatorMap.put(transactionId, transactionCoordinator);
    }

    static Node getTransactionCoordinatorForTransaction(String transactionId) {
        return transactionCoordinatorMap.get(transactionId);
    }

    static synchronized void removeTransactionCoordinatorMapping(String transactionId) {
        transactionCoordinatorMap.remove(transactionId);
    }

    private synchronized void enqueueRequest(StatelessTransactionManager.TxnRequestHandler requestHandler) {
        log.debug("Enqueuing transactional request {}", requestHandler.requestBuilder());
        pendingRequests.add(requestHandler);
    }

    synchronized StatelessTransactionManager.TxnRequestHandler nextRequest() {
        TxnRequestHandler nextRequestHandler = pendingRequests.poll();
        if (nextRequestHandler != null)
            log.trace("Request {} dequeued for sending", nextRequestHandler.requestBuilder());

        return nextRequestHandler;
    }

    void lookupCoordinator(StatelessTransactionManager.TxnRequestHandler request) {
        lookupCoordinator(request.coordinatorKey());
    }

    private void lookupCoordinator(String coordinatorKey) {

        FindCoordinatorRequestData data = new FindCoordinatorRequestData()
                .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id())
                .setKey(coordinatorKey);
        FindCoordinatorRequest.Builder builder = new FindCoordinatorRequest.Builder(data);
        enqueueRequest(new StatelessTransactionManager.FindCoordinatorHandler(builder, coordinatorKey));
    }

    synchronized void retry(StatelessTransactionManager.TxnRequestHandler request) {
        request.setRetry();
        enqueueRequest(request);
    }

    public TransactionalRequestResult beginPrepare(long producerId, String transactionalId) {
        PrepareTxnRequest.Builder builder = new PrepareTxnRequest.Builder(
                new PrepareTxnRequestData()
                        .setTransactionalId(transactionalId)
                        .setProducerId(producerId)
                        .setProducerEpoch((short) 0));

        PrepareTxnHandler handler = new PrepareTxnHandler(builder);
        enqueueRequest(handler);
        return handler.result;
    }

    public synchronized TransactionalRequestResult beginCommit(long producerId, String transactionalId) {
        EndTxnRequest.Builder builder = new EndTxnRequest.Builder(
                new EndTxnRequestData()
                        .setTransactionalId(transactionalId)
                        .setProducerId(producerId)
                        .setProducerEpoch((short) 0)
                        .setCommitted(true));

        StatelessTransactionManager.EndTxnHandler handler = new StatelessTransactionManager.EndTxnHandler(builder);
        enqueueRequest(handler);
        return handler.result;
    }

    public synchronized TransactionalRequestResult beginAbort(long producerId, String transactionalId) {
        EndTxnRequest.Builder builder = new EndTxnRequest.Builder(
                new EndTxnRequestData()
                        .setTransactionalId(transactionalId)
                        .setProducerId(producerId)
                        .setProducerEpoch((short) 0)
                        .setCommitted(false));

        StatelessTransactionManager.EndTxnHandler handler = new StatelessTransactionManager.EndTxnHandler(builder);
        enqueueRequest(handler);
        return handler.result;
    }

    abstract class TxnRequestHandler implements RequestCompletionHandler {
        protected final TransactionalRequestResult result;
        private boolean isRetry = false;

        TxnRequestHandler(TransactionalRequestResult result) {
            this.result = result;
        }

        TxnRequestHandler(String operation) {
            this(new TransactionalRequestResult(operation));
        }

        void fatalError(RuntimeException e) {
            result.fail(e);
        }

        void abortableError(RuntimeException e) {
            result.fail(e);
        }

        void fail(RuntimeException e) {
            result.fail(e);
        }

        void reenqueue() {
            synchronized (StatelessTransactionManager.this) {
                this.isRetry = true;
                enqueueRequest(this);
            }
        }

        long retryBackoffMs() {
            return retryBackoffMs;
        }

        @Override
        public void onComplete(ClientResponse response) {
            if (response.wasDisconnected()) {
                log.debug("Disconnected from {}. Will retry.", response.destination());
                if (this.needsCoordinator())
                    lookupCoordinator(this.coordinatorKey());
                reenqueue();
            } else if (response.versionMismatch() != null) {
                fatalError(response.versionMismatch());
            } else if (response.hasResponse()) {
                log.info("Received stateless transactional response {} for request {}", response.responseBody(),
                        requestBuilder());
                synchronized (StatelessTransactionManager.this) {
                    handleResponse(response.responseBody());
                }
            } else {
                fatalError(new KafkaException("Could not execute stateless transactional request for unknown reasons"));
            }
        }

        boolean needsCoordinator() {
            return coordinatorType() != null;
        }

        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            return FindCoordinatorRequest.CoordinatorType.TRANSACTION;
        }

        abstract String coordinatorKey();

        void setRetry() {
            this.isRetry = true;
        }

        boolean isRetry() {
            return isRetry;
        }

        boolean isPrepareTxn() {
            return false;
        }

        boolean isEndTxn() {
            return false;
        }

        abstract AbstractRequest.Builder<?> requestBuilder();

        abstract void handleResponse(AbstractResponse responseBody);

        abstract StatelessTransactionManager.Priority priority();
    }

    private class FindCoordinatorHandler extends StatelessTransactionManager.TxnRequestHandler {
        private final FindCoordinatorRequest.Builder builder;

        private final String transactionId;

        private FindCoordinatorHandler(FindCoordinatorRequest.Builder builder, String transactionId) {
            super("FindCoordinator");
            this.builder = builder;
            this.transactionId = transactionId;
        }

        @Override
        FindCoordinatorRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        StatelessTransactionManager.Priority priority() {
            return StatelessTransactionManager.Priority.FIND_COORDINATOR;
        }

        @Override
        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            return null;
        }

        @Override
        String coordinatorKey() {
            return null;
        }


        @Override
        public void handleResponse(AbstractResponse response) {
            FindCoordinatorRequest.CoordinatorType coordinatorType = FindCoordinatorRequest.CoordinatorType.forId(builder.data().keyType());

            List<FindCoordinatorResponseData.Coordinator> coordinators = ((FindCoordinatorResponse) response).coordinators();
            if (coordinators.size() != 1) {
                log.error("Group coordinator lookup failed: Invalid response containing more than a single coordinator");
                fatalError(new IllegalStateException("Group coordinator lookup failed: Invalid response containing more than a single coordinator"));
            }
            FindCoordinatorResponseData.Coordinator coordinatorData = coordinators.get(0);
            // For older versions without batching, obtain key from request data since it is not included in response
            String key = coordinatorData.key() == null ? builder.data().key() : coordinatorData.key();
            Errors error = Errors.forCode(coordinatorData.errorCode());
            if (error == Errors.NONE) {
                Node node = new Node(coordinatorData.nodeId(), coordinatorData.host(), coordinatorData.port());
                StatelessTransactionManager.addTransactionCoordinatorMapping(transactionId, node);
                result.done();
                log.info("Discovered {} coordinator {}", coordinatorType.toString().toLowerCase(Locale.ROOT), node);
            } else if (error.exception() instanceof RetriableException) {
                reenqueue();
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                fatalError(error.exception());
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                abortableError(GroupAuthorizationException.forGroupId(key));
            } else {
                fatalError(new KafkaException(String.format("Could not find a coordinator with type %s with key %s due to " +
                                "unexpected error: %s", coordinatorType, key,
                        coordinatorData.errorMessage())));
            }
        }
    }

    private class PrepareTxnHandler extends StatelessTransactionManager.TxnRequestHandler {
        private final PrepareTxnRequest.Builder builder;

        private PrepareTxnHandler(PrepareTxnRequest.Builder builder) {
            super("PrepareTxn()");
            this.builder = builder;
        }

        @Override
        PrepareTxnRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        StatelessTransactionManager.Priority priority() {
            return StatelessTransactionManager.Priority.PREPARE_TXN;
        }

        @Override
        String coordinatorKey() {
            return builder.data.transactionalId();
        }

        @Override
        boolean isPrepareTxn() {
            return true;
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            PrepareTxnResponse prepareTxnResponse = (PrepareTxnResponse) response;
            Errors error = prepareTxnResponse.error();

            if (error == Errors.NONE) {
                result.done();
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                lookupCoordinator(builder.data.transactionalId());
                reenqueue();
            } else if (error.exception() instanceof RetriableException) {
                reenqueue();
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                fatalError(error.exception());
            } else if (error == Errors.INVALID_TXN_STATE) {
                fatalError(error.exception());
            } else if (error == Errors.UNKNOWN_PRODUCER_ID || error == Errors.INVALID_PRODUCER_ID_MAPPING) {
                fatalError(error.exception());
            } else {
                log.info("Unexpected error in PrepareTxnResponse for transactionalId: " +
                        builder.data.transactionalId() +
                        " - " + error.message());
                result.done();
            }
        }
    }

    private class EndTxnHandler extends StatelessTransactionManager.TxnRequestHandler {
        private final EndTxnRequest.Builder builder;

        private EndTxnHandler(EndTxnRequest.Builder builder) {
            super("EndTxn(" + builder.data.committed() + ")");
            this.builder = builder;
        }

        @Override
        EndTxnRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        StatelessTransactionManager.Priority priority() {
            return StatelessTransactionManager.Priority.END_TXN;
        }

        @Override
        boolean isEndTxn() {
            return true;
        }

        @Override
        String coordinatorKey() {
            return builder.data.transactionalId();
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            EndTxnResponse endTxnResponse = (EndTxnResponse) response;
            Errors error = endTxnResponse.error();

            if (error == Errors.NONE) {
                result.done();
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                lookupCoordinator(builder.data.transactionalId());
                reenqueue();
            } else if (error.exception() instanceof RetriableException) {
                reenqueue();
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                fatalError(error.exception());
            } else if (error == Errors.INVALID_TXN_STATE) {
                fatalError(error.exception());
            } else if (error == Errors.UNKNOWN_PRODUCER_ID || error == Errors.INVALID_PRODUCER_ID_MAPPING) {
                fatalError(error.exception());
            } else {
                fatalError(new KafkaException("Unhandled error in EndTxnResponse: " +
                        builder.data.transactionalId() +
                        " - " + error.message()));
            }
        }
    }
}
