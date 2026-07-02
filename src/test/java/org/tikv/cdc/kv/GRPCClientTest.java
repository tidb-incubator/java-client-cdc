/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.tikv.cdc.kv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.ChangeDataGrpc;
import org.tikv.shade.io.grpc.Grpc;
import org.tikv.shade.io.grpc.InsecureServerCredentials;
import org.tikv.shade.io.grpc.ManagedChannel;
import org.tikv.shade.io.grpc.ManagedChannelBuilder;
import org.tikv.shade.io.grpc.Server;
import org.tikv.shade.io.grpc.Status;
import org.tikv.shade.io.grpc.StatusRuntimeException;
import org.tikv.shade.io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test case for GRPCClient using a real gRPC server to test retry logic without Mockito. */
public class GRPCClientTest {
    private static final Logger LOG = LoggerFactory.getLogger(GRPCClientTest.class);
    private static final int DEFAULT_PORT = 50051;
    private static Server server;
    private static String serverAddress = "localhost:" + DEFAULT_PORT;
    private static TestChangeDataServiceImpl serviceImpl;

    @BeforeClass
    public static void setupServer() throws IOException {
        serviceImpl = new TestChangeDataServiceImpl();
        server =
                Grpc.newServerBuilderForPort(DEFAULT_PORT, InsecureServerCredentials.create())
                        .addService(serviceImpl)
                        .build()
                        .start();
        LOG.info("Server started, listening on {}", DEFAULT_PORT);
    }

    @AfterClass
    public static void stopServer() {
        if (server != null) {
            server.shutdown();
            try {
                if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
            } catch (InterruptedException e) {
                server.shutdownNow();
            }
        }
    }

    /** Test the retry logic of GRPCClient when the server returns errors. */
    @Test(timeout = 300000)
    public void testRetryLogic() throws InterruptedException {
        // Reset service state
        serviceImpl.reset();
        // Configure the server to fail the first two requests, then succeed
        serviceImpl.setFailCount(2);

        // Create a ManagedChannel to connect to the test server
        ManagedChannel channel =
                ManagedChannelBuilder.forTarget(serverAddress).usePlaintext().build();

        // Create a ScheduledExecutorService and user Executor
        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
        ExecutorService userExecutor = Executors.newSingleThreadExecutor();

        // Create GRPCClient
        GRPCClient client = new GRPCClient(channel, ses, userExecutor, 1000,0);

        // Test response observer
        final CountDownLatch successLatch = new CountDownLatch(1);
        final AtomicInteger retryCount = new AtomicInteger(0);
        final AtomicReference<Throwable> errorRef = new AtomicReference<>();

        GRPCClient.ResilientResponseObserver<Cdcpb.ChangeDataRequest, Cdcpb.ChangeDataEvent>
                responseObserver =
                        new GRPCClient.ResilientResponseObserver<
                                Cdcpb.ChangeDataRequest, Cdcpb.ChangeDataEvent>() {
                            @Override
                            public void onNext(Cdcpb.ChangeDataEvent value) {
                                LOG.info("Received response: {}", value);
                                successLatch.countDown();
                            }

                            @Override
                            public void onError(Throwable t) {
                                LOG.error("Error received", t);
                                errorRef.set(t);
                                successLatch.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                LOG.info("Completed");
                                successLatch.countDown();
                            }

                            @Override
                            public void onEstablished() {
                                LOG.info("Stream established");
                                client.requestManual(1);
                            }

                            @Override
                            public void onReplaced(
                                    StreamObserver<Cdcpb.ChangeDataRequest>
                                            newStreamRequestObserver) {
                                LOG.info(
                                        "Stream replaced, retry count: {}",
                                        retryCount.incrementAndGet());
                                // Send a new request on the replaced stream
                                Cdcpb.ChangeDataRequest request = createTestRequest();
                                newStreamRequestObserver.onNext(request);
                            }
                        };

        try {
            // Start the stream and send a request
            StreamObserver<Cdcpb.ChangeDataRequest> requestStream =
                    client.callStream(
                            ChangeDataGrpc.getEventFeedMethod(), responseObserver, userExecutor);
            Cdcpb.ChangeDataRequest request = createTestRequest();
            requestStream.onNext(request);

            // Wait for the test to complete
            successLatch.await(100, TimeUnit.SECONDS);

            // Verify that retries happened and we eventually succeeded
            assertEquals("Should have retried 2 times", 2, retryCount.get());
            assertTrue("Should not have received an error", errorRef.get() == null);
            assertEquals(
                    "Server should have received 3 requests (original + 2 retries)",
                    3,
                    serviceImpl.getRequestCount());
        } finally {
            // Clean up resources
            channel.shutdown();
            ses.shutdown();
            userExecutor.shutdown();
        }
    }

    /** Test the maximum retry count behavior. */
    @Test(timeout = 30000)
    @Ignore("The CDC stream intentionally retries indefinitely; no SDK max-retry policy exists")
    public void testMaxRetryCountBehavior() throws InterruptedException {
        // Reset service state
        serviceImpl.reset();
        // Configure the server to always fail
        serviceImpl.setFailCount(Integer.MAX_VALUE);

        // Create a ManagedChannel to connect to the test server
        ManagedChannel channel =
                ManagedChannelBuilder.forTarget(serverAddress).usePlaintext().build();

        // Create a ScheduledExecutorService and user Executor
        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
        ExecutorService userExecutor = Executors.newSingleThreadExecutor();

        // Create GRPCClient
        GRPCClient client = new GRPCClient(channel, ses, userExecutor, 1000,0);

        // Test response observer
        final CountDownLatch errorLatch = new CountDownLatch(1);
        final AtomicInteger retryCount = new AtomicInteger(0);
        final AtomicReference<Throwable> errorRef = new AtomicReference<>();

        GRPCClient.ResilientResponseObserver<Cdcpb.ChangeDataRequest, Cdcpb.ChangeDataEvent>
                responseObserver =
                        new GRPCClient.ResilientResponseObserver<
                                Cdcpb.ChangeDataRequest, Cdcpb.ChangeDataEvent>() {
                            @Override
                            public void onNext(Cdcpb.ChangeDataEvent value) {
                                LOG.info("Received response: {}", value);
                            }

                            @Override
                            public void onError(Throwable t) {
                                LOG.error("Error received after retries", t);
                                errorRef.set(t);
                                errorLatch.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                LOG.info("Completed");
                            }

                            @Override
                            public void onEstablished() {
                                LOG.info("Stream established");
                                client.requestManual(1);
                            }

                            @Override
                            public void onReplaced(
                                    StreamObserver<Cdcpb.ChangeDataRequest>
                                            newStreamRequestObserver) {
                                int count = retryCount.incrementAndGet();
                                LOG.info("Stream replaced, retry count: {}", count);
                                // Stop retrying after 5 attempts to prevent infinite loop
                                if (count < 5) {
                                    Cdcpb.ChangeDataRequest request = createTestRequest();
                                    newStreamRequestObserver.onNext(request);
                                } else {
                                    errorLatch.countDown();
                                }
                            }
                        };

        try {
            // Start the stream and send a request
            StreamObserver<Cdcpb.ChangeDataRequest> requestStream =
                    client.callStream(
                            ChangeDataGrpc.getEventFeedMethod(), responseObserver, userExecutor);
            Cdcpb.ChangeDataRequest request = createTestRequest();
            requestStream.onNext(request);

            // Wait for the test to complete
            errorLatch.await(20, TimeUnit.SECONDS);

            // Verify that retries happened as expected
            assertTrue("Should have retried at least 4 times", retryCount.get() >= 4);
            assertTrue("Retry callback should not report a terminal error", errorRef.get() == null);
        } finally {
            // Clean up resources
            channel.shutdown();
            ses.shutdown();
            userExecutor.shutdown();
        }
    }

    /** Test the retry logic for non-retryable errors. */
    @Test(timeout = 30000)
    public void testNonRetryableErrors() throws InterruptedException {
        // Reset service state
        serviceImpl.reset();
        // Configure the server to return a non-retryable error
        serviceImpl.setReturnNonRetryableError(true);

        // Create a ManagedChannel to connect to the test server
        ManagedChannel channel =
                ManagedChannelBuilder.forTarget(serverAddress).usePlaintext().build();

        // Create a ScheduledExecutorService and user Executor
        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
        ExecutorService userExecutor = Executors.newSingleThreadExecutor();

        // Create GRPCClient
        GRPCClient client = new GRPCClient(channel, ses, userExecutor, 1000,0);

        // Test response observer
        final CountDownLatch errorLatch = new CountDownLatch(1);
        final AtomicInteger retryCount = new AtomicInteger(0);
        final AtomicReference<Throwable> errorRef = new AtomicReference<>();

        GRPCClient.ResilientResponseObserver<Cdcpb.ChangeDataRequest, Cdcpb.ChangeDataEvent>
                responseObserver =
                        new GRPCClient.ResilientResponseObserver<
                                Cdcpb.ChangeDataRequest, Cdcpb.ChangeDataEvent>() {
                            @Override
                            public void onNext(Cdcpb.ChangeDataEvent value) {
                                LOG.info("Received response: {}", value);
                            }

                            @Override
                            public void onError(Throwable t) {
                                LOG.error("Error received", t);
                                errorRef.set(t);
                                errorLatch.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                LOG.info("Completed");
                            }

                            @Override
                            public void onEstablished() {
                                LOG.info("Stream established");
                                client.requestManual(1);
                            }

                            @Override
                            public void onReplaced(
                                    StreamObserver<Cdcpb.ChangeDataRequest>
                                            newStreamRequestObserver) {
                                LOG.info(
                                        "Stream replaced, retry count: {}",
                                        retryCount.incrementAndGet());
                            }
                        };

        try {
            // Start the stream and send a request
            StreamObserver<Cdcpb.ChangeDataRequest> requestStream =
                    client.callStream(
                            ChangeDataGrpc.getEventFeedMethod(), responseObserver, userExecutor);
            Cdcpb.ChangeDataRequest request = createTestRequest();
            requestStream.onNext(request);

            // Wait for the test to complete
            errorLatch.await(5, TimeUnit.SECONDS);

            // Verify that no retries happened for non-retryable error
            assertEquals("Should not have retried", 0, retryCount.get());
            assertTrue("Should have received an error", errorRef.get() != null);
        } finally {
            // Clean up resources
            channel.shutdown();
            ses.shutdown();
            userExecutor.shutdown();
        }
    }

    /** Test the delay calculation after failures. */
    @Test
    public void testDelayAfterFailureMs() {
        // First attempt: no delay
        assertEquals("First attempt should have 0 delay", 0L, GRPCClient.delayAfterFailureMs(1));

        // Second attempt: random delay between 500-1000ms
        long delayForSecondAttempt = GRPCClient.delayAfterFailureMs(2);
        assertTrue(
                "Second attempt delay should be between 500-1000ms",
                delayForSecondAttempt >= 500L && delayForSecondAttempt < 1000L);

        // Third attempt: 2000ms
        assertEquals(
                "Third attempt should have 2000ms delay", 2000L, GRPCClient.delayAfterFailureMs(3));

        // Fourth attempt: 4000ms
        assertEquals(
                "Fourth attempt should have 4000ms delay",
                4000L,
                GRPCClient.delayAfterFailureMs(4));

        // Fifth attempt: 8000ms
        assertEquals(
                "Fifth attempt should have 8000ms delay", 8000L, GRPCClient.delayAfterFailureMs(5));

        // Sixth attempt: 8000ms (maximum delay)
        assertEquals(
                "Sixth attempt should have 8000ms delay (maximum)",
                8000L,
                GRPCClient.delayAfterFailureMs(6));
    }

    private Cdcpb.ChangeDataRequest createTestRequest() {
        return Cdcpb.ChangeDataRequest.newBuilder().setRequestId(12345).setCheckpointTs(0).build();
    }

    /** Test implementation of ChangeData service for testing GRPCClient retry logic. */
    private static class TestChangeDataServiceImpl extends ChangeDataGrpc.ChangeDataImplBase {
        private final AtomicInteger requestCount = new AtomicInteger(0);
        private final AtomicInteger remainingFailures = new AtomicInteger(0);
        private final AtomicBoolean returnNonRetryableError = new AtomicBoolean(false);

        public void reset() {
            requestCount.set(0);
            remainingFailures.set(0);
            returnNonRetryableError.set(false);
        }

        public void setFailCount(int count) {
            remainingFailures.set(count);
        }

        public void setReturnNonRetryableError(boolean value) {
            returnNonRetryableError.set(value);
        }

        public int getRequestCount() {
            return requestCount.get();
        }

        @Override
        public StreamObserver<Cdcpb.ChangeDataRequest> eventFeed(
                final StreamObserver<Cdcpb.ChangeDataEvent> responseObserver) {

            return new StreamObserver<Cdcpb.ChangeDataRequest>() {
                @Override
                public void onNext(Cdcpb.ChangeDataRequest request) {
                    int currentCount = requestCount.incrementAndGet();
                    LOG.info("Received request #{}, request: {}", currentCount, request);

                    // Check if we should return a non-retryable error
                    if (returnNonRetryableError.get()) {
                        // Return an INVALID_ARGUMENT error which is non-retryable
                        responseObserver.onError(
                                new StatusRuntimeException(
                                        Status.INVALID_ARGUMENT.withDescription(
                                                "Simulated non-retryable error")));
                        return;
                    }

                    // Check if we should fail this request
                    if (remainingFailures.getAndDecrement() > 0) {
                        LOG.info("Simulating failure for request #{}", currentCount);
                        responseObserver.onError(
                                new StatusRuntimeException(
                                        Status.UNAVAILABLE.withDescription("Simulated failure")));
                        return;
                    }

                    // Otherwise, return a successful response
                    LOG.info("Returning successful response for request #{}", currentCount);
                    Cdcpb.ChangeDataEvent response =
                            Cdcpb.ChangeDataEvent.newBuilder()
                                    .addEvents(
                                            Cdcpb.Event.newBuilder()
                                                    .setRegionId(1)
                                                    .setRequestId(request.getRequestId())
                                                    .build())
                                    .build();
                    responseObserver.onNext(response);
                }

                @Override
                public void onError(Throwable t) {
                    LOG.error("Request stream error", t);
                }

                @Override
                public void onCompleted() {
                    LOG.info("Request stream completed");
                    responseObserver.onCompleted();
                }
            };
        }
    }
}
