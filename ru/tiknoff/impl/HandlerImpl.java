package ru.tiknoff.impl;

import org.jetbrains.annotations.NotNull;
import ru.tiknoff.ApplicationStatusResponse;
import ru.tiknoff.Client;
import ru.tiknoff.Handler;
import ru.tiknoff.Response;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class HandlerImpl implements Handler {
    private final Client client;

    public HandlerImpl(Client client) {
        this.client = client;
    }

    @Override
    public ApplicationStatusResponse performOperation(String id) {
        var failuresCounter = new AtomicInteger(0);
        var lastMethodExecution = new AtomicReference<Duration>();
        return CompletableFuture.anyOf(getResponseCompletableFuture(client::getApplicationStatus1, id, failuresCounter, lastMethodExecution),
                        getResponseCompletableFuture(client::getApplicationStatus2, id, failuresCounter, lastMethodExecution))
                .thenApply(res -> {
                    if (res instanceof Response.Success success) {
                        return new ApplicationStatusResponse.Success(success.applicationId(), success.applicationStatus());
                    }
                    return new ApplicationStatusResponse.Failure(lastMethodExecution.get(), failuresCounter.get());
                })
                .completeOnTimeout(
                        new ApplicationStatusResponse.Failure(lastMethodExecution.get(), failuresCounter.get()),
                        15,
                        TimeUnit.SECONDS)
                .join();
    }

    @NotNull
    private CompletableFuture<Response> getResponseCompletableFuture(Function<String, Response> requester,
                                                                     String id,
                                                                     AtomicInteger failuresCounter,
                                                                     AtomicReference<Duration> lastMethodExecution) {
        return CompletableFuture.supplyAsync(() -> {
            var executeUntil = System.currentTimeMillis() + 1_000L * 15;
            var start = System.currentTimeMillis();
            do {
                var result = requester.apply(id);
                switch (result) {
                    case Response.Success success -> {
                        return success;
                    }
                    case Response.Failure __ -> {
                        lastMethodExecution.set(Duration.ofMillis(System.currentTimeMillis() - start));
                        failuresCounter.incrementAndGet();
                    }
                    case Response.RetryAfter retryAfter -> LockSupport.parkUntil(
                            System.currentTimeMillis() + retryAfter.delay().get(ChronoUnit.MILLIS));
                }
            } while (executeUntil <= System.currentTimeMillis());
            return null;
        });
    }
}
