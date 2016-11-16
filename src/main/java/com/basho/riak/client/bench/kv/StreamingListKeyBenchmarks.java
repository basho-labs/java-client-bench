/*
 * Copyright 2015 Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.bench.kv;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.kv.ListKeys;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author Alex Moore <amoore at gmail dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgsAppend = {"-server", "-disablesystemassertions"})
@Warmup(iterations = 5)
@Measurement(iterations = 100)
public class StreamingListKeyBenchmarks
{
    private static final Namespace testNamespace = new Namespace("plain", "listKeysBenchmark" + new Random().nextLong());

    private RiakClient client;

    @Setup
    public void prepare() throws UnknownHostException, ExecutionException, InterruptedException
    {

        System.out.println("Using namespace: " + testNamespace.toString());
        client = RiakClient.newClient(10017, "127.0.0.1");

        for (int i = 0; i < 10000; i++)
        {
            final Location location = new Location(testNamespace, Integer.toString(i));
            StoreValue s = new StoreValue.Builder(i).withLocation(location)
                                                      .withOption(StoreValue.Option.PW,Quorum.allQuorum())
                                                      .build();

            client.execute(s);
        }
    }

    @Benchmark
    public void StreamingListKeys(Blackhole bh) throws ExecutionException, InterruptedException
    {
        ListKeys(bh, (lk) -> client.executeAsyncStreaming(lk, 10));
    }

    @Benchmark
    public void BlockingListKeys(Blackhole bh) throws ExecutionException, InterruptedException
    {
        ListKeys(bh, (lk) -> client.executeAsync(lk));
    }

    private void ListKeys(Blackhole bh, Function<ListKeys, RiakFuture<ListKeys.Response, Namespace>> executionStrategy)
            throws ExecutionException, InterruptedException
    {
        final ListKeys listKeys = new ListKeys.Builder(testNamespace).build();

        final RiakFuture<ListKeys.Response, Namespace> streamingFuture = executionStrategy.apply(listKeys);

        final ListKeys.Response response = streamingFuture.get();

        for (Location location : response)
        {
            bh.consume(location);
        }
    }
}
