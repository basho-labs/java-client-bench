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
package com.basho.riak.client.bench.timeseries;

import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.CollectionConverters;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.Row;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import shaded.com.basho.riak.protobuf.RiakTsPB;
import shaded.com.google.protobuf.ByteString;
import shaded.com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author Sergey Galkin <srggal at gmail dot com>
 */
//@State(Scope.Thread)
//@BenchmarkMode(Mode.SingleShotTime)
//@OutputTimeUnit(TimeUnit.MILLISECONDS)
//@Fork(value = 1, jvmArgsAppend = {"-server", "-disablesystemassertions"})
//@Warmup(time = 100, timeUnit = TimeUnit.MILLISECONDS)
//@Measurement(time = 100, timeUnit = TimeUnit.MILLISECONDS)
public class TsPBBenchmarks {
    //@Param({ "1", "10", "100", "1000", "10000"})
    int rowCount;

    private final static ByteString TABLE_NAME = ByteString.copyFromUtf8("TsTest");
    private List<Row> rows;

    private RiakTsPB.TsQueryResp pbQueryResp;
    private byte encodedQueryResponse[];
    private List<ColumnDescription> columns;
    private Collection<RiakTsPB.TsRow> pbRows;

    @Setup
    public void prepare() {

        // -- Generate data for encoding bench
        final long currentTime = System.currentTimeMillis();
        rows = new ArrayList<Row>(rowCount);
        for (int i = 0; i < rowCount; ++i)
        {
            final long now = currentTime + i;
            // "Now" as binary string, as a long, as a time, percentage done with this test as a double, true/false if now is a power of 2
            rows.add(i, new Row(new Cell(Long.toBinaryString(now)),
                    new Cell(i),
                    Cell.newTimestamp(now),
                    new Cell(i / (double)rowCount),
                    new Cell((now & (now - 1)) == 0))
            );
        }

        // -- Generate data for decoding bench
        columns = Arrays.asList(
                new ColumnDescription("binv", ColumnDescription.ColumnType.VARCHAR),
                new ColumnDescription("longv", ColumnDescription.ColumnType.SINT64),
                new ColumnDescription("tsv", ColumnDescription.ColumnType.TIMESTAMP),
                new ColumnDescription("doublev", ColumnDescription.ColumnType.DOUBLE),
                new ColumnDescription("boolv", ColumnDescription.ColumnType.BOOLEAN)
        );

        final RiakTsPB.TsQueryResp.Builder builder = RiakTsPB.TsQueryResp.newBuilder();
        builder.addAllColumns(
                CollectionConverters.convertColumnDescriptionsToPb(columns)
        );

        // Nb: Broken, we removed this method when we moved to TTB.
        //pbRows = CollectionConverters.convertRowsToPb(rows);
        builder.addAllRows(pbRows);

        pbQueryResp = builder.build();
        encodedQueryResponse = pbQueryResp.toByteArray();
    }

//    @Benchmark
//    public void parseTsQueryResp(Blackhole bh) throws InvalidProtocolBufferException {
//        bh.consume(
//                RiakTsPB.TsQueryResp.parseFrom(encodedQueryResponse)
//        );
//    }

//    @Benchmark
//    public void decodeTsQueryResp(Blackhole bh){
//        bh.consume(
//                TimeSeriesPBConverter.convertPbGetResp(pbQueryResp)
//        );
//    }

//    @Benchmark
//    public void decodeTsQueryRespLight(Blackhole bh){
//        bh.consume(
//                TimeSeriesPBLightConverter.convertPbGetResp(pbQueryResp)
//        );
//    }

//    @Benchmark
//    public void decodeFullCycleTsQueryResp(Blackhole bh) throws InvalidProtocolBufferException {
//        final RiakTsPB.TsQueryResp resp = RiakTsPB.TsQueryResp.parseFrom(encodedQueryResponse);
//        bh.consume(
//                TimeSeriesPBConverter.convertPbGetResp(resp)
//        );
//    }

//    @Benchmark
//    public void decodeFullCycleTsQueryRespLight(Blackhole bh) throws InvalidProtocolBufferException {
//        final RiakTsPB.TsQueryResp resp = RiakTsPB.TsQueryResp.parseFrom(encodedQueryResponse);
//        bh.consume(
//                TimeSeriesPBLightConverter.convertPbGetResp(resp)
//        );
//    }

//    @Benchmark
//    public void encodePbRows(Blackhole bh) {
//        bh.consume( TimeSeriesPBConverter.convertRowsToPb(rows));
//    }

//    @Benchmark
//    public void encodeFullCycleTsPutReq(Blackhole bh) {
//        final RiakTsPB.TsPutReq.Builder builder = RiakTsPB.TsPutReq.newBuilder();
//        builder.setTable(TABLE_NAME);
//        builder.addAllRows(TimeSeriesPBConverter.convertRowsToPb(rows));
//
//        bh.consume(
//                new RiakMessage(RiakMessageCodes.MSG_TsPutReq, builder.build().toByteArray())
//            );
//    }
}
