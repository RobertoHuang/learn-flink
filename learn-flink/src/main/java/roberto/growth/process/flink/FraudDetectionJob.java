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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package roberto.growth.process.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;

import java.time.Duration;

/**
 * Skeleton code for the datastream walkthrough
 */
public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        GeneratorFunction<Long, Transaction> generator = index -> {
            Transaction transaction = new Transaction();
            transaction.setAccountId((index % 100));
            transaction.setAmount(Math.random() * 100);
            transaction.setTimestamp(System.currentTimeMillis());
            return transaction;
        };

        DataStream<Transaction> transactions = streamExecutionEnvironment.fromSource(
                new DataGeneratorSource<>(
                        generator,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(1000),
                        TypeInformation.of(Transaction.class)
                ),
                WatermarkStrategy.noWatermarks(),
                "transactions"
        );

        DataStream<Transaction> transactionsWithTimestamp = transactions.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<>(Duration.ofMinutes(1)) {
                    @Override
                    public long extractTimestamp(Transaction transaction) {
                        // 从 Transaction 对象中提取时间戳，作为事件时间
                        return transaction.getTimestamp();
                    }
                }
        );

        DataStream<Alert> alerts = transactionsWithTimestamp
                .keyBy(Transaction::getAccountId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .allowedLateness(Duration.ofMinutes(1))
                .process(new FraudDetector()).name("fraud-detector");

        alerts.addSink(new AlertSink()).name("send-alerts");

        streamExecutionEnvironment.execute("Fraud Detection");
    }
}

