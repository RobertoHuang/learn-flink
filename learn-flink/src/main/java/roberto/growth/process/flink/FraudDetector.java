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

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.util.Iterator;

/**
 * 欺诈检测器：检测固定1分钟窗口内累计交易额大于500的账户
 * 窗口对齐到整分钟：10:00:00-10:01:00, 10:01:00-10:02:00, 10:02:00-10:03:00 ...
 */
public class FraudDetector extends ProcessWindowFunction<Transaction, Alert, Long, TimeWindow> {
    private static final long serialVersionUID = 1L;
    private static final double LARGE_AMOUNT = 500.00;

    @Override
    public void process(Long accountId, Context context, Iterable<Transaction> elements, Collector<Alert> out) {
        double sumAmount = 0.0;
        Iterator<Transaction> iterator = elements.iterator();

        while (iterator.hasNext()) {
            Transaction transaction = iterator.next();
            sumAmount += transaction.getAmount();
        }

        if (sumAmount > LARGE_AMOUNT) {
            Alert alert = new Alert();
            alert.setId(accountId);
            out.collect(alert);
        }
    }
}
