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
package org.apache.kafka.common.errors;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 此致命异常表明另一个启动的生产者也具有相同的{@code transactional.id}。在任何时间{@code transactional.id}的值是全局唯一的，只能被一个生产者使用，
 * 如果一个新的生产者也设置了相同的{@code transactional.id}，那么会导致该生产者不能在发送事务请求。当我们遇到此异常时，必须关闭生产者示例。
 *
 * <p>但是对于幂等生产者来说，出现该异常是可以忽略的。
 *
 * @see org.apache.kafka.clients.producer.KafkaProducer#send(ProducerRecord, Callback)
 */
public class ProducerFencedException extends ApiException {

    public ProducerFencedException(String msg) {
        super(msg);
    }
}
