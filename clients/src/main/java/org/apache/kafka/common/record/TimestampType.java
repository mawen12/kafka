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
package org.apache.kafka.common.record;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.NoSuchElementException;

/**
 * 记录的时间戳类型
 *
 * @see RecordMetadata
 */
public enum TimestampType {
    /**
     * 没有时间戳，即代表{@link KafkaProducer#send(ProducerRecord)}所返回的
     * {@link RecordMetadata#timestamp()}为空
     */
    NO_TIMESTAMP_TYPE(-1, "NoTimestampType"),

    /**
     * 由用户指定的时间，如果用户未提供时间，则为生产者发送消息的时间。
     */
    CREATE_TIME(0, "CreateTime"),

    /**
     * kafka broker在追加消息时的本地时间
     */
    LOG_APPEND_TIME(1, "LogAppendTime");

    public final int id;
    public final String name;

    TimestampType(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public static TimestampType forName(String name) {
        for (TimestampType t : values())
            if (t.name.equals(name))
                return t;
        throw new NoSuchElementException("Invalid timestamp type " + name);
    }

    @Override
    public String toString() {
        return name;
    }
}
