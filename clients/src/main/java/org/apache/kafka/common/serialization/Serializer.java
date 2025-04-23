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
package org.apache.kafka.common.serialization;

import org.apache.kafka.common.header.Headers;

import java.io.Closeable;
import java.util.Map;

/**
 * 提供从对象转换到字节数组的接口。
 *
 * <p>实现该接口的类应该有一个无参构造器。
 *
 * <p>一旦它可用，就会去实现{@link org.apache.kafka.common.ClusterResourceListener}来接受集群元数据。
 * 请查阅{@code ClusterResourceListener}的类描述获取更多信息。
 *
 * @param <T> 要转换的Java类型
 */
public interface Serializer<T> extends Closeable {

    /**
     * 配置该类
     *
     * @param configs 以键值对形式的配置
     * @param isKey {@code true}为键，{@code false}为值
     */
    default void configure(Map<String, ?> configs, boolean isKey) {
        // 故意留空
    }

    /**
     * 将{@code data}转换为字节数组
     *
     * @param topic 数据关联的主题
     * @param data 类型化的数据
     * @return 序列化后的字节数组
     */
    byte[] serialize(String topic, T data);

    /**
     * 将{@code data}转换为字节数组
     *
     * @param topic 关联数据的主题
     * @param headers 关联记录的头
     * @param data 类型化的数据
     * @return 序列化后的字节数组
     */
    default byte[] serialize(String topic, Headers headers, T data) {
        return serialize(topic, data);
    }

    /**
     * 关闭该序列化器
     *
     * <p>由于可能被调用多次，因此该方法必须是幂等的。
     */
    @Override
    default void close() {
        // 故意留空
    }
}
