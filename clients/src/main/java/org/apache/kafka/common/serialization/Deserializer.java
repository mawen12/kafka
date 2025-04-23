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
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 提供从字节数组转换为对象的接口。
 *
 * <p>实现该接口的类应该有一个无参构造器。
 *
 * <p>一旦它可用，就会去实现{@link org.apache.kafka.common.ClusterResourceListener}来接受集群元数据。
 * 请查阅{@code ClusterResourceListener}的类描述获取更多信息。
 *
 * @param <T> 反序列化的结果类型
 */
public interface Deserializer<T> extends Closeable {

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
     * 反序列化记录的值，将其从字节数组转换为对象
     *
     * @param topic 关联该数据的主题
     * @param data 序列化字节数组，可能为空，实现类推荐在处理null时返回null或一个值，而非抛出异常。
     * @return 反序列化后的类型数据，可能为空
     */
    T deserialize(String topic, byte[] data);

    /**
     * 反序列化记录的值，将其从字节数组转换为对象
     *
     * @param topic 关联该数据的主题
     * @param headers 关联记录的头，可能为空
     * @param data 序列化字节数组，可能为空，实现类推荐在处理null时返回null或一个值，而非抛出异常。
     * @return 反序列化后的类型数据，可能为空
     */
    default T deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic, data);
    }

    /**
     * 反序列化记录的值，将其从{@link ByteBuffer}转换为对象
     *
     * @param topic 关联数据的主题
     * @param headers 关联记录的头，可能为空
     * @param data {@link ByteBuffer}，可能为空，实现类推荐在处理null时返回null或一个值，而非抛出异常。
     * @return 反序列化后的类型数据，可能为空
     */
    default T deserialize(String topic, Headers headers, ByteBuffer data) {
        return deserialize(topic, headers, Utils.toNullableArray(data));
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
