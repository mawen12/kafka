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

import org.apache.kafka.common.errors.SerializationException;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Map;

/**
 * 支持将{@code String}转换为{@code byte[]}。
 *
 * <p>字符串默认编码为UTF8，可以通过属性{@code key.serializer.encoding},{@code value.serializer.encoding}
 * 或{@code serializer.encoding}来指定。前两个参数有限于后一个
 */
public class StringSerializer implements Serializer<String> {
    private Charset encoding = StandardCharsets.UTF_8;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // 根据参数确认属性名
        String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
        // 读取属性值
        // TODO by mawen getOrDefault can simplify
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null)
            // 属性值为空，从serializer.encoding获取值
            encodingValue = configs.get("serializer.encoding");
        if (encodingValue instanceof String) {// 仅处理String类型的值
            String encodingName = (String) encodingValue;
            try {
                // 将字符串转换为字符集
                encoding = Charset.forName(encodingName);
            } catch (UnsupportedCharsetException | IllegalCharsetNameException e) {
                // 对于设置了错误的编码格式，直接抛出异常
                throw new SerializationException("Unsupported encoding " + encodingName, e);
            }
        }
    }

    @Override
    public byte[] serialize(String topic, String data) {
        if (data == null)
            return null;
        else
            // 直接使用字符串本身提供的方法进行转换
            return data.getBytes(encoding);
    }
}