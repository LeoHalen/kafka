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
package org.apache.kafka.clients.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.test.MockSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;


/**
 * @author Halen Leo · 2022/4/21
 */
public class MockProducerSendTest {

    private static final Logger log = LoggerFactory.getLogger(MockProducerSendTest.class);

    private ObjectMapper objectMapper = new ObjectMapper();

    private final String topic = "halen-test-topic";
    private MockProducer<byte[], byte[]> mockProducer;
    private KafkaProducer<String, String> producer;
    private final ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(topic, "key1".getBytes(), "value1".getBytes());
    private final ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(topic, "key2".getBytes(), "value2".getBytes());
    private final ProducerRecord<String, String> record3 = new ProducerRecord<>(topic, "key4", "value4");
    private final String groupId = "group";

    private void buildMockProducer(boolean autoComplete) {
        this.mockProducer = new MockProducer<>(autoComplete, new MockSerializer(), new MockSerializer());
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "123.56.216.221:9092");
        this.producer = new KafkaProducer<String, String>(props, new StringSerializer(), new StringSerializer());
    }

    @Test
    public void testManualCompletion() throws Exception {
        buildMockProducer(false);
        Future<RecordMetadata> md1 = mockProducer.send(record1);
        assertFalse(md1.isDone(), "Send shouldn't have completed");
        Future<RecordMetadata> md2 = mockProducer.send(record2);
        assertFalse(md2.isDone(), "Send shouldn't have completed");
        assertTrue(mockProducer.completeNext(), "Complete the first request");
        assertFalse(isError(md1), "Requst should be successful");
        assertFalse(md2.isDone(), "Second request still incomplete");
        IllegalArgumentException e = new IllegalArgumentException("blah");
        assertTrue(mockProducer.errorNext(e), "Complete the second request with an error");
        try {
            md2.get();
            fail("Expected error to be thrown");
        } catch (ExecutionException err) {
            assertEquals(e, err.getCause());
        }
        assertFalse(mockProducer.completeNext(), "No more requests to complete");

//        Future<RecordMetadata> md3 = producer.send(record1);
//        Future<RecordMetadata> md4 = producer.send(record2);
//        assertTrue(!md3.isDone() && !md4.isDone(), "Requests should not be completed.");
//        producer.flush();
//        assertTrue(md3.isDone() && md4.isDone(), "Requests should be completed.");
    }

    @Test
    public void testSendProcess() throws Exception {
        buildMockProducer(true);
        Future<RecordMetadata> future = producer.send(record3);
        RecordMetadata recordMetadata = future.get();
        log.debug("打印发送成功的元数据信息: {}", recordMetadata);
        Thread.sleep(100000L);
    }

    private boolean isError(Future<?> future) {
        try {
            future.get();
            return false;
        } catch (Exception e) {
            return true;
        }
    }
}
