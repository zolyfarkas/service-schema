/*
 * Copyright 2016 The Apache Software Foundation.
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
package org.apache.avro.logicalTypes;

import com.google.common.collect.ImmutableMap;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.junit.Test;

/**
 *
 * @author zfarkas
 */
public class TestBigInteger {

  @Test
  public void testBigInteger() {
    LogicalType type = new BigIntegerFactory()
            .create(Schema.Type.BYTES, (Map) ImmutableMap.of("precision", 10));
    serializeDeserialize(type,  new java.math.BigInteger("3"));
    serializeDeserialize(type,  new java.math.BigInteger("-3"));
    serializeDeserialize(type,  new java.math.BigInteger("0"));
  }

  @Test
  public void testBigInteger2() {
    LogicalType type = new BigIntegerFactory()
            .create(Schema.Type.STRING, (Map) ImmutableMap.of("precision", 10));
    serializeDeserialize(type,  new java.math.BigInteger("3"));
    serializeDeserialize(type,  new java.math.BigInteger("-3"));
    serializeDeserialize(type,  new java.math.BigInteger("0"));
  }

  @Test
  public void testBigInteger3() {
    LogicalType type = new BigIntegerFactory()
            .create(Schema.Type.STRING, Collections.EMPTY_MAP);
    serializeDeserialize(type,  new java.math.BigInteger("3"));
    serializeDeserialize(type,  new java.math.BigInteger("-3"));
    serializeDeserialize(type,  new java.math.BigInteger("0"));
  }

  @Test
  public void testBigInteger4() {
    LogicalType type = new BigIntegerFactory()
            .create(Schema.Type.BYTES, Collections.EMPTY_MAP);
    serializeDeserialize(type,  new java.math.BigInteger("3"));
    serializeDeserialize(type,  new java.math.BigInteger("-3"));
    serializeDeserialize(type,  new java.math.BigInteger("0"));
  }

  private void serializeDeserialize(LogicalType<BigInteger> type, java.math.BigInteger nr) {
    Object buf =type.serialize(nr);
    java.math.BigInteger nr2 = type.deserialize(buf);
    System.out.println("NR " + nr2);
    Assert.assertEquals(nr, nr2);
  }

}
