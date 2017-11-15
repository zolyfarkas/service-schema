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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.Map;
import org.junit.Assert;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.hamcrest.Matchers;
import org.junit.Test;

/**
 *
 * @author zfarkas
 */
public class TestDecimal {

  @Test
  public void testDecimal() {
    LogicalType type2 = new DecimalFactory()
            .create(Schema.Type.STRING, (Map) ImmutableMap.of("precision", 32, "scale", 10));
    runTests(type2);
    runTestFailure(type2);

    LogicalType type = new DecimalFactory()
            .create(Schema.Type.BYTES, (Map) ImmutableMap.of("precision", 32, "scale", 10));
    runTests(type);
    runTestFailure(type2);
  }

  @Test
  public void testDecimalRoundingModeSerialization() {
    LogicalType type2 = new DecimalFactory()
            .create(Schema.Type.STRING,
                    (Map) ImmutableMap.of("precision", 32, "scale", 10, "serRounding", RoundingMode.HALF_DOWN.DOWN));
    serializeDeserialize(type2, new BigDecimal("0.12345678910"), new BigDecimal(0.0000000001));
  }

  @Test
  public void testDecimalRoundingModeDeSerialization() {
    LogicalType type2 = new DecimalFactory()
            .create(Schema.Type.STRING,
                    (Map) ImmutableMap.of("precision", 32, "scale", 10, "deserRounding", RoundingMode.HALF_DOWN.DOWN));
    BigDecimal nr2 = (BigDecimal) type2.deserialize("0.1234567891000124");
    Assert.assertTrue(new BigDecimal("0.1234567891000124").subtract(nr2).abs().compareTo(new BigDecimal(0.0000000001))
            < 0);


     LogicalType typex = new DecimalFactory()
            .create(Schema.Type.BYTES,
                    (Map) ImmutableMap.of("precision", 32, "scale", 20,
                            "serRounding", RoundingMode.HALF_DOWN.DOWN,
                            "deserRounding", RoundingMode.HALF_DOWN.DOWN));


    LogicalType type1 = new DecimalFactory()
            .create(Schema.Type.BYTES,
                    (Map) ImmutableMap.of("precision", 32, "scale", 10,
                            "serRounding", RoundingMode.HALF_DOWN.DOWN,
                            "deserRounding", RoundingMode.HALF_DOWN.DOWN));
    ByteBuffer buf = (ByteBuffer) typex.serialize(new BigDecimal("0.1234567891000124"));
    nr2 = (BigDecimal) type1.deserialize(buf);
    Assert.assertTrue(new BigDecimal("0.1234567891000124").subtract(nr2).abs().compareTo(new BigDecimal(0.0000000001))
            < 0);
  }


  public void runTests(LogicalType type2) {
    serializeDeserialize(type2, new BigDecimal("3.1"), BigDecimal.ZERO);
    serializeDeserialize(type2, new BigDecimal("-3.1"), BigDecimal.ZERO);
    serializeDeserialize(type2, new BigDecimal("100000"), BigDecimal.ZERO);
    serializeDeserialize(type2, new BigDecimal("-100000"), BigDecimal.ZERO);
    serializeDeserialize(type2, new BigDecimal("0"), BigDecimal.ZERO);
    serializeDeserialize(type2, new BigDecimal("0.123456789"), BigDecimal.ZERO);
    serializeDeserialize(type2, new BigDecimal("0.1234567890"), BigDecimal.ZERO);
    serializeDeserialize(type2, new BigDecimal("-0.123456789"), BigDecimal.ZERO);
    serializeDeserialize(type2, new BigDecimal("-0.1234567890"), BigDecimal.ZERO);
  }

  public void runTestFailure(LogicalType type2) {
    try {
      serializeDeserialize(type2, new BigDecimal("0.12345678910"), BigDecimal.ZERO);
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      Assert.assertThat(ex.getMessage(), Matchers.containsString("Cannot serialize"));
      System.out.println("Expected " + ex);
    }
  }

  private void serializeDeserialize(LogicalType type, BigDecimal nr, BigDecimal epsilon) {
    Object buf;
    try {
      buf = type.serialize(nr);
    } catch (RuntimeException ex) {
      throw new IllegalArgumentException("Cannot serialize " + nr, ex);
    }
    BigDecimal nr2;
    try {
      nr2 = (BigDecimal) type.deserialize(buf);
    } catch (RuntimeException ex) {
      throw new IllegalArgumentException("Cannot deserialize " + nr, ex);
    }
    Assert.assertTrue("Comparing " + nr + " with " + nr2, nr.subtract(nr2).abs().compareTo(epsilon) <= 0);
  }

}
