/*
 * Copyright 2018 The Apache Software Foundation.
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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.temporal.Temporal;
import org.junit.Assert;
import org.apache.avro.Schema;
import org.junit.Test;

/**
 *
 * @author Zoltan Farkas
 */
public class AnyTemporalTest {


  @Test
  public void testTemporal() {
    AnyTemporal at = new AnyTemporal(Schema.Type.STRING);
    testDT(at);
    testD(at);
    testYM(at);
    testY(at);
  }

  public void testDT(AnyTemporal at) {
    LocalDateTime now = LocalDateTime.now();
    Object serialize = at.serialize(now);
    Temporal now2 = at.deserialize(serialize);
    Assert.assertEquals(now, now2);
  }

  public void testD(AnyTemporal at) {
    LocalDate now = LocalDate.now();
    Object serialize = at.serialize(now);
    Temporal now2 = at.deserialize(serialize);
    Assert.assertEquals(now, now2);
  }

  public void testYM(AnyTemporal at) {
    YearMonth now = YearMonth.now();
    Object serialize = at.serialize(now);
    Temporal now2 = at.deserialize(serialize);
    Assert.assertEquals(now, now2);
  }

  public void testY(AnyTemporal at) {
    Year now = Year.now();
    Object serialize = at.serialize(now);
    Temporal now2 = at.deserialize(serialize);
    Assert.assertEquals(now, now2);
  }

}
