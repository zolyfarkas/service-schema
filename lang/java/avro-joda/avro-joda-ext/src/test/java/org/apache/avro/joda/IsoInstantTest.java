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
package org.apache.avro.joda;

import org.apache.avro.Schema;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Zoltan Farkas
 */
public class IsoInstantTest {


  @Test
  public void testIsoInstantString() {
    IsoInstant instantType = new IsoInstant(Schema.Type.STRING);
    Instant epoch = new Instant(0);
    String str = (String) instantType.serialize(epoch);
    Assert.assertEquals("1970-01-01T00:00:00.000Z", str);
    Instant di = (Instant) instantType.deserialize(str);
    Assert.assertEquals(di, epoch);
  }

  @Test
  public void testIsoInstantLong() {
    IsoInstant instantType = new IsoInstant(Schema.Type.LONG);
    Instant epoch = new Instant(0);
    Long str = (Long) instantType.serialize(epoch);
    Assert.assertEquals(0L, str.longValue());
    Instant di = (Instant) instantType.deserialize(str);
    Assert.assertEquals(di, epoch);
  }

}
