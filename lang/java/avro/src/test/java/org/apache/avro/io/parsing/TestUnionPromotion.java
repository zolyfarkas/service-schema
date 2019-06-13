/*
 * Copyright 2019 The Apache Software Foundation.
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
package org.apache.avro.io.parsing;

import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import org.junit.Assert;
import org.apache.avro.AvroUtils;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.junit.Test;

/**
 *
 * @author Zoltan Farkas
 */
public class TestUnionPromotion {

  @Test
  public void testUnionPromotionSimple() throws Exception {
    Schema directFieldSchema = SchemaBuilder
        .record("MyRecord").namespace("ns")
        .fields()
          .name("field1").type().stringType().noDefault()
        .endRecord();
    Schema schemaWithField = SchemaBuilder
        .record("MyRecord").namespace("ns")
        .fields()
          .name("field1").type().nullable().stringType().noDefault()
        .endRecord();
    GenericData.Record record = new GenericRecordBuilder(directFieldSchema)
            .set("field1", "someValue").build();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    AvroUtils.writeAvroBin(bos, record);
    Object read = AvroUtils.readAvroBin(new ByteArrayInputStream(bos.toByteArray()),
            directFieldSchema, schemaWithField);
    Assert.assertEquals("someValue", ((GenericRecord) read).get("field1").toString());

  }


  @Test
  public void testUnionPromotionCollection() throws Exception {
    Schema directFieldSchema = SchemaBuilder
        .record("MyRecord").namespace("ns")
        .fields()
          .name("field1").type().map().values().stringType().noDefault()
        .endRecord();
    Schema schemaWithField = SchemaBuilder
        .record("MyRecord").namespace("ns")
        .fields()
          .name("field1").type().nullable().map().values().stringType().noDefault()
        .endRecord();
    GenericData.Record record = new GenericRecordBuilder(directFieldSchema)
            .set("field1", ImmutableMap.of("a", "someValue")).build();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    AvroUtils.writeAvroBin(bos, record);
    Object read = AvroUtils.readAvroBin(new ByteArrayInputStream(bos.toByteArray()),
            directFieldSchema, schemaWithField);
    Map name = (Map) ((GenericRecord) read).get("field1");
    Assert.assertEquals("someValue", name.get(new Utf8("a")).toString());

  }
}
