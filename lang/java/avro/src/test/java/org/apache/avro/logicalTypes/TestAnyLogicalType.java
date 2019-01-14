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
package org.apache.avro.logicalTypes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.avro.AvroUtils;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Zoltan Farkas
 */
public class TestAnyLogicalType {


  @Test
  public void testJsonRecord() throws IOException {
    Schema anyRecord = SchemaBuilder.record("test")
            .fields()
            .requiredString("avsc")
            .requiredBytes("content")
            .endRecord();
    anyRecord.addProp(LogicalType.LOGICAL_TYPE_PROP, "any");
    LogicalType lt = LogicalTypes.fromSchema(anyRecord);
    anyRecord.setLogicalType(lt);

    Schema testSchema = SchemaBuilder.builder().record("test_record").fields()
            .name("anyField").type(anyRecord)
            .noDefault()
            .endRecord();
    GenericData.Record record = new GenericData.Record(testSchema);
    record.put("anyField", "someString");
      String writeAvroExtendedJson = AvroUtils.writeAvroExtendedJson(record);
    System.out.println(writeAvroExtendedJson);
    GenericRecord back = AvroUtils.readAvroExtendedJson(new StringReader(writeAvroExtendedJson), testSchema);
    Assert.assertEquals(record.toString(), back.toString());

  }


 @Test
  public void testJsonRecord2() throws IOException {
    GenericData.Record record = createTestRecord();
    String writeAvroExtendedJson = AvroUtils.writeAvroExtendedJson(record);
    System.out.println(writeAvroExtendedJson);
    GenericRecord back = AvroUtils.readAvroExtendedJson(new StringReader(writeAvroExtendedJson), record.getSchema());
    Assert.assertEquals(record.toString(), back.toString());
  }

  @Test
  public void testJsonRecord3() throws IOException {
    GenericData.Record record = createTestRecord();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    AvroUtils.writeAvroBin(bos,  record);
    System.out.println(new String(bos.toByteArray(), StandardCharsets.UTF_8));
    GenericRecord back = (GenericRecord) AvroUtils.readAvroBin(new ByteArrayInputStream(bos.toByteArray()),
            record.getSchema());
    Assert.assertEquals(record.toString(), back.toString());
  }

  public static GenericData.Record createTestRecord() {
    Schema anyRecord = SchemaBuilder.record("test")
            .fields()
            .requiredString("avsc")
            .requiredBytes("content")
            .endRecord();
    anyRecord.addProp(LogicalType.LOGICAL_TYPE_PROP, "any");
    LogicalType lt = LogicalTypes.fromSchema(anyRecord);
    anyRecord.setLogicalType(lt);
    Schema testSchema = SchemaBuilder.builder().record("test_record").fields()
            .name("someCrap").type(Schema.create(Schema.Type.BOOLEAN)).withDefault(true)
            .name("anyField").type(anyRecord).noDefault()
            .name("otherCrap").type(Schema.create(Schema.Type.STRING)).withDefault("bubu")
            .endRecord();
    GenericData.Record record = new GenericData.Record(testSchema);
    record.put("someCrap", true);
    record.put("anyField", Arrays.asList("someString"));
    record.put("otherCrap", "false");
    return record;
  }


  public static GenericData.Record createTestRecord2() {
    Schema anyRecord = SchemaBuilder.record("test")
            .fields()
            .requiredString("avsc")
            .requiredBytes("content")
            .endRecord();
    anyRecord.addProp(LogicalType.LOGICAL_TYPE_PROP, "any");
    LogicalType lt = LogicalTypes.fromSchema(anyRecord);
    anyRecord.setLogicalType(lt);
    Schema testSchema = SchemaBuilder.builder().record("test_record").fields()
            .name("someCrap").type(Schema.create(Schema.Type.BOOLEAN)).withDefault(true)
            .name("anyField").type(Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), anyRecord)))
            .noDefault()
            .name("otherCrap").type(Schema.create(Schema.Type.STRING)).withDefault("bubu")
            .endRecord();
    GenericData.Record record = new GenericData.Record(testSchema);
    record.put("someCrap", true);
    record.put("anyField", Arrays.asList("someString"));
    record.put("otherCrap", "false");
    return record;
  }

  @Test
  public void testJsonRecord2Json() throws IOException {
    GenericData.Record record = createTestRecord2();
    String writeAvroExtendedJson = AvroUtils.writeAvroExtendedJson(record);
    System.out.println(writeAvroExtendedJson);
    GenericRecord back = AvroUtils.readAvroExtendedJson(new StringReader(writeAvroExtendedJson), record.getSchema());
    Assert.assertEquals(record.toString(), back.toString());
  }

  @Test
  public void testJsonRecord2OJson() throws IOException {
    GenericData.Record record = createTestRecord2();
    String writeAvroExtendedJson = AvroUtils.writeAvroJson(record);
    System.out.println(writeAvroExtendedJson);
    GenericRecord back = AvroUtils.readAvroJson(new StringReader(writeAvroExtendedJson), record.getSchema());
    Assert.assertEquals(record.toString(), back.toString());
  }

  @Test
  public void testJsonRecord2Bin() throws IOException {
    GenericData.Record record = createTestRecord2();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    AvroUtils.writeAvroBin(bos,  record);
    System.out.println(new String(bos.toByteArray(), StandardCharsets.UTF_8));
    GenericRecord back = (GenericRecord) AvroUtils.readAvroBin(new ByteArrayInputStream(bos.toByteArray()),
            record.getSchema());
    Assert.assertEquals(record.toString(), back.toString());
  }




}
