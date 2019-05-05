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
import java.io.InputStream;
import java.io.StringReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.AvroUtils;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaResolver;
import org.apache.avro.SchemaResolvers;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ExtendedJsonDecoder;
import org.apache.avro.reflect.ExtendedReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Assert;
import org.junit.Test;
import org.spf4j.base.avro.DebugDetail;
import org.spf4j.base.avro.HealthRecord;
import org.spf4j.base.avro.Method;
import org.spf4j.base.avro.ServiceError;
import org.spf4j.base.avro.StackSampleElement;
import org.spf4j.base.avro.jmx.MBeanAttributeInfo;

/**
 *
 * @author Zoltan Farkas
 */
public class TestAnyLogicalType {


  static {
   SchemaResolvers.registerDefault(new SchemaResolver() {
      @Override
      public Schema resolveSchema(String id) {
        if ("org.spf4j.avro:core-schema:0.17:c".equals(id)) {
          return ServiceError.getClassSchema();
        } else {
          throw new UnsupportedOperationException();
        }
      }

      @Override
      public String getId(Schema schema) {
        return schema.getProp("mvnId");
      }
    });
  }

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
    AvroUtils.writeAvroBin(bos, record);
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
    AvroUtils.writeAvroBin(bos, record);
    System.out.println(new String(bos.toByteArray(), StandardCharsets.UTF_8));
    GenericRecord back = (GenericRecord) AvroUtils.readAvroBin(new ByteArrayInputStream(bos.toByteArray()),
            record.getSchema());
    Assert.assertEquals(record.toString(), back.toString());
  }

  @Test
  public void testHealthCheckCluster2Repro() throws IOException {
    AvroUtils.readAvroExtendedJson(ClassLoader.getSystemResourceAsStream("testAnyJson.json"), HealthRecord.class);
  }

  @Test
  public void testJMXAttrsRepro() throws IOException {
    AvroUtils.readAvroExtendedJson(ClassLoader.getSystemResourceAsStream("testAnyJson2.json"),
            Schema.createArray(MBeanAttributeInfo.getClassSchema()));
  }


  @Test
  public void testServiceErrorParsing() throws IOException {
    ServiceError err0 = new ServiceError(404, "404", "bla", null, null);
    ServiceError err1 = new ServiceError(404, "404", "bla", err0, new DebugDetail("origin", Collections.EMPTY_LIST,
            new org.spf4j.base.avro.Throwable("aclass", "exception0", Collections.EMPTY_LIST,
                    null, Collections.EMPTY_LIST), Arrays.asList(new StackSampleElement(0, 0, 1,
                    new Method("a", "b")))));
    ServiceError err2 = new ServiceError(400, "400", "bla2", err1,
            new DebugDetail("origin", Collections.EMPTY_LIST,
                    new org.spf4j.base.avro.Throwable("aclass", "exception", Collections.EMPTY_LIST,
                            null, Collections.EMPTY_LIST), Collections.EMPTY_LIST));
    byte[] writeAvroExtendedJson = AvroUtils.writeAvroExtendedJson((SpecificRecord) err2);
    String strVal = new String(writeAvroExtendedJson, StandardCharsets.UTF_8);
    System.out.println(strVal);
    ServiceError back = AvroUtils.readAvroExtendedJson(new ByteArrayInputStream(writeAvroExtendedJson),
            ServiceError.class);
    Assert.assertEquals(err2.toString(), back.toString());
  }

  @Test
  public void testServiceErrorParsing2() throws IOException {
    URL resource = Thread.currentThread().getContextClassLoader().getResource("bugRepro2.json");
    try (InputStream openStream = resource.openStream()) {
      Schema schema = ExtendedReflectData.get().getSchema(ServiceError.class);
      DatumReader reader = new ReflectDatumReader(schema, schema);
      Decoder decoder = new ExtendedJsonDecoder(schema, openStream);
      try {
        ServiceError back = (ServiceError) reader.read(null, decoder);
        Assert.assertNotNull(back);
        System.out.println(back);
      } catch (RuntimeException ex) {
        System.out.println("Exception processing at " + decoder);
        throw ex;
      }

    }

  }

}
