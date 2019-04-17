/*
 * Copyright 2017 The Apache Software Foundation.
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
package org.apache.avro;

import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Zoltan Farkas
 */
public class TestEnumEvolution {

  @Test
  public void testEnumSchem() {
    Schema schema = SchemaBuilder
            .enumeration("MyEnum")
            .enumDefault("UNKNOWN")
            .symbols("UNKNOWN", "A", "B", "C");
    System.out.println(schema.toString());
    Assert.assertThat(schema.toString(), Matchers.containsString("\"default\":\"UNKNOWN\""));
    Schema fromString = Schema.fromString(schema.toString());
    Assert.assertEquals("UNKNOWN", fromString.getEnumDefault());
  }

  @Test
  public void testEnumSchem2() throws IOException {
    Schema schema = SchemaBuilder
            .enumeration("MyEnum")
            .enumDefault("UNKNOWN")
            .symbols("UNKNOWN", "A", "B", "C");
    schema.addProp("symbolAliases", ImmutableMap.of("A", Arrays.asList("A A")));
    schema.addProp("stringSymbols", ImmutableMap.of("B", "BB"));
    System.out.println(schema.toString());
    Assert.assertThat(schema.toString(), Matchers.containsString("\"default\":\"UNKNOWN\""));
    Schema fromString = Schema.fromString(schema.toString());
    Assert.assertEquals("UNKNOWN", fromString.getEnumDefault());

    Schema recSchema = SchemaBuilder
            .record("myrecord").namespace("org.example")
            .fields().name("enumVal").type(schema).noDefault()
            .endRecord();

    readtest1(recSchema);
    readtest2(recSchema);
    readtest3(recSchema);
  }

  public void readtest1(Schema recSchema) throws IOException {
    JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(recSchema, "{\"enumVal\":\"A A\"}");
    GenericDatumReader reader = new GenericDatumReader(recSchema);
    GenericRecord read = (GenericRecord) reader.read(null, jsonDecoder);
    System.out.println(read);
    Assert.assertEquals("A", ((GenericEnumSymbol) read.get("enumVal")).toString());
  }

  public void readtest2(Schema recSchema) throws IOException {
    JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(recSchema, "{\"enumVal\":\"BB\"}");
    GenericDatumReader reader = new GenericDatumReader(recSchema);
    GenericRecord read = (GenericRecord) reader.read(null, jsonDecoder);
    System.out.println(read);
    Assert.assertEquals("BB", ((GenericEnumSymbol) read.get("enumVal")).toString());
  }

  public void readtest3(Schema recSchema) throws IOException {
    JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(recSchema, "{\"enumVal\":\"dfsfgsdg\"}");
    GenericDatumReader reader = new GenericDatumReader(recSchema);
    GenericRecord read = (GenericRecord) reader.read(null, jsonDecoder);
    System.out.println(read);
    Assert.assertEquals("UNKNOWN", ((GenericEnumSymbol) read.get("enumVal")).toString());
  }


  @Test
  public void testCompatibility() throws IOException {
    Schema schema1 = SchemaBuilder
            .record("myrecord").namespace("org.example").aliases("oldrecord").fields()
            .name("f0").aliases("f0alias").type().enumeration("MyEnum").prop("fallbackSymbol", "UNKNOWN")
            .symbols("UNKNOWN", "A", "B", "C").enumDefault("UNKNOWN")
            .endRecord();

    Schema schema2 = SchemaBuilder
            .record("myrecord").namespace("org.example").aliases("oldrecord").fields()
            .name("f0").aliases("f0alias").type().enumeration("MyEnum").prop("default", "UNKNOWN")
            .symbols("UNKNOWN", "A", "B", "C", "D").enumDefault("UNKNOWN")
            .endRecord();

    SchemaCompatibility.SchemaPairCompatibility compat1 =
            SchemaCompatibility.checkReaderWriterCompatibility(schema1, schema2);
    Assert.assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, compat1.getType());
    SchemaCompatibility.SchemaPairCompatibility compat2 = SchemaCompatibility.checkReaderWriterCompatibility(schema2, schema1);
    Assert.assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, compat2.getType());
    GenericRecord rec = new GenericData.Record(schema2);
    rec.put("f0", new GenericData.EnumSymbol(schema2.getField("f0").schema(), "D"));

    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> writer =
      new GenericDatumWriter<GenericRecord>(schema2);
    Encoder encoder = EncoderFactory.get().binaryEncoder(bao, null);
    writer.write(rec, encoder);
    encoder.flush();

    GenericDatumReader<GenericRecord> reader =
      new GenericDatumReader<GenericRecord>(schema2, schema1);
    Decoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bao.toByteArray()), null);
    GenericRecord read = reader.read(null, decoder);
    System.out.println(read);
    Assert.assertEquals("UNKNOWN", read.get("f0").toString());
  }

  @Test
  public void testIncompatibility() {
      Schema schema1 = SchemaBuilder
            .record("myrecord").namespace("org.example").aliases("oldrecord").fields()
            .name("f0").aliases("f0alias").type().enumeration("MyEnum")
            .symbols("UNKNOWN", "A", "B", "C").noDefault()
            .endRecord();

    Schema schema2 = SchemaBuilder
            .record("myrecord").namespace("org.example").aliases("oldrecord").fields()
            .name("f0").aliases("f0alias").type().enumeration("MyEnum")
            .symbols("UNKNOWN", "A", "B", "C", "D").noDefault()
            .endRecord();

    SchemaCompatibility.SchemaPairCompatibility compat =
            SchemaCompatibility.checkReaderWriterCompatibility(schema1, schema2);
    Assert.assertEquals(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE, compat.getType());
    SchemaCompatibility.SchemaPairCompatibility compat2 =
            SchemaCompatibility.checkReaderWriterCompatibility(schema2, schema1);
    Assert.assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, compat2.getType());

  }

  @Test(expected = AvroTypeException.class)
  public void testEnumValidation() {
        SchemaBuilder
            .record("myrecord").namespace("org.example").aliases("oldrecord").fields()
            .name("f0").aliases("f0alias").type().enumeration("MyEnum").prop("fallbackSymbol", "CRAP")
            .symbols("UNKNOWN", "A", "B", "C").enumDefault("CRAP")
            .endRecord();
  }


}
