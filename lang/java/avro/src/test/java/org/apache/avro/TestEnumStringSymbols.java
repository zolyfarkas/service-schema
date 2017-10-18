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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.internal.JacksonUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Zoltan Farkas
 */
public class TestEnumStringSymbols {

  @Test
  public void testStringSymbols() throws IOException {
    Schema schema1 = SchemaBuilder
            .record("myrecord").namespace("org.example").aliases("oldrecord").fields()
            .name("f0").type().enumeration("MyEnum")
            .prop("fallbackSymbol", "UNKNOWN")
            .prop("stringSymbols", JacksonUtils.toJsonNode(ImmutableMap.of("A", "A+")))
            .symbols("UNKNOWN", "A", "B", "C").enumDefault("UNKNOWN")
            .endRecord();
    GenericRecord rec = new GenericData.Record(schema1);
    rec.put("f0", new GenericData.EnumSymbol(schema1.getField("f0").schema(), "A"));

    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> writer =
      new GenericDatumWriter<GenericRecord>(schema1);
    Encoder encoder = EncoderFactory.get().binaryEncoder(bao, null);
    writer.write(rec, encoder);
    encoder.flush();

    GenericDatumReader<GenericRecord> reader =
      new GenericDatumReader<GenericRecord>(schema1, schema1);
    Decoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bao.toByteArray()), null);
    GenericRecord read = reader.read(null, decoder);
    System.out.println(read);
    Assert.assertEquals("A+", read.get("f0").toString());


    ByteArrayOutputStream bao2 = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> writer2 =
      new GenericDatumWriter<GenericRecord>(schema1);
    Encoder encoder2 = EncoderFactory.get().jsonEncoder(schema1, bao2);
    writer2.write(rec, encoder2);
    encoder2.flush();
    System.out.println("Record: " + bao2);
    GenericDatumReader<GenericRecord> reader2 =
      new GenericDatumReader<GenericRecord>(schema1, schema1);
    Decoder decoder2 = DecoderFactory.get().jsonDecoder(schema1, new ByteArrayInputStream(bao2.toByteArray()));
    GenericRecord read2 = reader2.read(null, decoder2);
    System.out.println(read2);
    Assert.assertEquals("A+", read2.get("f0").toString());

  }

}
