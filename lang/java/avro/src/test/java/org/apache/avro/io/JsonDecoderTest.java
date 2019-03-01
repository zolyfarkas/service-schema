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
package org.apache.avro.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class JsonDecoderTest {


  private static final String SCHEMA
        = "{\n" +
"  \"type\": \"record\",\n" +
"  \"name\": \"test\",\n" +
"  \"namespace\": \"test.name\",\n" +
"  \"fields\": [\n" +
"    {\n" +
"      \"name\": \"items\",\n" +
"      \"type\": {\n" +
"        \"type\": \"array\",\n" +
"        \"items\": {\n" +
"          \"type\": \"record\",\n" +
"          \"name\": \"items\",\n" +
"          \"fields\": [\n" +
"            {\n" +
"              \"name\": \"name\",\n" +
"              \"type\": \"string\"\n" +
"            },\n" +
"            {\n" +
"              \"name\": \"state\",\n" +
"              \"type\": \"string\"\n" +
"            }\n" +
"          ]\n" +
"        }\n" +
"      }\n" +
"    },\n" +
"    {\n" +
"      \"name\": \"firstname\",\n" +
"      \"type\": \"string\"\n" +
"    }\n" +
"  ]\n" +
"}";


  private static final String testData = "{ \"items\": [\n" +
"\n" +
"{ \"name\": \"dallas\", \"state\": \"TX\", \"country\": {\n" +
"                \"string\": \"USA\"\n" +
"            }}\n" +
"\n" +
"], \"firstname\":\"fname\", \"lastname\":\"lname\" }";

  @Test
  public void testDecoding() throws IOException {
    Schema writerSchema = new Schema.Parser().parse(SCHEMA);
    Schema readerSchema = writerSchema;
    ByteArrayInputStream bis =
            new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8));
    Decoder decoder = DecoderFactory.get().jsonDecoder(writerSchema, bis);
    GenericDatumReader reader = new GenericDatumReader(writerSchema, readerSchema);
    GenericRecord testData = (GenericRecord) reader.read(null, decoder);
    System.out.println(testData);
  }

}
