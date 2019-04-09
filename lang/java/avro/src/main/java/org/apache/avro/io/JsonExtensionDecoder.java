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
package org.apache.avro.io;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.avro.Schema;

/**
 *
 * @author Zoltan Farkas
 */
public interface JsonExtensionDecoder {

  <T> T readValue(final Schema schema, final Class<T> clasz) throws IOException;

  JsonNode readValueAsTree(final Schema schema) throws IOException;

  TokenBuffer bufferValue(final Schema schema) throws IOException;

  BigInteger readBigInteger(final Schema schema) throws IOException;

  BigDecimal readBigDecimal(final Schema schema) throws IOException;

}
