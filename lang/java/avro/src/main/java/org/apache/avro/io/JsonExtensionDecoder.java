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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import javax.annotation.Nullable;
import org.apache.avro.Schema;

/**
 * An extension interface that allows to decode decimal, and other json types naturally.
 * @author Zoltan Farkas
 */
public interface JsonExtensionDecoder {

  <T> T readValue(final Schema schema, final Class<T> clasz) throws IOException;

  JsonNode readValueAsTree(final Schema schema) throws IOException;

  JsonParser bufferValue(final Schema schema) throws IOException;

  /** NUll if no number is next */
  @Nullable
  BigInteger readBigInteger(final Schema schema) throws IOException;

  /** Null if no number is next*/
  @Nullable
  BigDecimal readBigDecimal(final Schema schema) throws IOException;

}
