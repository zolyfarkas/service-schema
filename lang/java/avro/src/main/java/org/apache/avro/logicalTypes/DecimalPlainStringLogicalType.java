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

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.avro.Schema;

/**
 * @author Zoltan Farkas
 */
public final class DecimalPlainStringLogicalType extends DecimalBase {

  public DecimalPlainStringLogicalType(Number precision, Number scale, Schema.Type type,
          RoundingMode serRm, RoundingMode deserRm) {
    super(precision, scale, type, serRm, deserRm);
  }

  @Override
  public BigDecimal doDeserialize(Object object) {
    return new BigDecimal(object.toString());
  }

  @Override
  public Object doSerialize(BigDecimal decimal) {
     return decimal.toPlainString();
  }

}
