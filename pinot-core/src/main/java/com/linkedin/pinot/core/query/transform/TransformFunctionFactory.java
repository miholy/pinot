/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.transform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.Utils;

public class TransformFunctionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransformFunctionFactory.class);

  public static TransformFunction get(String functionName) {
    try {
      TransformFunction transformFunction = TransformFunctionRegistry.get(functionName);
      return transformFunction;
    } catch (Exception ex) {
      LOGGER.error("Caught exception while building transform function", ex);
      Utils.rethrowException(ex);
      throw new AssertionError("Should not reach here");
    }
  }
}
