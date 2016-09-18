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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.core.query.transform.TransformFunction;
import com.linkedin.pinot.core.query.transform.TransformFunctionRegistry;
import com.linkedin.pinot.core.query.transform.function.StringConverterFunction;
import com.linkedin.pinot.core.query.transform.function.TimeConverterFunction;

public class TransformFunctionRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransformFunctionRegistry.class);

  private static Map<String, Class<? extends TransformFunction>> keyToFunction =
      new ConcurrentHashMap<String, Class<? extends TransformFunction>>();

  static {
    keyToFunction.put("convert_days_to_hours", TimeConverterFunction.ToHoursSinceEpochFromDaysSinceEpochConverter.class);
    keyToFunction.put("convert_days_to_mins", TimeConverterFunction.ToMinutesSinceEpochFromDaysSinceEpochConverter.class);
    keyToFunction.put("convert_days_to_seconds", TimeConverterFunction.ToSecondsSinceEpochFromDaysSinceEpochConverter.class);
    keyToFunction.put("convert_days_to_milliseconds", TimeConverterFunction.ToMillisecondsSinceEpochFromDaysSinceEpochConverter.class);

    keyToFunction.put("convert_hours_to_days", TimeConverterFunction.ToDaysSinceEpochFromHoursSinceEpochConverter.class);
    keyToFunction.put("convert_hours_to_mins", TimeConverterFunction.ToMinutesSinceEpochFromHoursSinceEpochConverter.class);
    keyToFunction.put("convert_hours_to_seconds", TimeConverterFunction.ToSecondsSinceEpochFromHoursSinceEpochConverter.class);
    keyToFunction.put("convert_hours_to_milliseconds", TimeConverterFunction.ToMillisecondsSinceEpochFromHoursSinceEpochConverter.class);

    keyToFunction.put("convert_mins_to_days", TimeConverterFunction.ToDaysSinceEpochFromMinutesSinceEpochConverter.class);
    keyToFunction.put("convert_mins_to_hours", TimeConverterFunction.ToHoursSinceEpochFromMinutesSinceEpochConverter.class);
    keyToFunction.put("convert_mins_to_seconds", TimeConverterFunction.ToSecondsSinceEpochFromMinutesSinceEpochConverter.class);
    keyToFunction.put("convert_mins_to_milliseconds", TimeConverterFunction.ToMillisecondsSinceEpochFromMinutesSinceEpochConverter.class);

    keyToFunction.put("convert_seconds_to_days", TimeConverterFunction.ToDaysSinceEpochFromSecondsSinceEpochConverter.class);
    keyToFunction.put("convert_seconds_to_hours", TimeConverterFunction.ToHoursSinceEpochFromSecondsSinceEpochConverter.class);
    keyToFunction.put("convert_seconds_to_mins", TimeConverterFunction.ToMinutesSinceEpochFromSecondsSinceEpochConverter.class);
    keyToFunction.put("convert_seconds_to_milliseconds", TimeConverterFunction.ToMillisecondsSinceEpochFromSecondsSinceEpochConverter.class);

    keyToFunction.put("convert_milliseconds_to_days", TimeConverterFunction.ToDaysSinceEpochFromMillisecondsSinceEpochConverter.class);
    keyToFunction.put("convert_milliseconds_to_hours", TimeConverterFunction.ToHoursSinceEpochFromMillisecondsSinceEpochConverter.class);
    keyToFunction.put("convert_milliseconds_to_mins", TimeConverterFunction.ToMinutesSinceEpochFromMillisecondsSinceEpochConverter.class);
    keyToFunction.put("convert_milliseconds_to_seconds", TimeConverterFunction.ToSecondsSinceEpochFromMillisecondsSinceEpochConverter.class);

    keyToFunction.put("get_length", StringConverterFunction.StringLengthConverter.class);
    keyToFunction.put("to_lower", StringConverterFunction.ToLowerCaseConverter.class);
    keyToFunction.put("to_upper", StringConverterFunction.ToUpperCaseConverter.class);
  }

  public static void register(String transformKey, Class<? extends TransformFunction> transformFunction) {
    keyToFunction.put(transformKey, transformFunction);
  }

  public static boolean contains(String column) {
    return keyToFunction.containsKey(column);
  }

  @SuppressWarnings("unchecked")
  public static TransformFunction get(String transformKey) {
    try {
      Class<? extends TransformFunction> cls = keyToFunction.get(transformKey.toLowerCase());
      if (cls != null) {
        return cls.newInstance();
      }
      cls = (Class<? extends TransformFunction>) Class.forName(transformKey);
      keyToFunction.put(transformKey, cls);
      return cls.newInstance();
    } catch (Exception ex) {
      LOGGER.error("Caught exception while getting transform function", ex);
      Utils.rethrowException(ex);
      throw new AssertionError("Should not reach this");
    }
  }

}
