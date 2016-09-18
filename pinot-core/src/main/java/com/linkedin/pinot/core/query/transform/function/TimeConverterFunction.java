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
package com.linkedin.pinot.core.query.transform.function;

import com.linkedin.pinot.core.query.transform.TransformFunction;

public class TimeConverterFunction {

  public static class ToHoursSinceEpochFromDaysSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 24;
    }
  }

  public static class ToMinutesSinceEpochFromDaysSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 1440;
    }
  }

  public static class ToSecondsSinceEpochFromDaysSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 86400;
    }
  }

  public static class ToMillisecondsSinceEpochFromDaysSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 86400000L;
    }
  }

  public static class ToDaysSinceEpochFromHoursSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 24;
    }
  }

  public static class ToMinutesSinceEpochFromHoursSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 60;
    }
  }

  public static class ToSecondsSinceEpochFromHoursSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 3600;
    }
  }

  public static class ToMillisecondsSinceEpochFromHoursSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 3600000L;
    }
  }

  public static class ToDaysSinceEpochFromMinutesSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 1440;
    }
  }

  public static class ToHoursSinceEpochFromMinutesSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 60;
    }
  }

  public static class ToSecondsSinceEpochFromMinutesSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 60;
    }
  }

  public static class ToMillisecondsSinceEpochFromMinutesSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 60000L;
    }
  }

  public static class ToDaysSinceEpochFromSecondsSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 86400;
    }
  }

  public static class ToHoursSinceEpochFromSecondsSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 3600;
    }
  }

  public static class ToMinutesSinceEpochFromSecondsSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 60;
    }
  }

  public static class ToMillisecondsSinceEpochFromSecondsSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 1000L;
    }
  }

  public static class ToDaysSinceEpochFromMillisecondsSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 86400000L;
    }
  }

  public static class ToHoursSinceEpochFromMillisecondsSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 3600000;
    }
  }

  public static class ToMinutesSinceEpochFromMillisecondsSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 60000;
    }
  }

  public static class ToSecondsSinceEpochFromMillisecondsSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 1000;
    }
  }
}
