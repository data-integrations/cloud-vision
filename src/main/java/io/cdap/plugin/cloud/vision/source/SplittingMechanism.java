/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.cloud.vision.source;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Determines splitting mechanisms. Choose amongst default (uses the default splitting mechanism of file input format),
 * directory (by each sub directory).
 */
public enum SplittingMechanism {
  DEFAULT("default"),
  DIRECTORY("directory");

  private static final Map<String, SplittingMechanism> byDisplayName = Arrays.stream(values())
    .collect(Collectors.toMap(SplittingMechanism::getDisplayName, Function.identity()));

  private final String displayName;

  SplittingMechanism(String displayName) {
    this.displayName = displayName;
  }

  @Nullable
  public static SplittingMechanism fromDisplayName(String displayName) {
    return byDisplayName.get(displayName);
  }

  public String getDisplayName() {
    return displayName;
  }
}