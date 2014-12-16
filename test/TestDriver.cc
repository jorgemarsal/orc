/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include "orc/orc-config.hh"
#include "TestDriver.hh"

#include "wrap/orc-proto-wrapper.hh"
#include "wrap/gtest-wrapper.h"


#include <iostream>

const char *exampleDirectory = 0;

GTEST_API_ int main(int argc, char **argv) {
  GOOGLE_PROTOBUF_VERIFY_VERSION;
  std::cout << "ORC version: " << ORC_VERSION << "\n";
  if (argc == 2) {
    exampleDirectory = argv[1];
  } else {
    exampleDirectory = "../examples";
  }
  std::cout << "example dir = " << exampleDirectory << "\n";
  testing::InitGoogleTest(&argc, argv);
  int result = RUN_ALL_TESTS();
  return result;
}
