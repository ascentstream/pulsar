#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

set -e
set -o pipefail
set -o errexit

TEST_GROUP=$1
if [ -z "$TEST_GROUP" ]; then
  echo "usage: $0 [test_group]"
  exit 1
fi
shift

# runs integration tests
mvn_run_integration_test() {
  (
  set -x

  local use_fail_fast=1
  if [[ "$GITHUB_ACTIONS" == "true" && "$GITHUB_EVENT_NAME" != "pull_request" ]]; then
    use_fail_fast=0
  fi
  if [[ "$1" == "--no-fail-fast" ]]; then
    use_fail_fast=0
    shift;
  fi
  local failfast_args
  if [ $use_fail_fast -eq 1 ]; then
    failfast_args="-DtestFailFast=true -DtestFailFastFile=/tmp/test_fail_fast_killswitch.$$.$RANDOM.$(date +%s) --fail-fast"
  else
    failfast_args="-DtestFailFast=false --fail-at-end"
  fi
  # run the integration tests
  mvn -B -ntp -DredirectTestOutputToFile=false $failfast_args -f tests/pom.xml test "$@"
  )
}

test_group_shade() {
  mvn_run_integration_test "$@" -DShadeTests -DtestForkCount=1 -DtestReuseFork=false
}

test_group_backwards_compat() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-backwards-compatibility.xml -DintegrationTests
  mvn_run_integration_test "$@" -DBackwardsCompatTests -DtestForkCount=1 -DtestReuseFork=false
}

test_group_cli() {
  # run pulsar cli integration tests
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-cli.xml -DintegrationTests

  # run pulsar auth integration tests
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-auth.xml -DintegrationTests
}

test_group_function() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-function.xml -DintegrationTests
}

test_group_messaging() {
  # run integration messaging tests
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-messaging.xml -DintegrationTests
  # run integration proxy tests
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-proxy.xml -DintegrationTests
  # run integration proxy with WebSocket tests
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-proxy-websocket.xml -DintegrationTests
}

test_group_schema() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-schema.xml -DintegrationTests
}

test_group_standalone() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-standalone.xml -DintegrationTests
}

test_group_transaction() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-transaction.xml -DintegrationTests
}

test_group_tiered_filesystem() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=tiered-filesystem-storage.xml -DintegrationTests
}

test_group_tiered_jcloud() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=tiered-jcloud-storage.xml -DintegrationTests
}

test_group_pulsar_connectors_thread() {
  # run integration function
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-thread.xml -DintegrationTests -Dgroups=function

  # run integration source
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-thread.xml -DintegrationTests -Dgroups=source

  # run integration sink
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-thread.xml -DintegrationTests -Dgroups=sink
}

test_group_pulsar_connectors_process() {
  # run integration function
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-process.xml -DintegrationTests -Dgroups=function

  # run integration source
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-process.xml -DintegrationTests -Dgroups=source

  # run integraion sink
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-process.xml -DintegrationTests -Dgroups=sink
}

test_group_sql() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-sql.xml -DintegrationTests -DtestForkCount=1 -DtestReuseFork=false
}

test_group_pulsar_io() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-io-sources.xml -DintegrationTests -Dgroups=source
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-io-sinks.xml -DintegrationTests -Dgroups=sink
}

test_group_pulsar_io_ora() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-io-ora-source.xml -DintegrationTests -Dgroups=source -DtestRetryCount=0
}



echo "Test Group : $TEST_GROUP"
test_group_function_name="test_group_$(echo "$TEST_GROUP" | tr '[:upper:]' '[:lower:]')"
if [[ "$(LC_ALL=C type -t $test_group_function_name)" == "function" ]]; then
  eval "$test_group_function_name" "$@"
else
  echo "INVALID TEST GROUP"
  exit 1
fi
