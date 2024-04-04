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

set -e
set -o pipefail
set -o errexit

# solution for printing output in "set -x" trace mode without tracing the echo calls
shopt -s expand_aliases
echo_and_restore_trace() {
  builtin echo "$@"
  [ $trace_enabled -eq 1 ] && set -x || true
}
alias echo='{ [[ $- =~ .*x.* ]] && trace_enabled=1 || trace_enabled=0; set +x; } 2> /dev/null; echo_and_restore_trace'

MVN_COMMAND='mvn -B -ntp'

function mvn_test() {
  (
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
    echo "::group::Run tests for " "$@"
    $MVN_COMMAND test $failfast_args "$@"
    echo "::endgroup::"
  )
}

echo -n "Test Group : $TEST_GROUP"

# Test Groups  -- start --
function broker_group_1() {
  mvn_test -pl pulsar-broker -Dgroups='broker' -DtestReuseFork=true -DskipAfterFailureCount=1
}

function broker_group_2() {
  mvn_test -pl pulsar-broker -Dgroups='schema,utils,functions-worker,broker-io,broker-discovery,broker-compaction,broker-naming,websocket,other' -DtestReuseFork=false -DfailIfNoTests=false
}

function broker_group_3() {
  mvn_test -pl pulsar-broker -Dgroups='broker-admin'
}

function broker_client_api() {
  mvn_test -pl pulsar-broker -Dgroups='broker-api'
}

function broker_client_impl() {
  mvn_test -pl pulsar-broker -Dgroups='broker-impl'
}

function broker_jdk8() {
  mvn_test -pl pulsar-broker -Dgroups='broker-jdk8' -Dpulsar.allocator.pooled=true
}

# prints summaries of failed tests to console
# by using the targer/surefire-reports files
# works only when testForkCount > 1 since that is when surefire will create reports for individual test classes
function print_testng_failures() {
  (
    { set +x; } 2>/dev/null
    local testng_failed_file="$1"
    local report_prefix="${2:-Test failure in}"
    local group_title="${3:-Detailed test failures}"
    if [ -f "$testng_failed_file" ]; then
      local testng_report_dir=$(dirname "$testng_failed_file")
      local failed_count=0
      for failed_test_class in $(cat "$testng_failed_file" | grep 'class name=' | perl -p -e 's/.*\"(.*?)\".*/$1/'); do
        ((failed_count += 1))
        if [ $failed_count -eq 1 ]; then
          echo "::endgroup::"
          echo "::group::${group_title}"
        fi
        local test_report_file="${testng_report_dir}/${failed_test_class}.txt"
        if [ -f "${test_report_file}" ]; then
          local test_report="$(cat "${test_report_file}" | egrep "^Tests run: " | perl -p -se 's/^(Tests run: .*) <<< FAILURE! - in (.*)$/::warning::$report_prefix $2 - $1/' -- -report_prefix="${report_prefix}")"
          echo "$test_report"
          cat "${test_report_file}"
        fi
      done
    fi
  )
}

function broker_flaky() {
  echo "::endgroup::"
  echo "::group::Running quarantined tests"
  mvn_test --no-fail-fast -pl pulsar-broker -Dgroups='quarantine' -DexcludedGroups='flaky' -DfailIfNoTests=false \
    -DtestForkCount=2 ||
    print_testng_failures pulsar-broker/target/surefire-reports/testng-failed.xml "Quarantined test failure in" "Quarantined test failures"
  echo "::endgroup::"
  echo "::group::Running flaky tests"
  mvn_test --no-fail-fast -pl pulsar-broker -Dgroups='flaky' -DexcludedGroups='quarantine' -DtestForkCount=2
  echo "::endgroup::"
  local modules_with_flaky_tests=$(git grep -l '@Test.*"flaky"' | grep '/src/test/java/' | \
    awk -F '/src/test/java/' '{ print $1 }' | grep -v -E 'pulsar-broker' | sort | uniq | \
    perl -0777 -p -e 's/\n(\S)/,$1/g')
  if [ -n "${modules_with_flaky_tests}" ]; then
    echo "::group::Running flaky tests in modules '${modules_with_flaky_tests}'"
    mvn_test --no-fail-fast -pl "${modules_with_flaky_tests}" -Dgroups='flaky' -DexcludedGroups='quarantine' -DfailIfNoTests=false
    echo "::endgroup::"
  fi
}

function proxy() {
    echo "::group::Running pulsar-proxy tests"
    mvn_test -pl pulsar-proxy -Dtest="org.apache.pulsar.proxy.server.ProxyServiceTlsStarterTest"
    mvn_test -pl pulsar-proxy -Dtest="org.apache.pulsar.proxy.server.ProxyServiceStarterTest"
    mvn_test -pl pulsar-proxy -Dexclude='org.apache.pulsar.proxy.server.ProxyServiceTlsStarterTest,
                                                  org.apache.pulsar.proxy.server.ProxyServiceStarterTest'
    echo "::endgroup::"
}

function other() {
  $MVN_COMMAND clean install -PbrokerSkipTest \
                                     -Dexclude='org/apache/pulsar/proxy/**/*.java,
                                                **/ManagedLedgerTest.java,
                                                **/TestPulsarKeyValueSchemaHandler.java,
                                                **/PrimitiveSchemaTest.java,
                                                **/BlobStoreManagedLedgerOffloaderTest.java,
                                                **/BlobStoreManagedLedgerOffloaderStreamingTest.java'

  mvn_test -pl managed-ledger -Dinclude='**/ManagedLedgerTest.java,
                                                  **/OffloadersCacheTest.java'

  mvn_test -pl pulsar-sql/presto-pulsar-plugin -Dinclude='**/TestPulsarKeyValueSchemaHandler.java'

  mvn_test -pl pulsar-client -Dinclude='**/PrimitiveSchemaTest.java'

  mvn_test -pl tiered-storage/jcloud -Dinclude='**/BlobStoreManagedLedgerOffloaderTest.java'
  mvn_test -pl tiered-storage/jcloud -Dinclude='**/BlobStoreManagedLedgerOffloaderStreamingTest.java'

  echo "::endgroup::"
  local modules_with_quarantined_tests=$(git grep -l '@Test.*"quarantine"' | grep '/src/test/java/' | \
    awk -F '/src/test/java/' '{ print $1 }' | egrep -v 'pulsar-broker|pulsar-proxy' | sort | uniq | \
    perl -0777 -p -e 's/\n(\S)/,$1/g')
  if [ -n "${modules_with_quarantined_tests}" ]; then
    echo "::group::Running quarantined tests outside of pulsar-broker & pulsar-proxy (if any)"
    mvn_test --no-fail-fast -pl "${modules_with_quarantined_tests}" test -Dgroups='quarantine' -DexcludedGroups='flaky' \
      -DfailIfNoTests=false || \
        echo "::warning::There were test failures in the 'quarantine' test group."
    echo "::endgroup::"
  fi
}

# Test Groups  -- end --

TEST_GROUP=$1

echo "Test Group : $TEST_GROUP"

set -x

case $TEST_GROUP in

  BROKER_GROUP_1)
    broker_group_1
    ;;

  BROKER_GROUP_2)
    broker_group_2
    ;;

  BROKER_GROUP_3)
    broker_group_3
    ;;

  BROKER_CLIENT_API)
    broker_client_api
    ;;

  BROKER_CLIENT_IMPL)
    broker_client_impl
    ;;

  BROKER_FLAKY)
    broker_flaky
    ;;

  PROXY)
    proxy
    ;;

  OTHER)
    other
    ;;

  BROKER_JDK8)
    broker_jdk8
    ;;

  *)
    echo -n "INVALID TEST GROUP"
    exit 1
    ;;
esac
