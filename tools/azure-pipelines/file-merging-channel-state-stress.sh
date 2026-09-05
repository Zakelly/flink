#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
duration=${STRESS_DURATION_SECONDS:-18000}
iteration_timeout=${STRESS_ITERATION_TIMEOUT_SECONDS:-600}
: "${STRESS_LOG_DIR:?Set STRESS_LOG_DIR to an absolute output directory}"
: "${MAVEN_CACHE_FOLDER:?Set MAVEN_CACHE_FOLDER to the Maven repository used for compilation}"
export MAVEN_ARGS="${MAVEN_ARGS:--Dmaven.repo.local=$MAVEN_CACHE_FOLDER}"
export PROFILE="${PROFILE:--Djdk17 -Pjava17-target}"
[[ "$duration" =~ ^[1-9][0-9]*$ && "$iteration_timeout" =~ ^[1-9][0-9]*$ ]]
[[ "$STRESS_LOG_DIR" = /* ]]
command -v timeout >/dev/null
mkdir -p "$STRESS_LOG_DIR"

test_class=org.apache.flink.test.checkpointing.FileMergingChannelStateITCase
reports_dir="$PWD/flink-tests/target/surefire-reports"
if [[ -d "$reports_dir" ]]; then
    mv "$reports_dir" "$STRESS_LOG_DIR/pre-existing-reports"
fi

# Use the same Maven settings and mirror as the compile and preparation jobs.
# The helper handles unsuccessful mirror probes itself.
set +eu
source ./tools/ci/maven-utils.sh || exit $?
set -eu

deadline=$((SECONDS + duration))
iteration=0
passed=0
while (( SECONDS < deadline )); do
    iteration=$((iteration + 1))
    run_dir="$STRESS_LOG_DIR/run-$iteration"
    mkdir "$run_dir"
    remaining=$((deadline - SECONDS))
    echo "Starting iteration $iteration ($remaining seconds remaining)." | tee -a "$STRESS_LOG_DIR/summary.log"

    # Dependencies were installed during preparation. Run only flink-tests so shaded
    # dependency JARs are used instead of upstream reactor target/classes directories.
    run_status=0
    timeout --kill-after=30s "${iteration_timeout}s" \
        bash -c 'run_mvn "$@"' _ -pl flink-tests -Dfast \
        "-Dtest=$test_class" -Dsurefire.failIfNoSpecifiedTests=true \
        -Dsurefire.rerunFailingTestsCount=0 -Dmaven.test.failure.ignore=false \
        "-Dlog4j.configurationFile=file:$PWD/tools/ci/log4j.properties" \
        "-Dlog.dir=$run_dir" test >"$run_dir/maven.log" 2>&1 || run_status=$?

    if [[ -d "$reports_dir" ]]; then
        mv "$reports_dir" "$run_dir/reports"
    fi
    if (( run_status != 0 )); then
        echo "Iteration $iteration failed (exit $run_status); stopping." | tee -a "$STRESS_LOG_DIR/summary.log"
        tail -n 100 "$run_dir/maven.log"
        exit "$run_status"
    fi

    # A successful Maven exit alone does not prove the requested test was executed.
    python3 - "$run_dir/reports/TEST-$test_class.xml" <<'PY'
import sys
import xml.etree.ElementTree as ET

report = ET.parse(sys.argv[1]).getroot()
assert int(report.attrib['tests']) == 1, report.attrib
assert all(int(report.attrib.get(key, 0)) == 0
           for key in ('failures', 'errors', 'skipped')), report.attrib
PY
    passed=$((passed + 1))
    echo "Iteration $iteration passed." | tee -a "$STRESS_LOG_DIR/summary.log"
done

echo "Completed $passed successful iterations; ${duration}s budget reached, no further iteration will start." | tee -a "$STRESS_LOG_DIR/summary.log"
(( passed > 0 ))
