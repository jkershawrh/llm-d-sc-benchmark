#!/usr/bin/env bash
set -euo pipefail

# One unchanged-image recovery cycle on one direct target Pod IP.  Every load
# Job is created suspended before any is released, uses zero warm-up requests,
# and carries its own absolute start epoch.  This harness never scales or
# patches the target Deployment.

checkpoint_delay_seconds() {
  local scheduled_ms
  local now_s
  local scheduled_s
  scheduled_ms=$1
  now_s=$2
  [[ "$scheduled_ms" =~ ^[0-9]+$ && "$now_s" =~ ^[0-9]+$ ]] || return 64
  scheduled_s=$((scheduled_ms / 1000))
  printf '%s\n' "$((scheduled_s - now_s))"
}

# Cluster-free executable seam for the nounset-sensitive checkpoint arithmetic.
if [[ "${1:-}" == --internal-checkpoint-delay ]]; then
  (( $# == 3 )) || exit 64
  checkpoint_delay_seconds "$2" "$3"
  exit $?
fi

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

KUBECONFIG_PATH=${KUBECONFIG_PATH:-/tmp/llm-d-sc-arena-kubeconfig}
NAMESPACE=${NAMESPACE:-llm-d-sc-scaleout}
DEPLOYMENT=${DEPLOYMENT:-classifier-target}
TARGET_SELECTOR=${TARGET_SELECTOR:-app.kubernetes.io/component=classifier-target,app.kubernetes.io/name=llm-d-sc-scaleout}
TARGET_CONTAINER=${TARGET_CONTAINER:-llm-d-sc}
TARGET_NODE=${TARGET_NODE:-gnr2.fm2aihpcsed.com}
DRIVER_NODE=${DRIVER_NODE:-rhgnr1}
LOCK_NAME=${LOCK_NAME:-sc-benchmark-matrix-lock}

RECOVERY_RUN_ID=${RECOVERY_RUN_ID:?set a unique DNS-safe RECOVERY_RUN_ID}
RECOVERY_CYCLE_INDEX=${RECOVERY_CYCLE_INDEX:-0}
START_LEAD_SECONDS=${START_LEAD_SECONDS:-360}
PLAN_ONLY=${PLAN_ONLY:-0}
DELETE_COMPLETED_JOBS=${DELETE_COMPLETED_JOBS:-1}
RESULT_ROOT=${RESULT_ROOT:-${REPO_ROOT}/results/recovery-cycles}
RUN_DIR=${RUN_DIR:-${RESULT_ROOT}/${RECOVERY_RUN_ID}}

readonly ARMED_DRIVER_IMAGE='image-registry.openshift-image-registry.svc:5000/llm-d-sc-gremlins/llm-d-sc-benchmark-driver-armed-51541f00e5fa@sha256:ef0f32ad3a7a29f4cd1f68ae8b8cfbc1bf36d66a173df8f68fd531db9d762aae'
readonly ARMED_DRIVER_SOURCE_SHA256='51541f00e5fa6e1918b4e57b9bfa432337345b1854b7289c836c3752543929d9'
DRIVER_IMAGE=${DRIVER_IMAGE:-${ARMED_DRIVER_IMAGE}}
DRIVER_BUILD_SOURCE_SHA256=${DRIVER_BUILD_SOURCE_SHA256:-${ARMED_DRIVER_SOURCE_SHA256}}
TARGET_IMAGE=${TARGET_IMAGE:-sha256:04323612ce3f73873b4c3ed6e09264e828241537e2c1a4231b43f32e9744d5aa}
MODEL_SHA256=${MODEL_SHA256:-7914abbd152278879b4c3235d188e3006753bb778b7de6266fbcbe4c4ba2ef2f}
TOKENIZER_SHA256=${TOKENIZER_SHA256:-851ca67100d372ca3ae031a6abd168f53489eebfd7d89523f35c5c9b4d372c3c}
TOKEN_COUNT=${TOKEN_COUNT:-64}

MAX_ROWS=${MAX_ROWS:-10000}
JOB_SEQUENCE_SPAN=${JOB_SEQUENCE_SPAN:-10001}
PHASE_MAX_IN_FLIGHT=${PHASE_MAX_IN_FLIGHT:-512}
DISPATCH_LATE_AFTER_MS=${DISPATCH_LATE_AFTER_MS:-1}
DROP_LATE_AFTER_MS=${DROP_LATE_AFTER_MS:-100}
RPC_TIMEOUT_MS=${RPC_TIMEOUT_MS:-30000}
MAX_DISPATCH_P99_LAG_MS=${MAX_DISPATCH_P99_LAG_MS:-5}
MAX_DRAIN_SECONDS=${MAX_DRAIN_SECONDS:-90}
METRIC_MAX_GAP_SECONDS=${METRIC_MAX_GAP_SECONDS:-10}
METRIC_SETTLE_SECONDS=${METRIC_SETTLE_SECONDS:-15}
HEALTH_MONITOR_INTERVAL_SECONDS=${HEALTH_MONITOR_INTERVAL_SECONDS:-10}
OC_REQUEST_TIMEOUT=${OC_REQUEST_TIMEOUT:-30s}
CURL_CONNECT_TIMEOUT_SECONDS=${CURL_CONNECT_TIMEOUT_SECONDS:-10}
CURL_MAX_TIME_SECONDS=${CURL_MAX_TIME_SECONDS:-30}
TARGET_COUNTER_SETTLE_SECONDS=${TARGET_COUNTER_SETTLE_SECONDS:-30}
TARGET_BASELINE_QUIET_SECONDS=${TARGET_BASELINE_QUIET_SECONDS:-12}
TOPOLOGY_PREFLIGHT_TIMEOUT_SECONDS=${TOPOLOGY_PREFLIGHT_TIMEOUT_SECONDS:-120}
TARGET_COUNTER_TOLERANCE=0
readonly ARMED_BARRIER_LEAD_SECONDS=180
readonly TARGET_BOUND_SCHEDULE_LEAD_SECONDS=175
readonly TARGET_BOUND_COMPLETION_LEAD_SECONDS=155
readonly PRE_T0_CANCELLATION_COMPLETION_LEAD_SECONDS=25
readonly PRE_T0_FOREGROUND_DELETE_TIMEOUT_SECONDS=90
readonly PRE_T0_ZERO_OBJECT_VERIFICATION_BUDGET_SECONDS=15
readonly IMMEDIATE_CANCELLATION_REQUEST_TIMEOUT_SECONDS=5
readonly COMMAND_TERMINATION_GRACE_SECONDS=5
readonly PRE_T0_CANCELLATION_SAFETY_MARGIN_SECONDS=10

# ARMED support is derived only from the smoke-validated immutable image/source
# pair above.  There is deliberately no environment flag that can authorize a
# different driver.  The later local-source check and 14-record barrier remain
# independent fail-closed gates.
DRIVER_ARMING_PROTOCOL=sustained-corpus-probe-armed-v1
if [[ "$DRIVER_IMAGE" == "$ARMED_DRIVER_IMAGE" \
   && "$DRIVER_BUILD_SOURCE_SHA256" == "$ARMED_DRIVER_SOURCE_SHA256" ]]; then
  DRIVER_ARMING_SUPPORTED=true
else
  DRIVER_ARMING_SUPPORTED=false
fi
readonly DRIVER_ARMING_SUPPORTED
DRIVER_PACKAGE_VERSION=0.1.0

TOPOLOGY_PREFLIGHT_RUNNER=${TOPOLOGY_PREFLIGHT_RUNNER:-${SCRIPT_DIR}/arena-sc-topology-preflight.py}
SUMMARY_RUNNER=${SUMMARY_RUNNER:-${SCRIPT_DIR}/arena-sc-same-pod-recovery-summarize.py}

k=(oc --kubeconfig "$KUBECONFIG_PATH" --request-timeout="$OC_REQUEST_TIMEOUT")
run_dir_owned=0
lock_acquired=0
jobs_created=0
plan_complete=0
measurement_complete=0
run_invalid=0
final_decision=""
last_error=""
monitor_pid=""
checkpoint_pids=()
checkpoint_names=()
pre_t0_cancel_deadline_override_s=0
command_termination_grace_seconds=$COMMAND_TERMINATION_GRACE_SECONDS
zero_object_verification_budget_seconds=$PRE_T0_ZERO_OBJECT_VERIFICATION_BUDGET_SECONDS

die() {
  last_error=$*
  echo "ERROR: ${last_error}" >&2
  if (( run_dir_owned == 1 )); then
    printf '%s\n' "$last_error" >"${RUN_DIR}/recovery-error.txt"
  fi
  exit 2
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

run_with_timeout() {
  local timeout_seconds
  timeout_seconds=$1
  shift
  python3 - "$timeout_seconds" "$@" <<'PY'
import os
import signal
import subprocess
import sys

process = subprocess.Popen(sys.argv[2:], start_new_session=True)
try:
    return_code = process.wait(timeout=int(sys.argv[1]))
except subprocess.TimeoutExpired:
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()
    raise SystemExit(124)
raise SystemExit(return_code)
PY
}

run_before_epoch() {
  local deadline_epoch_s
  local now_epoch_s
  local timeout_seconds
  deadline_epoch_s=$1
  shift
  now_epoch_s=$(date -u +%s)
  timeout_seconds=$((deadline_epoch_s - now_epoch_s - command_termination_grace_seconds))
  (( timeout_seconds > 0 )) || return 124
  run_with_timeout "$timeout_seconds" "$@"
}

unsigned_integer() {
  local name
  local value
  name=$1
  value=$2
  [[ "$value" =~ ^[0-9]+$ ]] || die "${name} must be an unsigned integer"
}

positive_integer() {
  local name
  local value
  name=$1
  value=$2
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "${name} must be a positive integer"
}

cleanup() {
  cleanup_entry=$?
  local exit_status
  local cleanup_error
  local owner
  local status
  local pid
  local now_epoch_s
  local t0_epoch_s
  local cancellation_deadline_epoch_s
  local delete_timeout_seconds
  local delete_exit_status
  local snapshot_exit_status
  local remaining_seconds
  local deletion_started_epoch_ms
  local deletion_completed_epoch_ms
  local cancellation_snapshot
  local remaining_objects
  local cancellation_valid
  local immediate_jobs_pods_delete_exit_status
  exit_status=$cleanup_entry
  cleanup_error=""
  owner=""
  status="aborted"
  trap - EXIT INT TERM ERR
  set +e

  # Always request immediate label-scoped deletion before waiting on any
  # observer.  Direct Pod deletion complements Job foreground propagation so a
  # late cleanup cannot silently leave an absolute-start driver alive.
  if (( jobs_created == 1 )) && { (( measurement_complete == 0 )) || (( DELETE_COMPLETED_JOBS == 1 )); }; then
    now_epoch_s=$(date -u +%s)
    t0_epoch_s=$((${t0_epoch_ms:-0} / 1000))
    deletion_started_epoch_ms=$((now_epoch_s * 1000))
    immediate_jobs_pods_delete_exit_status=0
    if (( measurement_complete == 0 )); then
      run_with_timeout "$IMMEDIATE_CANCELLATION_REQUEST_TIMEOUT_SECONDS" \
        "${k[@]}" delete jobs,pods -n "$NAMESPACE" -l "benchmark.llm-d/run-id=${RECOVERY_RUN_ID}" \
        --ignore-not-found --cascade=foreground --wait=false >/dev/null 2>&1
      immediate_jobs_pods_delete_exit_status=$?
    fi
    if (( t0_epoch_s > 0 && now_epoch_s < t0_epoch_s )); then
      if (( pre_t0_cancel_deadline_override_s > 0 )); then
        cancellation_deadline_epoch_s=$pre_t0_cancel_deadline_override_s
      else
        cancellation_deadline_epoch_s=$((t0_epoch_s - PRE_T0_CANCELLATION_COMPLETION_LEAD_SECONDS))
      fi
      now_epoch_s=$(date -u +%s)
      remaining_seconds=$((cancellation_deadline_epoch_s - now_epoch_s \
        - command_termination_grace_seconds - zero_object_verification_budget_seconds))
      delete_timeout_seconds=$remaining_seconds
      (( delete_timeout_seconds > PRE_T0_FOREGROUND_DELETE_TIMEOUT_SECONDS )) \
        && delete_timeout_seconds=$PRE_T0_FOREGROUND_DELETE_TIMEOUT_SECONDS
      delete_exit_status=124
      snapshot_exit_status=124
      remaining_objects=-1
      cancellation_snapshot='{"items":[]}'
      cancellation_valid=false
      if (( delete_timeout_seconds > 0 )); then
        run_before_epoch "$cancellation_deadline_epoch_s" \
          "${k[@]}" delete jobs,pods -n "$NAMESPACE" -l "benchmark.llm-d/run-id=${RECOVERY_RUN_ID}" \
          --ignore-not-found --cascade=foreground --wait=true \
          --timeout="${delete_timeout_seconds}s" >/dev/null 2>&1
        delete_exit_status=$?
      fi
      if (( delete_exit_status == 0 )); then
        cancellation_snapshot=$(run_before_epoch "$cancellation_deadline_epoch_s" \
          "${k[@]}" get jobs,pods -n "$NAMESPACE" \
          -l "benchmark.llm-d/run-id=${RECOVERY_RUN_ID}" -o json 2>/dev/null)
        snapshot_exit_status=$?
        if (( snapshot_exit_status == 0 )); then
          remaining_objects=$(jq -r 'if (.items|type)=="array" then (.items|length) else -1 end' \
            <<<"$cancellation_snapshot" 2>/dev/null)
          [[ "$remaining_objects" =~ ^[0-9]+$ ]] || remaining_objects=-1
        fi
      fi
      deletion_completed_epoch_ms=$(( $(date -u +%s) * 1000 ))
      if (( delete_exit_status == 0 && snapshot_exit_status == 0 && remaining_objects == 0 \
            && deletion_completed_epoch_ms <= cancellation_deadline_epoch_s * 1000 )); then
        cancellation_valid=true
      else
        cleanup_error="pre-T0 recovery Job cancellation did not verify zero labeled Jobs/Pods before its hard deadline"
        # Keep cancellation fail-closed even after a bounded foreground delete
        # or verification failure.  These are best-effort repeats; the run
        # remains cleanup_failed because zero-before-T0 was not proven.
        "${k[@]}" delete jobs,pods -n "$NAMESPACE" -l "benchmark.llm-d/run-id=${RECOVERY_RUN_ID}" \
          --ignore-not-found --cascade=foreground --wait=false >/dev/null 2>&1
      fi
      if (( run_dir_owned == 1 )); then
        printf '%s\n' "$cancellation_snapshot" >"${RUN_DIR}/pre-t0-cancellation-snapshot.json"
        jq -n --arg run_id "$RECOVERY_RUN_ID" \
          --arg selector "benchmark.llm-d/run-id=${RECOVERY_RUN_ID}" \
          --argjson started "$deletion_started_epoch_ms" \
          --argjson completed "$deletion_completed_epoch_ms" \
          --argjson deadline "$((cancellation_deadline_epoch_s * 1000))" \
          --argjson t0 "$((t0_epoch_s * 1000))" \
          --argjson delete_exit "$delete_exit_status" \
          --argjson immediate_jobs_pods_delete_exit "$immediate_jobs_pods_delete_exit_status" \
          --argjson snapshot_exit "$snapshot_exit_status" \
          --argjson remaining "$remaining_objects" \
          --argjson valid "$cancellation_valid" \
          '{schema_version:1,run_id:$run_id,selector:$selector,
            deletion_started_epoch_ms:$started,deletion_completed_epoch_ms:$completed,
            completion_deadline_epoch_ms:$deadline,t0_epoch_ms:$t0,
            immediate_jobs_pods_delete_exit_status:$immediate_jobs_pods_delete_exit,
            foreground_delete_exit_status:$delete_exit,
            zero_object_snapshot_exit_status:$snapshot_exit,
            remaining_labeled_jobs_and_pods:$remaining,verified_zero_before_t0:$valid}' \
          >"${RUN_DIR}/pre-t0-cancellation.json"
      fi
    else
      "${k[@]}" delete jobs,pods -n "$NAMESPACE" -l "benchmark.llm-d/run-id=${RECOVERY_RUN_ID}" \
        --ignore-not-found --cascade=foreground --wait=true --timeout=180s >/dev/null 2>&1 \
        || cleanup_error="failed to delete recovery driver Jobs/Pods"
    fi
  fi

  if [[ -n "$monitor_pid" ]] && kill -0 "$monitor_pid" 2>/dev/null; then
    kill "$monitor_pid" 2>/dev/null
    wait "$monitor_pid" 2>/dev/null
  fi
  for pid in "${checkpoint_pids[@]:-}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null
      wait "$pid" 2>/dev/null
    fi
  done
  if (( lock_acquired == 1 )); then
    owner=$("${k[@]}" get configmap "$LOCK_NAME" -n "$NAMESPACE" -o jsonpath='{.data.run-id}' 2>/dev/null)
    if [[ "$owner" == "$RECOVERY_RUN_ID" ]]; then
      "${k[@]}" delete configmap "$LOCK_NAME" -n "$NAMESPACE" --wait=true --timeout=60s >/dev/null 2>&1 \
        || cleanup_error="${cleanup_error:+${cleanup_error}; }failed to release benchmark lock"
    else
      cleanup_error="${cleanup_error:+${cleanup_error}; }benchmark lock ownership changed"
    fi
  fi
  if [[ -n "$cleanup_error" && $exit_status -eq 0 ]]; then
    exit_status=1
  fi
  if [[ -n "$cleanup_error" ]]; then
    status=cleanup_failed
  elif (( plan_complete == 1 )); then
    status=planned
  elif (( run_invalid == 1 )); then
    status=invalid
  elif (( measurement_complete == 1 )); then
    status=completed
  fi
  if (( run_dir_owned == 1 )); then
    jq -n \
      --arg run_id "$RECOVERY_RUN_ID" \
      --arg status "$status" \
      --arg decision "$final_decision" \
      --arg error "${last_error}${cleanup_error:+${last_error:+; }${cleanup_error}}" \
      --arg completed_at "$(date -u +%FT%TZ)" \
      --argjson exit_status "$exit_status" \
      '{schema_version:1,run_id:$run_id,status:$status,
        decision:(if $decision == "" then null else $decision end),
        exit_status:$exit_status,completed_at:$completed_at,
        error:(if $error == "" then null else $error end)}' \
      >"${RUN_DIR}/recovery-status.json"
  fi
  exit "$exit_status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap '[[ -n "$last_error" ]] || last_error="command failed at line ${LINENO}"' ERR

# Cluster-free executable seam for the fail-fast cleanup path.  It deliberately
# registers a live, stalled observer, reaches a synthetic hard deadline, and
# lets the real EXIT cleanup prove that labeled Job deletion starts before the
# observer is killed or waited.  Tests place a recording `oc` earlier in PATH.
if [[ "${1:-}" == --internal-observer-stall-fail-fast \
   || "${1:-}" == --internal-observer-late-stall-fail-fast ]]; then
  synthetic_mode=$1
  (( $# == 3 )) || exit 64
  synthetic_deadline_epoch_s=$2
  synthetic_t0_epoch_s=$3
  unsigned_integer synthetic_deadline_epoch_s "$synthetic_deadline_epoch_s"
  unsigned_integer synthetic_t0_epoch_s "$synthetic_t0_epoch_s"
  (( synthetic_deadline_epoch_s < synthetic_t0_epoch_s )) || exit 64
  [[ ! -e "$RUN_DIR" ]] || exit 64
  mkdir -p "$RUN_DIR"
  run_dir_owned=1
  jobs_created=1
  command_termination_grace_seconds=0
  zero_object_verification_budget_seconds=0
  t0_epoch_ms=$((synthetic_t0_epoch_s * 1000))
  if [[ "$synthetic_mode" == --internal-observer-late-stall-fail-fast ]]; then
    pre_t0_cancel_deadline_override_s=$((synthetic_deadline_epoch_s - 1))
  else
    pre_t0_cancel_deadline_override_s=$((synthetic_t0_epoch_s - 2))
  fi
  (
    trap 'exit 143' TERM
    while true; do sleep 0.1; done
  ) &
  checkpoint_pids=("$!")
  checkpoint_names=("synthetic-stall")
  printf '%s\n' "${checkpoint_pids[0]}" >"${RUN_DIR}/synthetic-observer.pid"
  while (( $(date -u +%s) < synthetic_deadline_epoch_s )); do sleep 0.1; done
  die "synthetic target-bound observer missed its pre-T0 completion deadline"
fi

for command in jq git python3; do require_command "$command"; done
if command -v sha256sum >/dev/null 2>&1; then
  sha256=(sha256sum)
elif command -v shasum >/dev/null 2>&1; then
  sha256=(shasum -a 256)
else
  die "required SHA-256 tool not found"
fi

sha256_path() {
  "${sha256[@]}" "$1" | awk '{print $1}'
}

sha256_stdin() {
  "${sha256[@]}" | awk '{print $1}'
}

[[ "$RECOVERY_RUN_ID" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] \
  || die "RECOVERY_RUN_ID must be a lower-case DNS label"
(( ${#RECOVERY_RUN_ID} <= 45 )) || die "RECOVERY_RUN_ID must be no more than 45 characters"
unsigned_integer RECOVERY_CYCLE_INDEX "$RECOVERY_CYCLE_INDEX"
(( RECOVERY_CYCLE_INDEX <= 1000000 )) || die "RECOVERY_CYCLE_INDEX is unreasonably large"
positive_integer START_LEAD_SECONDS "$START_LEAD_SECONDS"
(( START_LEAD_SECONDS >= 360 )) || die "START_LEAD_SECONDS must be at least 360"
for pair in \
  "MAX_ROWS:$MAX_ROWS" "JOB_SEQUENCE_SPAN:$JOB_SEQUENCE_SPAN" \
  "PHASE_MAX_IN_FLIGHT:$PHASE_MAX_IN_FLIGHT" "RPC_TIMEOUT_MS:$RPC_TIMEOUT_MS" \
  "MAX_DISPATCH_P99_LAG_MS:$MAX_DISPATCH_P99_LAG_MS" \
  "MAX_DRAIN_SECONDS:$MAX_DRAIN_SECONDS" "METRIC_MAX_GAP_SECONDS:$METRIC_MAX_GAP_SECONDS" \
  "METRIC_SETTLE_SECONDS:$METRIC_SETTLE_SECONDS" \
  "HEALTH_MONITOR_INTERVAL_SECONDS:$HEALTH_MONITOR_INTERVAL_SECONDS" \
  "CURL_CONNECT_TIMEOUT_SECONDS:$CURL_CONNECT_TIMEOUT_SECONDS" \
  "CURL_MAX_TIME_SECONDS:$CURL_MAX_TIME_SECONDS" \
  "TARGET_COUNTER_SETTLE_SECONDS:$TARGET_COUNTER_SETTLE_SECONDS" \
  "TARGET_BASELINE_QUIET_SECONDS:$TARGET_BASELINE_QUIET_SECONDS" \
  "TOPOLOGY_PREFLIGHT_TIMEOUT_SECONDS:$TOPOLOGY_PREFLIGHT_TIMEOUT_SECONDS"; do
  positive_integer "${pair%%:*}" "${pair#*:}"
done
for pair in "DISPATCH_LATE_AFTER_MS:$DISPATCH_LATE_AFTER_MS" "DROP_LATE_AFTER_MS:$DROP_LATE_AFTER_MS"; do
  unsigned_integer "${pair%%:*}" "${pair#*:}"
done
(( DROP_LATE_AFTER_MS >= DISPATCH_LATE_AFTER_MS )) || die "DROP_LATE_AFTER_MS must be at least DISPATCH_LATE_AFTER_MS"
(( MAX_ROWS == 10000 && JOB_SEQUENCE_SPAN == 10001 )) || die "the frozen recovery protocol requires MAX_ROWS=10000 and JOB_SEQUENCE_SPAN=10001"
(( TOKEN_COUNT == 64 )) || die "the frozen recovery protocol requires TOKEN_COUNT=64"
(( PHASE_MAX_IN_FLIGHT == 512 )) || die "the frozen recovery protocol requires PHASE_MAX_IN_FLIGHT=512"
(( DISPATCH_LATE_AFTER_MS == 1 && DROP_LATE_AFTER_MS == 100 && RPC_TIMEOUT_MS == 30000 )) \
  || die "the frozen recovery protocol requires scheduler thresholds 1ms/100ms and RPC timeout 30000ms"
(( MAX_DISPATCH_P99_LAG_MS == 5 && MAX_DRAIN_SECONDS == 90 && METRIC_MAX_GAP_SECONDS == 10 )) \
  || die "the frozen recovery protocol requires dispatch p99=5ms, drain=90s, and telemetry gap=10s"
(( HEALTH_MONITOR_INTERVAL_SECONDS <= 10 )) || die "health monitor interval cannot exceed 10 seconds"
[[ "$OC_REQUEST_TIMEOUT" =~ ^[1-9][0-9]*s$ ]] || die "OC_REQUEST_TIMEOUT must be a positive whole number of seconds"
(( ARMED_BARRIER_LEAD_SECONDS > TARGET_BOUND_SCHEDULE_LEAD_SECONDS \
   && TARGET_BOUND_SCHEDULE_LEAD_SECONDS > TARGET_BOUND_COMPLETION_LEAD_SECONDS \
   && TARGET_BOUND_COMPLETION_LEAD_SECONDS >= PRE_T0_CANCELLATION_COMPLETION_LEAD_SECONDS \
      + IMMEDIATE_CANCELLATION_REQUEST_TIMEOUT_SECONDS + COMMAND_TERMINATION_GRACE_SECONDS \
      + PRE_T0_FOREGROUND_DELETE_TIMEOUT_SECONDS + COMMAND_TERMINATION_GRACE_SECONDS \
      + PRE_T0_ZERO_OBJECT_VERIFICATION_BUDGET_SECONDS \
      + PRE_T0_CANCELLATION_SAFETY_MARGIN_SECONDS )) \
  || die "pre-T0 ARMED/checkpoint/cancellation margins are internally inconsistent"
(( CURL_CONNECT_TIMEOUT_SECONDS <= CURL_MAX_TIME_SECONDS )) \
  || die "CURL_CONNECT_TIMEOUT_SECONDS cannot exceed CURL_MAX_TIME_SECONDS"
(( TARGET_COUNTER_SETTLE_SECONDS >= 20 )) \
  || die "TARGET_COUNTER_SETTLE_SECONDS must cover at least two 10-second metrics-log intervals"
(( TARGET_BASELINE_QUIET_SECONDS >= 10 )) \
  || die "TARGET_BASELINE_QUIET_SECONDS must cover one metrics-log interval"
[[ "$PLAN_ONLY" == 0 || "$PLAN_ONLY" == 1 ]] || die "PLAN_ONLY must be 0 or 1"
[[ "$DELETE_COMPLETED_JOBS" == 0 || "$DELETE_COMPLETED_JOBS" == 1 ]] || die "DELETE_COMPLETED_JOBS must be 0 or 1"
[[ "$DRIVER_IMAGE" =~ @sha256:[0-9a-f]{64}$ ]] || die "DRIVER_IMAGE must be digest-pinned"
[[ "$TARGET_IMAGE" =~ ^sha256:[0-9a-f]{64}$ ]] || die "TARGET_IMAGE must be a sha256 digest"
for pair in "MODEL_SHA256:$MODEL_SHA256" "TOKENIZER_SHA256:$TOKENIZER_SHA256" "DRIVER_BUILD_SOURCE_SHA256:$DRIVER_BUILD_SOURCE_SHA256"; do
  [[ "${pair#*:}" =~ ^[0-9a-f]{64}$ ]] || die "${pair%%:*} must be a SHA-256 digest"
done
driver_probe_source=${DRIVER_PROBE_SOURCE:-${REPO_ROOT}/instrumentation/reference/src/bin/sustained-corpus-probe.rs}
[[ -s "$driver_probe_source" ]] || die "missing benchmark-driver probe source: ${driver_probe_source}"
local_probe_sha=$(sha256_path "$driver_probe_source")
local_source_matches_pinned=false
if [[ "$local_probe_sha" == "$DRIVER_BUILD_SOURCE_SHA256" ]]; then
  local_source_matches_pinned=true
fi
[[ -x "$SUMMARY_RUNNER" || -f "$SUMMARY_RUNNER" ]] || die "summary runner not found: ${SUMMARY_RUNNER}"

[[ ! -e "$RUN_DIR" ]] || die "refusing to overwrite existing run directory: ${RUN_DIR}"
mkdir -p "$RUN_DIR"
run_dir_owned=1
git -C "$REPO_ROOT" rev-parse HEAD >"${RUN_DIR}/git-head.txt"
git -C "$REPO_ROOT" status --short >"${RUN_DIR}/git-status.txt"

plan_created_epoch_ms=$(( $(date -u +%s) * 1000 ))
t0_epoch_ms=$((plan_created_epoch_ms + START_LEAD_SECONDS * 1000))
cycle_base=$((19000000000 + 150000 * RECOVERY_CYCLE_INDEX))
reserved_end=$((cycle_base + 150000))
arming_nonces_json=$(
  for ordinal in {0..13}; do
    printf '%s\n' "${RECOVERY_RUN_ID}|${RECOVERY_CYCLE_INDEX}|${ordinal}|${t0_epoch_ms}|${cycle_base}|${DRIVER_IMAGE}" \
      | sha256_stdin
  done | jq -Rsc 'split("\n")[:-1]'
)

jq -n \
  --arg run_id "$RECOVERY_RUN_ID" \
  --arg driver_image "$DRIVER_IMAGE" \
  --arg driver_source_sha256 "$DRIVER_BUILD_SOURCE_SHA256" \
  --arg armed_driver_image "$ARMED_DRIVER_IMAGE" \
  --arg armed_driver_source_sha256 "$ARMED_DRIVER_SOURCE_SHA256" \
  --arg driver_package_version "$DRIVER_PACKAGE_VERSION" \
  --arg target_image "$TARGET_IMAGE" \
  --arg model_sha256 "$MODEL_SHA256" \
  --arg tokenizer_sha256 "$TOKENIZER_SHA256" \
  --arg arming_protocol "$DRIVER_ARMING_PROTOCOL" \
  --argjson arming_supported "$DRIVER_ARMING_SUPPORTED" \
  --argjson local_source_matches_pinned "$local_source_matches_pinned" \
  --arg local_source_sha256 "$local_probe_sha" \
  --argjson arming_nonces "$arming_nonces_json" \
  --argjson cycle_index "$RECOVERY_CYCLE_INDEX" \
  --argjson created "$plan_created_epoch_ms" \
  --argjson t0 "$t0_epoch_ms" \
  --argjson cycle_base "$cycle_base" \
  --argjson reserved_end "$reserved_end" \
  --argjson armed_barrier_lead "$ARMED_BARRIER_LEAD_SECONDS" \
  --argjson target_bound_schedule_lead "$TARGET_BOUND_SCHEDULE_LEAD_SECONDS" \
  --argjson target_bound_completion_lead "$TARGET_BOUND_COMPLETION_LEAD_SECONDS" \
  --argjson cancellation_completion_lead "$PRE_T0_CANCELLATION_COMPLETION_LEAD_SECONDS" \
  --argjson foreground_delete_timeout "$PRE_T0_FOREGROUND_DELETE_TIMEOUT_SECONDS" \
  --argjson zero_object_verification_budget "$PRE_T0_ZERO_OBJECT_VERIFICATION_BUDGET_SECONDS" \
  --argjson cancellation_safety_margin "$PRE_T0_CANCELLATION_SAFETY_MARGIN_SECONDS" \
  --argjson max_in_flight "$PHASE_MAX_IN_FLIGHT" '
  def job($ordinal;$phase;$rate;$duration;$start;$slots;$probe_offset;$mif):
    {ordinal:$ordinal,name:("scr-"+$run_id+"-j"+($ordinal|tostring|if length==1 then "0"+. else . end)),
     phase:$phase,offered_rps:$rate,duration_seconds:$duration,start_epoch_ms:$start,
     expected_slots:$slots,recovery_offset_seconds:$probe_offset,
     sequence_base:($cycle_base + 10001*$ordinal),candidate_rows:10000,
     warmup_requests:0,max_in_flight:$mif,arming_nonce:$arming_nonces[$ordinal]};
  ([0,1,2,3,5,8,13,21,34,55,89]) as $offsets
  | ([job(0;"pre";"35";180;$t0;6300;null;$max_in_flight),
      job(1;"overload";"47";120;($t0+185000);5640;null;$max_in_flight)]
     + [$offsets|to_entries[]|job((.key+2);"recovery_probe";"1";1;($t0+305000+.value*1000);1;.value;1)]
     + [job(13;"post";"35";180;($t0+400000);6300;null;$max_in_flight)]) as $jobs
  | {schema_version:1,protocol:"same_pod_open_loop_recovery_v1",run_id:$run_id,
     cycle_index:$cycle_index,created_epoch_ms:$created,t0_epoch_ms:$t0,
     invariants:{one_target_pod_uid:true,one_direct_pod_ip:true,zero_warmup_arrivals:true,
       all_jobs_precreated_suspended:true,target_source_unchanged:true},
     target_shape:{inference_workers:"1",rayon_num_threads:"1",candle_num_threads:"unset",metrics_log_seconds:"10",
       qos_class:"Guaranteed",resources:{requests:{cpu:"2",memory:"4Gi"},limits:{cpu:"2",memory:"4Gi"}},
       runtime_cpu_max:"max",runtime_cpuset_logical_cpus:2,complete_smt_sibling_sets:true,
       runtime_pid1_executable:"/usr/local/bin/llm-d-sc",runtime_environment_verified:true},
     arming:{required:true,protocol:$arming_protocol,pinned_driver_supports_protocol:$arming_supported,
       live_executable:$arming_supported,pair_matches_allowlist:$arming_supported,
       allowlist:{driver_image:$armed_driver_image,driver_source_sha256:$armed_driver_source_sha256},
       evidence_source:"one pre-start JSON stdout record per driver process",
       validation_contract:{records:14,deadline:("T0-"+($armed_barrier_lead|tostring)+"s"),all_jobs_required:true,
         schema:"llm-d-sc.benchmark-driver.armed",schema_version:1,record_type:"ARMED",
         explicit_config_required:true,all_config_fields_must_match_frozen_job:true,
         digest_role:"recorded pinned-driver provenance; explicit config equality authorizes load",
         required_fields:["run_id","job_id","nonce","endpoint","scheduled_start_epoch_ms","expected_slots","duration_seconds","armed_epoch_ms","scheduled_rows_blake3","config","config_digest"],
         release_rule:"no arrival may be authorized unless all 14 records match the frozen plan and bound target"},
       blocker:(if $arming_supported then null else "driver image/source pair is not the exact smoke-validated ARMED allowlist" end)},
     pinned:{driver_image:$driver_image,driver_source_sha256:$driver_source_sha256,
       driver_package_version:$driver_package_version,
       target_image:$target_image,model_sha256:$model_sha256,tokenizer_sha256:$tokenizer_sha256,
       local_driver_source_sha256:$local_source_sha256,local_source_matches_pinned:$local_source_matches_pinned},
     sequence_reservation:{formula:"C_r = 19000000000 + 150000*r",cycle_base:$cycle_base,
       job_span:10001,reserved_end_exclusive:$reserved_end},
     phases:{pre:{rate:35,duration_seconds:180},no_arrival_gap_seconds:5,
       overload:{rate:47,duration_seconds:120},recovery_window_seconds:90,
       post_gap_seconds:5,post:{rate:35,duration_seconds:180}},
     jobs:$jobs,
     checkpoints:[
       {name:"target-bound",
        scheduled_epoch_ms:($t0-$target_bound_schedule_lead*1000),
        completion_deadline_epoch_ms:($t0-$target_bound_completion_lead*1000),
        load_authorizing:true},
       {name:"pre-mid",scheduled_epoch_ms:($t0+90000)},
       {name:"gap-mid",scheduled_epoch_ms:($t0+182000)},
       {name:"overload-mid",scheduled_epoch_ms:($t0+245000)},
       {name:"recovery-30",scheduled_epoch_ms:($t0+335000)},
       {name:"recovery-50",scheduled_epoch_ms:($t0+355000)},
       {name:"post-mid",scheduled_epoch_ms:($t0+490000)},
       {name:"post-after",scheduled_epoch_ms:($t0+582000)}],
     telemetry_window:{start_epoch_s:(($t0/1000)-30),end_epoch_s:(($t0/1000)+610),step_seconds:5},
     gates:{scheduler_p99_lag_ms_max:5,schedule_drop_ratio_max:0,driver_in_flight_drop_ratio_max:0,
       drain_seconds_max:90,steady_success_min:0.999,steady_drain_max:0.001,
       target_bound_schedule_lead_seconds:$target_bound_schedule_lead,
       target_bound_completion_lead_seconds:$target_bound_completion_lead,
       pre_t0_cancellation_completion_lead_seconds:$cancellation_completion_lead,
       pre_t0_foreground_delete_timeout_seconds_max:$foreground_delete_timeout,
       pre_t0_zero_object_verification_budget_seconds:$zero_object_verification_budget,
       pre_t0_cancellation_safety_margin_seconds:$cancellation_safety_margin,
       post_pre_useful_relative_delta_max:0.02,post_p50_ratio_max:1.10,post_p99_ratio_max:1.20,
       overload_queue_ratio_min_exclusive:10,overload_drain_ratio_min_exclusive:0.01,
       target_counter_tolerance:0,
       recovery_rtt_us_max:"max(2 * pre p99, 50000)",recovery_green_seconds_max:34,
       recovery_amber_seconds_max:55,recovery_last_probe_seconds:89}}' \
  >"${RUN_DIR}/recovery-plan.json"

"$SUMMARY_RUNNER" --help >/dev/null 2>&1 || die "summary runner cannot start"
if (( PLAN_ONLY == 1 )); then
  plan_complete=1
  jq . "${RUN_DIR}/recovery-plan.json"
  exit 0
fi

[[ "$DRIVER_ARMING_SUPPORTED" == true ]] \
  || die "live execution blocked: driver image/source pair is not the exact smoke-validated allowlist for ${DRIVER_ARMING_PROTOCOL}; Kubernetes Pod Ready is not an application-level ARMED barrier"
[[ "$local_source_matches_pinned" == true ]] \
  || die "live execution blocked: local driver source does not match the digest-pinned driver build provenance"

for command in oc curl; do require_command "$command"; done
[[ -x "$TOPOLOGY_PREFLIGHT_RUNNER" ]] || die "topology runner is not executable: ${TOPOLOGY_PREFLIGHT_RUNNER}"

for node in "$TARGET_NODE" "$DRIVER_NODE"; do
  "${k[@]}" wait --for=condition=Ready "node/${node}" --timeout=60s >/dev/null \
    || die "node ${node} is not Ready"
done

lock_manifest=$(jq -cn \
  --arg name "$LOCK_NAME" --arg namespace "$NAMESPACE" --arg run_id "$RECOVERY_RUN_ID" \
  '{apiVersion:"v1",kind:"ConfigMap",metadata:{name:$name,namespace:$namespace},
    data:{"run-id":$run_id,"kind":"same-pod-recovery-cycle"}}')
if ! printf '%s\n' "$lock_manifest" | "${k[@]}" create -f - >/dev/null; then
  die "benchmark lock ${LOCK_NAME} is already held"
fi
lock_acquired=1

"${k[@]}" get deployment "$DEPLOYMENT" -n "$NAMESPACE" -o json >"${RUN_DIR}/deployment-before.json"
"${k[@]}" get pods -n "$NAMESPACE" -l "$TARGET_SELECTOR" -o json >"${RUN_DIR}/targets-before.json"
"${k[@]}" get nodes "$TARGET_NODE" "$DRIVER_NODE" -o json >"${RUN_DIR}/nodes-before.json"
"${k[@]}" get events -n "$NAMESPACE" -o json >"${RUN_DIR}/events-before.json"
jq -e '.spec.replicas==1 and (.status.availableReplicas//0)==1 and (.status.updatedReplicas//0)==1' \
  "${RUN_DIR}/deployment-before.json" >/dev/null \
  || die "target Deployment must be stable at exactly one available replica"
jq -e --arg container "$TARGET_CONTAINER" '
  [.spec.template.spec.containers[]? | select(.name==$container)] as $containers
  | ($containers|length)==1
  and ([$containers[0].env[]? | select(.name=="LLM_D_SC_INFERENCE_WORKERS")]|length)==1
  and ([$containers[0].env[]? | select(.name=="LLM_D_SC_INFERENCE_WORKERS")][0].value)=="1"
  and ([$containers[0].env[]? | select(.name=="RAYON_NUM_THREADS")]|length)==1
  and ([$containers[0].env[]? | select(.name=="RAYON_NUM_THREADS")][0].value)=="1"
  and ([$containers[0].env[]? | select(.name=="CANDLE_NUM_THREADS")]|length)==0
  and ([$containers[0].env[]? | select(.name=="LLM_D_SC_METRICS_LOG_SECS")]|length)==1
  and ([$containers[0].env[]? | select(.name=="LLM_D_SC_METRICS_LOG_SECS")][0].value)=="10"
  and (($containers[0].envFrom//[])|length)==0
  and (($containers[0].command//[])|length)==0
  and (($containers[0].args//[])|length)==0
  and $containers[0].resources=={"requests":{"cpu":"2","memory":"4Gi"},
    "limits":{"cpu":"2","memory":"4Gi"}}' \
  "${RUN_DIR}/deployment-before.json" >/dev/null \
  || die "target Deployment is not exact W1/RT1/Candle-unset with 2 CPU/4Gi requests and limits"

jq -e \
  --arg node "$TARGET_NODE" --arg digest "$TARGET_IMAGE" --arg container "$TARGET_CONTAINER" '
  (.items|length)==1
  and .items[0].metadata.deletionTimestamp == null
  and .items[0].status.phase == "Running"
  and (.items[0].status.podIP|type=="string" and length>0)
  and .items[0].spec.nodeName == $node
  and .items[0].status.qosClass == "Guaranteed"
  and any(.items[0].status.conditions[]?; .type=="Ready" and .status=="True")
  and ([.items[0].status.containerStatuses[]?|select(.name==$container)]|length)==1
  and ([.items[0].status.containerStatuses[]?|select(.name==$container)][0].restartCount)==0
  and ([.items[0].status.containerStatuses[]?|select(.name==$container)][0].imageID|endswith($digest))' \
  "${RUN_DIR}/targets-before.json" >/dev/null \
  || die "pre-load target must be exactly one Ready, restart-free Pod on the pinned node/image"

target_pod=$(jq -r '.items[0].metadata.name' "${RUN_DIR}/targets-before.json")
target_uid=$(jq -r '.items[0].metadata.uid' "${RUN_DIR}/targets-before.json")
target_ip=$(jq -r '.items[0].status.podIP' "${RUN_DIR}/targets-before.json")
target_image_id=$(jq -r --arg container "$TARGET_CONTAINER" '[.items[0].status.containerStatuses[]|select(.name==$container)][0].imageID' "${RUN_DIR}/targets-before.json")
target_ready_transition=$(jq -r '.items[0].status.conditions[]|select(.type=="Ready" and .status=="True")|.lastTransitionTime' "${RUN_DIR}/targets-before.json")
target_started_at=$(jq -r --arg container "$TARGET_CONTAINER" '[.items[0].status.containerStatuses[]|select(.name==$container)][0].state.running.startedAt' "${RUN_DIR}/targets-before.json")
target_ready_epoch_ms=$(python3 -c 'from datetime import datetime; import sys; print(int(datetime.fromisoformat(sys.argv[1].replace("Z", "+00:00")).timestamp()*1000))' "$target_ready_transition")
(( t0_epoch_ms - target_ready_epoch_ms >= ARMED_BARRIER_LEAD_SECONDS * 1000 )) \
  || die "T0 must be at least ${ARMED_BARRIER_LEAD_SECONDS} seconds after the exact target Pod became Ready"

"${k[@]}" logs -n "$NAMESPACE" "$target_pod" -c "$TARGET_CONTAINER" --timestamps=true --since-time="$target_started_at" \
  >"${RUN_DIR}/target-logs-before.txt"
if grep -q 'llm-d-sc metrics:' "${RUN_DIR}/target-logs-before.txt"; then
  die "target Pod has prior classification traffic; cumulative queue telemetry is not isolatable"
fi
sleep "$TARGET_BASELINE_QUIET_SECONDS"
"${k[@]}" logs -n "$NAMESPACE" "$target_pod" -c "$TARGET_CONTAINER" --timestamps=true --since-time="$target_started_at" \
  >"${RUN_DIR}/target-logs-before.txt"
if grep -q 'llm-d-sc metrics:' "${RUN_DIR}/target-logs-before.txt"; then
  die "target Pod emitted classification counters during the traffic-clean quiet interval"
fi
jq -n --arg target_uid "$target_uid" --arg target_ip "$target_ip" \
  --arg started_at "$target_started_at" --arg log_sha256 "$(sha256_path "${RUN_DIR}/target-logs-before.txt")" \
  --argjson captured "$(( $(date -u +%s) * 1000 ))" \
  --argjson quiet "$TARGET_BASELINE_QUIET_SECONDS" \
  '{schema_version:1,target_uid:$target_uid,target_ip:$target_ip,container_started_at:$started_at,
    captured_epoch_ms:$captured,quiet_interval_seconds:$quiet,log_sha256:$log_sha256,
    traffic_clean:true,counters:{served:0,hits:0,misses:0}}' \
  >"${RUN_DIR}/target-counter-baseline.json"

topology_stdout="${RUN_DIR}/topology-preflight-stdout.txt"
topology_stderr="${RUN_DIR}/topology-preflight-stderr.txt"
topology_report="${RUN_DIR}/topology-preflight-report.json"
topology_execution="${RUN_DIR}/topology-preflight-execution.json"
set +e
run_with_timeout "$TOPOLOGY_PREFLIGHT_TIMEOUT_SECONDS" "$TOPOLOGY_PREFLIGHT_RUNNER" live \
  --kubeconfig "$KUBECONFIG_PATH" --namespace "$NAMESPACE" \
  --selector "$TARGET_SELECTOR" --expected-pods 1 --container "$TARGET_CONTAINER" --format json \
  >"$topology_stdout" 2>"$topology_stderr"
topology_exit=$?
set -e
topology_valid=false
if jq -s -e 'length==1 and (.[0]|type)=="object"' "$topology_stdout" >/dev/null 2>&1; then
  jq -s '.[0]' "$topology_stdout" >"$topology_report"
  if (( topology_exit == 0 )) && jq -e \
      --arg name "$target_pod" --arg uid "$target_uid" --arg node "$TARGET_NODE" '
      .schema_version==1 and .verdict=="PASS" and .placement_verdict=="PASS"
      and .gate_passed==true and .exit_code==0 and .snapshot.capture.mode=="live-read-only"
      and .summary.pods==1 and .summary.pods_validated==1
      and .summary.placement_violations==0 and .summary.invalid_reasons==0
      and .summary.gate_ineligibility_reasons==0
      and (.pods|length)==1 and .pods[0].name==$name and .pods[0].uid==$uid
      and .pods[0].node==$node and (.pods[0].cpuset|type=="string" and length>0)
      and .pods[0].complete_smt_sibling_sets==true' \
      "$topology_report" >/dev/null; then
    topology_valid=true
  fi
fi
report_sha=""
if [[ -s "$topology_report" ]]; then report_sha=$(sha256_path "$topology_report"); fi
jq -n \
  --argjson runner_exit "$topology_exit" \
  --argjson valid "$topology_valid" \
  --arg report_sha "$report_sha" \
  --arg stdout_sha "$(sha256_path "$topology_stdout")" \
  --arg stderr_sha "$(sha256_path "$topology_stderr")" \
  '{schema_version:1,gate:"cpu_topology_pre_load",runner_exit_code:$runner_exit,
    report_json_valid:($report_sha!=""),report_gate_valid:$valid,target_identity_match:$valid,
    load_authorized:$valid,disposition:(if $valid then "pass" else "invalid_pre_load" end),
    evidence_sha256:{report:(if $report_sha=="" then null else $report_sha end),
      raw_stdout:$stdout_sha,stderr:$stderr_sha}}' >"$topology_execution"
[[ "$topology_valid" == true ]] || die "CPU topology preflight denied load"
target_cpuset=$(jq -r '.pods[0].cpuset' "$topology_report")
target_cpuset_count=$(awk -F, '{n=0; for(i=1;i<=NF;i++){split($i,a,"-"); n+=(a[2]==""?1:a[2]-a[1]+1)}; print n}' <<<"$target_cpuset")
[[ "$target_cpuset_count" == 2 ]] \
  || die "exact 2-CPU target shape requires an effective cpuset with two logical CPUs"

"${k[@]}" exec -n "$NAMESPACE" "$target_pod" -c "$TARGET_CONTAINER" -- sh -c '
  printf "cpuset_cpus_effective "; cat /sys/fs/cgroup/cpuset.cpus.effective
  printf "cpu_max "; cat /sys/fs/cgroup/cpu.max
  cat /sys/fs/cgroup/cpu.stat' >"${RUN_DIR}/runtime-cgroup-before.txt" \
  || die "cannot capture the target runtime cgroup before load"
runtime_cpuset=$(awk '$1=="cpuset_cpus_effective"{print $2}' "${RUN_DIR}/runtime-cgroup-before.txt")
runtime_cpu_max=$(awk '$1=="cpu_max"{$1=""; sub(/^ /,""); print}' "${RUN_DIR}/runtime-cgroup-before.txt")
[[ "$runtime_cpuset" == "$target_cpuset" ]] \
  || die "runtime cpuset differs from topology preflight"
[[ "${runtime_cpu_max%% *}" == max ]] \
  || die "runtime cpu.max is quota-limited; exact benchmark shape requires cpu.max=max"
jq -n --arg cpuset "$runtime_cpuset" --arg cpu_max "$runtime_cpu_max" \
  --argjson logical_cpus "$target_cpuset_count" \
  '{schema_version:1,cpuset_cpus_effective:$cpuset,cpu_max:$cpu_max,
    cpu_max_quota:($cpu_max|split(" ")[0]),logical_cpus:$logical_cpus}' \
  >"${RUN_DIR}/runtime-cgroup-before.json"

"${k[@]}" exec -n "$NAMESPACE" "$target_pod" -c "$TARGET_CONTAINER" -- sh -c '
  printf "pid1_exe "; readlink /proc/1/exe
  tr "\000" "\n" </proc/1/environ | awk -F= "\$1==\"LLM_D_SC_INFERENCE_WORKERS\" || \$1==\"RAYON_NUM_THREADS\" || \$1==\"CANDLE_NUM_THREADS\" || \$1==\"LLM_D_SC_METRICS_LOG_SECS\""' \
  >"${RUN_DIR}/runtime-process-before.txt" \
  || die "cannot capture the exact PID1 executable/environment before load"
runtime_pid1_exe=$(awk '$1=="pid1_exe"{print $2}' "${RUN_DIR}/runtime-process-before.txt")
runtime_workers=$(awk -F= '$1=="LLM_D_SC_INFERENCE_WORKERS"{print substr($0,index($0,"=")+1)}' "${RUN_DIR}/runtime-process-before.txt")
runtime_rayon=$(awk -F= '$1=="RAYON_NUM_THREADS"{print substr($0,index($0,"=")+1)}' "${RUN_DIR}/runtime-process-before.txt")
runtime_metrics_log=$(awk -F= '$1=="LLM_D_SC_METRICS_LOG_SECS"{print substr($0,index($0,"=")+1)}' "${RUN_DIR}/runtime-process-before.txt")
runtime_workers_count=$(awk -F= '$1=="LLM_D_SC_INFERENCE_WORKERS"{n++} END{print n+0}' "${RUN_DIR}/runtime-process-before.txt")
runtime_rayon_count=$(awk -F= '$1=="RAYON_NUM_THREADS"{n++} END{print n+0}' "${RUN_DIR}/runtime-process-before.txt")
runtime_metrics_log_count=$(awk -F= '$1=="LLM_D_SC_METRICS_LOG_SECS"{n++} END{print n+0}' "${RUN_DIR}/runtime-process-before.txt")
runtime_candle_count=$(awk -F= '$1=="CANDLE_NUM_THREADS"{n++} END{print n+0}' "${RUN_DIR}/runtime-process-before.txt")
[[ "$runtime_pid1_exe" == /usr/local/bin/llm-d-sc \
   && "$runtime_workers_count" == 1 && "$runtime_workers" == 1 \
   && "$runtime_rayon_count" == 1 && "$runtime_rayon" == 1 \
   && "$runtime_metrics_log_count" == 1 && "$runtime_metrics_log" == 10 \
   && "$runtime_candle_count" == 0 ]] \
  || die "actual PID1 executable/environment is not exact W1/RT1/Candle-unset/metrics-log=10"
jq -n --arg executable "$runtime_pid1_exe" --arg workers "$runtime_workers" \
  --arg rayon "$runtime_rayon" --arg metrics_log "$runtime_metrics_log" \
  '{schema_version:1,pid1_executable:$executable,
    environment:{LLM_D_SC_INFERENCE_WORKERS:$workers,RAYON_NUM_THREADS:$rayon,
      LLM_D_SC_METRICS_LOG_SECS:$metrics_log,CANDLE_NUM_THREADS:null},
    candle_num_threads_present:false}' >"${RUN_DIR}/runtime-process-before.json"

if [[ "$TARGET_NODE" == "$DRIVER_NODE" ]]; then
  topology="same-node-direct-${TARGET_NODE}"
else
  topology="cross-node-direct-${TARGET_NODE}-from-${DRIVER_NODE}"
fi

jq -n \
  --arg run_id "$RECOVERY_RUN_ID" --arg namespace "$NAMESPACE" --arg deployment "$DEPLOYMENT" \
  --arg target_container "$TARGET_CONTAINER" --arg target_name "$target_pod" \
  --arg target_uid "$target_uid" --arg target_ip "$target_ip" --arg target_node "$TARGET_NODE" \
  --arg target_image_id "$target_image_id" --arg ready_transition "$target_ready_transition" --arg started_at "$target_started_at" \
  --arg cpuset "$target_cpuset" --arg cpu_max "$runtime_cpu_max" --arg target_image "$TARGET_IMAGE" \
  --arg baseline_sha "$(jq -r .log_sha256 "${RUN_DIR}/target-counter-baseline.json")" \
  --arg driver_node "$DRIVER_NODE" --arg driver_image "$DRIVER_IMAGE" \
  --arg driver_source "$DRIVER_BUILD_SOURCE_SHA256" --arg driver_package_version "$DRIVER_PACKAGE_VERSION" --arg model "$MODEL_SHA256" \
  --arg tokenizer "$TOKENIZER_SHA256" --arg topology "$topology" \
  --argjson plan_created "$plan_created_epoch_ms" \
  --argjson cpuset_count "$target_cpuset_count" --argjson counter_tolerance "$TARGET_COUNTER_TOLERANCE" \
  --argjson dispatch_late "$DISPATCH_LATE_AFTER_MS" --argjson drop_late "$DROP_LATE_AFTER_MS" \
  --argjson rpc_timeout "$RPC_TIMEOUT_MS" --argjson max_dispatch "$MAX_DISPATCH_P99_LAG_MS" \
  --argjson max_drain "$MAX_DRAIN_SECONDS" '
  {schema_version:1,run_id:$run_id,namespace:$namespace,deployment:$deployment,
   plan_created_epoch_ms:$plan_created,target_container:$target_container,
   target:{name:$target_name,uid:$target_uid,ip:$target_ip,node:$target_node,
     image_id:$target_image_id,ready_transition_time:$ready_transition,container_started_at:$started_at,
     cpuset_cpus_effective:$cpuset,cpu_max:$cpu_max},
   target_shape:{inference_workers:"1",rayon_num_threads:"1",candle_num_threads:"unset",metrics_log_seconds:"10",
     qos_class:"Guaranteed",resources:{requests:{cpu:"2",memory:"4Gi"},limits:{cpu:"2",memory:"4Gi"}},
     runtime:{cpu_max_quota:"max",cpuset_logical_cpus:$cpuset_count,complete_smt_sibling_sets:true,
       pid1_executable:"/usr/local/bin/llm-d-sc",environment_verified:true}},
   counter_attribution:{baseline_log_sha256:$baseline_sha,baseline:{served:0,hits:0,misses:0},
     tolerance:$counter_tolerance,tolerance_justification:"zero: a traffic-clean Pod, exclusive lock, completed drivers, and two metrics-log settle intervals permit exact reconciliation"},
   target_image:$target_image,driver_node:$driver_node,driver_image:$driver_image,
   driver_source_sha256:$driver_source,driver_package_version:$driver_package_version,
   model_sha256:$model,tokenizer_sha256:$tokenizer,
   topology:$topology,
   scheduler_thresholds:{dispatch_late_after_ms:$dispatch_late,drop_late_after_ms:$drop_late,
     rpc_timeout_ms:$rpc_timeout,max_dispatch_p99_lag_ms:$max_dispatch,max_drain_seconds:$max_drain},
   service_thresholds:{steady_success_min:0.999,steady_drain_max:0.001,
     post_useful_relative_delta_max:0.02,post_p50_ratio_max:1.10,post_p99_ratio_max:1.20,
     overload_queue_ratio_min_exclusive:10,overload_drain_ratio_min_exclusive:0.01}}' \
  >"${RUN_DIR}/run-provenance.json"

mkdir -p "${RUN_DIR}/job-manifests" "${RUN_DIR}/drivers" "${RUN_DIR}/checkpoints" "${RUN_DIR}/metrics"
while IFS= read -r job_spec; do
  job_name=$(jq -r .name <<<"$job_spec")
  job_phase=$(jq -r .phase <<<"$job_spec")
  job_rate=$(jq -r .offered_rps <<<"$job_spec")
  job_duration=$(jq -r .duration_seconds <<<"$job_spec")
  job_start=$(jq -r .start_epoch_ms <<<"$job_spec")
  job_sequence=$(jq -r .sequence_base <<<"$job_spec")
  job_mif=$(jq -r .max_in_flight <<<"$job_spec")
  job_ordinal=$(jq -r .ordinal <<<"$job_spec")
  job_arming_nonce=$(jq -r .arming_nonce <<<"$job_spec")
  "${k[@]}" create job "$job_name" -n "$NAMESPACE" --image="$DRIVER_IMAGE" --dry-run=client -o json \
    | jq \
      --arg run "$RECOVERY_RUN_ID" --arg phase "$job_phase" --arg ordinal "$job_ordinal" \
      --arg target_pod "$target_pod" --arg target_uid "$target_uid" --arg target_ip "$target_ip" \
      --arg target_image "$TARGET_IMAGE" --arg model "$MODEL_SHA256" --arg tokenizer "$TOKENIZER_SHA256" \
      --arg topology "$topology" --arg driver_node "$DRIVER_NODE" --arg driver_image "$DRIVER_IMAGE" \
      --arg start "$job_start" --arg duration "$job_duration" --arg sequence "$job_sequence" \
      --arg rate "$job_rate" --arg max_in_flight "$job_mif" --arg token_count "$TOKEN_COUNT" \
      --arg max_rows "$MAX_ROWS" --arg dispatch_late "$DISPATCH_LATE_AFTER_MS" \
      --arg drop_late "$DROP_LATE_AFTER_MS" --arg rpc_timeout "$RPC_TIMEOUT_MS" \
      --arg armed_run_id "$RECOVERY_RUN_ID" --arg armed_job_id "$job_name" \
      --arg armed_nonce "$job_arming_nonce" '
      .metadata.labels += {"benchmark.llm-d/run-id":$run,
        "benchmark.llm-d/component":"same-pod-recovery-driver",
        "benchmark.llm-d/phase":$phase,"benchmark.llm-d/job-ordinal":$ordinal}
      | .metadata.annotations += {"benchmark.llm-d/target-pod":$target_pod,
        "benchmark.llm-d/target-uid":$target_uid,"benchmark.llm-d/target-ip":$target_ip,
        "benchmark.llm-d/start-epoch-ms":$start}
      | .spec.suspend=true | .spec.backoffLimit=0 | .spec.activeDeadlineSeconds=1200
      | .spec.ttlSecondsAfterFinished=86400
      | .spec.template.metadata.labels += {"benchmark.llm-d/run-id":$run,
        "benchmark.llm-d/component":"same-pod-recovery-driver",
        "benchmark.llm-d/phase":$phase,"benchmark.llm-d/job-ordinal":$ordinal}
      | .spec.template.spec.nodeSelector={"kubernetes.io/hostname":$driver_node}
      | .spec.template.spec.securityContext={"runAsNonRoot":true,"seccompProfile":{"type":"RuntimeDefault"}}
      | .spec.template.spec.containers[0].command=["/usr/local/bin/llm-d-sc-sustained-corpus-probe"]
      | .spec.template.spec.containers[0].args=[
          "--target",($target_ip+":50051"),"--token-count",$token_count,
          "--sequence-base",$sequence,"--max-rows",$max_rows,
          "--tokenizer-sha256",$tokenizer,"--concurrency","1","--connections","1",
          "--warmup-requests","0","--duration-seconds",$duration,"--start-epoch-ms",$start,
          "--target-image",$target_image,"--model-sha256",$model,"--topology",$topology,
          "--raw-latencies","--driver-image",$driver_image,"--offered-rps",$rate,
          "--max-in-flight",$max_in_flight,"--dispatch-late-after-ms",$dispatch_late,
          "--drop-late-after-ms",$drop_late,"--rpc-timeout-ms",$rpc_timeout]
      | .spec.template.spec.containers[0].args += ["--armed-run-id",$armed_run_id,
          "--armed-job-id",$armed_job_id,"--armed-nonce",$armed_nonce]
      | .spec.template.spec.containers[0].resources={"requests":{"cpu":"500m","memory":"256Mi"},
          "limits":{"cpu":"4","memory":"1Gi"}}
      | .spec.template.spec.containers[0].securityContext={"allowPrivilegeEscalation":false,
          "readOnlyRootFilesystem":true,"capabilities":{"drop":["ALL"]}}' \
    >"${RUN_DIR}/job-manifests/${job_name}.json"
  "${k[@]}" create -f "${RUN_DIR}/job-manifests/${job_name}.json" >/dev/null \
    || die "failed to precreate ${job_name}"
  jobs_created=1
done < <(jq -c '.jobs[]' "${RUN_DIR}/recovery-plan.json")

"${k[@]}" get jobs -n "$NAMESPACE" -l "benchmark.llm-d/run-id=${RECOVERY_RUN_ID}" -o json \
  >"${RUN_DIR}/jobs-precreated.json"
jq -e '(.items|length)==14 and all(.items[];.spec.suspend==true)' "${RUN_DIR}/jobs-precreated.json" >/dev/null \
  || die "all 14 future Jobs were not captured suspended"

for job_name in $(jq -r '.jobs[].name' "${RUN_DIR}/recovery-plan.json"); do
  "${k[@]}" patch job "$job_name" -n "$NAMESPACE" --type=merge -p '{"spec":{"suspend":false}}' >/dev/null \
    || die "failed to release ${job_name}"
done

driver_ready_deadline=$((t0_epoch_ms / 1000 - ARMED_BARRIER_LEAD_SECONDS))
mkdir -p "${RUN_DIR}/arming"
driver_ready=0
armed_count=0
while (( $(date -u +%s) < driver_ready_deadline )); do
  driver_pods_json=$("${k[@]}" get pods -n "$NAMESPACE" -l "benchmark.llm-d/run-id=${RECOVERY_RUN_ID}" -o json)
  driver_ready=$(jq '[.items[]|select(.status.phase=="Running")|select(any(.status.conditions[]?;.type=="Ready" and .status=="True"))]|length' <<<"$driver_pods_json")
  while IFS=$'\t' read -r ordinal job_name nonce endpoint start slots duration rate max_in_flight sequence; do
    armed_file="${RUN_DIR}/arming/j${ordinal}.json"
    [[ ! -s "$armed_file" ]] || continue
    stream_file="${RUN_DIR}/arming/j${ordinal}.stdout-so-far"
    if "${k[@]}" logs -n "$NAMESPACE" job/"$job_name" >"$stream_file" 2>/dev/null; then
      if jq -s -e --arg protocol "$DRIVER_ARMING_PROTOCOL" --arg run_id "$RECOVERY_RUN_ID" \
          --arg job_id "$job_name" --arg nonce "$nonce" --arg endpoint "$endpoint" \
          --arg rate "$rate" --arg driver_image "$DRIVER_IMAGE" --arg target_image "$TARGET_IMAGE" \
          --arg model "$MODEL_SHA256" --arg tokenizer "$TOKENIZER_SHA256" --arg topology "$topology" \
          --arg driver_package_version "$DRIVER_PACKAGE_VERSION" \
          --argjson start "$start" --argjson slots "$slots" --argjson duration "$duration" \
          --argjson max_in_flight "$max_in_flight" --argjson sequence "$sequence" \
          --argjson rate_numerator "$rate" \
          --argjson created "$plan_created_epoch_ms" \
          --argjson deadline "$((t0_epoch_ms - ARMED_BARRIER_LEAD_SECONDS * 1000))" '
          [.[] | select(.schema=="llm-d-sc.benchmark-driver.armed" and .schema_version==1
            and .record_type=="ARMED" and .protocol_version==$protocol)] as $records
          | ($records|length)==1
          and $records[0].run_id==$run_id and $records[0].job_id==$job_id
          and $records[0].nonce==$nonce and $records[0].endpoint==$endpoint
          and $records[0].scheduled_start_epoch_ms==$start
          and $records[0].expected_slots==$slots and $records[0].duration_seconds==$duration
          and ($records[0].armed_epoch_ms|type)=="number"
          and $records[0].armed_epoch_ms >= $created and $records[0].armed_epoch_ms <= $deadline
          and ($records[0].scheduled_rows_blake3|test("^[0-9a-f]{64}$"))
          and ($records[0].config.selected_rows_blake3|test("^[0-9a-f]{64}$"))
          and $records[0].config.scheduled_rows_blake3==$records[0].scheduled_rows_blake3
          and ($records[0].config | del(.selected_rows_blake3,.scheduled_rows_blake3))=={
            candidate_rows:10000,closed_loop_concurrency_argument:1,connections:1,
            corpus_blake3:null,corpus_mode:"generated",corpus_offset:0,
            dispatch_late_after_ms:1,driver_image:$driver_image,
            driver_package_version:$driver_package_version,drop_late_after_ms:100,
            duration_seconds:$duration,expected_slots:$slots,first_sequence:$sequence,
            generator_scheme:"alpha_bravo_lsb_identity_service_fill_v1",job_id:$job_id,
            last_sequence:($sequence+9999),max_in_flight:$max_in_flight,
            model_sha256:$model,nonce:$nonce,offered_rate_denominator:1,
            offered_rate_numerator:$rate_numerator,offered_rate_requested_decimal:$rate,
            offered_rps:$rate,protocol_version:$protocol,raw_latencies:true,
            rpc_timeout_ms:30000,run_id:$run_id,scheduled_start_epoch_ms:$start,
            target_endpoint:$endpoint,target_image:$target_image,
            token_count_including_specials:64,tokenizer_sha256:$tokenizer,
            topology:$topology,warmup_requests:0}
          and $records[0].config_digest.algorithm=="blake3"
          and $records[0].config_digest.canonicalization=="sorted-string-map-v1"
          and ($records[0].config_digest.hex|test("^[0-9a-f]{64}$"))' "$stream_file" >/dev/null; then
        jq -s --arg protocol "$DRIVER_ARMING_PROTOCOL" \
          '[.[] | select(.schema=="llm-d-sc.benchmark-driver.armed" and .schema_version==1
            and .record_type=="ARMED" and .protocol_version==$protocol)][0]' \
          "$stream_file" >"$armed_file"
      fi
    fi
  done < <(jq -r --arg endpoint "${target_ip}:50051" '.jobs[]|[
    ((.ordinal|tostring|if length==1 then "0"+. else . end)),.name,.arming_nonce,$endpoint,
    .start_epoch_ms,.expected_slots,.duration_seconds,.offered_rps,.max_in_flight,.sequence_base]|@tsv' \
    "${RUN_DIR}/recovery-plan.json")
  armed_count=$(find "${RUN_DIR}/arming" -name 'j??.json' -type f -size +0c | wc -l | tr -d ' ')
  if [[ "$driver_ready" == 14 && "$armed_count" == 14 ]]; then break; fi
  sleep 2
done
"${k[@]}" get pods -n "$NAMESPACE" -l "benchmark.llm-d/run-id=${RECOVERY_RUN_ID}" -o json \
  >"${RUN_DIR}/driver-pods-before.json"
driver_digest=${DRIVER_IMAGE##*@}
jq -e --arg node "$DRIVER_NODE" --arg digest "$driver_digest" '
  (.items|length)==14 and all(.items[];
    .status.phase=="Running" and .spec.nodeName==$node
    and any(.status.conditions[]?;.type=="Ready" and .status=="True")
    and (.status.containerStatuses|length)==1
    and .status.containerStatuses[0].restartCount==0
    and (.status.containerStatuses[0].imageID|endswith($digest)))' \
  "${RUN_DIR}/driver-pods-before.json" >/dev/null \
  || die "all 14 pinned driver Pods were not Kubernetes Ready on ${DRIVER_NODE} by T0-${ARMED_BARRIER_LEAD_SECONDS}s"
(( $(date -u +%s) <= driver_ready_deadline )) \
  || die "driver application-level ARMED barrier did not close at least ${ARMED_BARRIER_LEAD_SECONDS} seconds before T0"
[[ "$armed_count" == 14 ]] \
  || die "application-level ARMED barrier failed: expected 14 validated ${DRIVER_ARMING_PROTOCOL} records, observed ${armed_count}"
driver_ready_verified_epoch_ms=$(( $(date -u +%s) * 1000 ))
jq -n --argjson verified "$driver_ready_verified_epoch_ms" --argjson t0 "$t0_epoch_ms" \
  '{schema_version:1,verified_epoch_ms:$verified,t0_epoch_ms:$t0,
    ready_driver_pods:14,minimum_lead_seconds:180,
    observed_lead_seconds:(($t0-$verified)/1000),load_authorizing:false,
    note:"Kubernetes readiness is secondary evidence; driver-armed.json is the load-authorizing barrier"}' \
  >"${RUN_DIR}/driver-kubernetes-readiness.json"
jq -s --arg protocol "$DRIVER_ARMING_PROTOCOL" --argjson verified "$driver_ready_verified_epoch_ms" \
  '{schema_version:1,protocol:$protocol,verified_epoch_ms:$verified,all_14_armed:true,records:.}' \
  "${RUN_DIR}"/arming/j??.json >"${RUN_DIR}/driver-armed.json"

abort_from_observer() {
  local message
  local output
  message=$1
  output=$2
  printf '%s\n' "$message" >"$output"
  "${k[@]}" delete jobs,pods -n "$NAMESPACE" -l "benchmark.llm-d/run-id=${RECOVERY_RUN_ID}" \
    --ignore-not-found --cascade=foreground --wait=false >/dev/null 2>&1 || true
  exit 9
}

observer_exit_guard() {
  local exit_status
  local kind
  local name
  local output
  exit_status=$1
  kind=$2
  name=$3
  trap - EXIT INT TERM ERR
  if (( exit_status != 0 )); then
    output="${RUN_DIR}/${kind}-${name}-unexpected-exit.txt"
    if [[ ! -s "$output" ]]; then
      printf '%s\n' "${kind} ${name}: observer exited unexpectedly with status ${exit_status}" >"$output"
    fi
    # This child-side request closes the small polling race at T0.  The main
    # process independently reaps the PID and performs the authoritative abort.
    "${k[@]}" delete jobs,pods -n "$NAMESPACE" -l "benchmark.llm-d/run-id=${RECOVERY_RUN_ID}" \
      --ignore-not-found --cascade=foreground --wait=false >/dev/null 2>&1 || true
  fi
  exit "$exit_status"
}

run_observer_command() {
  local completion_deadline_epoch_s
  completion_deadline_epoch_s=$1
  shift
  if (( completion_deadline_epoch_s > 0 )); then
    run_before_epoch "$completion_deadline_epoch_s" "$@"
  else
    "$@"
  fi
}

capture_checkpoint() {
  local name
  local scheduled_ms
  local completion_deadline_ms
  local completion_deadline_s
  local pod_file
  local cgroup_file
  local output
  local observed
  local cpuset
  local cpu_max
  local image_id
  local restart
  local ready
  local ip
  local uid
  local node
  local now_s
  local delay
  local completed_epoch_ms
  local gate_file
  local gate_tmp
  name=$1
  scheduled_ms=$2
  completion_deadline_ms=$3
  completion_deadline_s=$((completion_deadline_ms / 1000))
  trap 'observer_exit_guard "$?" "checkpoint" "$name"' EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  trap - ERR
  pod_file="${RUN_DIR}/checkpoints/${name}-pod.json"
  cgroup_file="${RUN_DIR}/checkpoints/${name}-cgroup.txt"
  output="${RUN_DIR}/checkpoints/${name}.json"
  now_s=$(date -u +%s)
  delay=$(checkpoint_delay_seconds "$scheduled_ms" "$now_s")
  if (( delay > 0 )); then sleep "$delay"; fi
  run_observer_command "$completion_deadline_s" \
    "${k[@]}" get pod "$target_pod" -n "$NAMESPACE" -o json >"$pod_file" \
    || abort_from_observer "checkpoint ${name}: target Pod disappeared" "${RUN_DIR}/checkpoints/${name}.error.txt"
  run_observer_command "$completion_deadline_s" \
    "${k[@]}" exec -n "$NAMESPACE" "$target_pod" -c "$TARGET_CONTAINER" -- sh -c '
    printf "cpuset_cpus_effective "; cat /sys/fs/cgroup/cpuset.cpus.effective
    printf "cpu_max "; cat /sys/fs/cgroup/cpu.max
    cat /sys/fs/cgroup/cpu.stat' >"$cgroup_file" \
    || abort_from_observer "checkpoint ${name}: cgroup capture failed" "${RUN_DIR}/checkpoints/${name}.error.txt"
  observed=$(( $(date -u +%s) * 1000 ))
  cpuset=$(awk '$1=="cpuset_cpus_effective"{print $2}' "$cgroup_file")
  cpu_max=$(awk '$1=="cpu_max"{$1=""; sub(/^ /,""); print}' "$cgroup_file")
  uid=$(jq -r .metadata.uid "$pod_file")
  ip=$(jq -r .status.podIP "$pod_file")
  node=$(jq -r .spec.nodeName "$pod_file")
  restart=$(jq -r --arg container "$TARGET_CONTAINER" '[.status.containerStatuses[]|select(.name==$container)][0].restartCount' "$pod_file")
  image_id=$(jq -r --arg container "$TARGET_CONTAINER" '[.status.containerStatuses[]|select(.name==$container)][0].imageID' "$pod_file")
  ready=$(jq -r 'any(.status.conditions[]?;.type=="Ready" and .status=="True")' "$pod_file")
  jq -n --arg name "$name" --arg target_name "$target_pod" --arg uid "$uid" --arg ip "$ip" \
    --arg node "$node" --arg image_id "$image_id" --arg cpuset "$cpuset" --arg cpu_max "$cpu_max" \
    --argjson scheduled "$scheduled_ms" --argjson observed "$observed" \
    --argjson ready "$ready" --argjson restart "$restart" '
    {schema_version:1,name:$name,scheduled_epoch_ms:$scheduled,observed_epoch_ms:$observed,
     target:{name:$target_name,uid:$uid,ip:$ip,node:$node,ready:$ready,
       restart_count:$restart,image_id:$image_id},cpuset_cpus_effective:$cpuset,cpu_max:$cpu_max}' >"$output"
  if ! jq -e --arg uid "$target_uid" --arg ip "$target_ip" --arg node "$TARGET_NODE" \
      --arg image "$target_image_id" --arg cpuset "$target_cpuset" --arg cpu_max "$runtime_cpu_max" '
      .target.uid==$uid and .target.ip==$ip and .target.node==$node and .target.image_id==$image
      and .target.ready==true and .target.restart_count==0 and .cpuset_cpus_effective==$cpuset
      and .cpu_max==$cpu_max and (.cpu_max|split(" ")[0])=="max"' \
      "$output" >/dev/null; then
    abort_from_observer "checkpoint ${name}: target identity/health/topology changed" "${RUN_DIR}/checkpoints/${name}.error.txt"
  fi
  if (( completion_deadline_s > 0 )); then
    completed_epoch_ms=$(( $(date -u +%s) * 1000 ))
    if (( completed_epoch_ms > completion_deadline_ms )); then
      abort_from_observer "checkpoint ${name}: completed after its hard pre-T0 deadline" "${RUN_DIR}/checkpoints/${name}.error.txt"
    fi
    gate_file="${RUN_DIR}/checkpoints/${name}-gate.json"
    gate_tmp="${gate_file}.tmp"
    jq -n --arg name "$name" --argjson completed "$completed_epoch_ms" \
      --argjson deadline "$completion_deadline_ms" \
      '{schema_version:1,name:$name,completion_epoch_ms:$completed,
        completion_deadline_epoch_ms:$deadline,load_authorized:true}' >"$gate_tmp"
    mv "$gate_tmp" "$gate_file"
  fi
}

monitor_health() {
  local end_s
  local pod_json
  local nodes_json
  local sample
  local now
  end_s=$1
  trap 'observer_exit_guard "$?" "monitor" "health"' EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  trap - ERR
  : >"${RUN_DIR}/health-monitor.ndjson"
  while true; do
    now=$(date -u +%s)
    pod_json=$("${k[@]}" get pod "$target_pod" -n "$NAMESPACE" -o json) \
      || abort_from_observer "health monitor: target Pod disappeared" "${RUN_DIR}/health-monitor-error.txt"
    nodes_json=$("${k[@]}" get nodes "$TARGET_NODE" "$DRIVER_NODE" -o json) \
      || abort_from_observer "health monitor: node query failed" "${RUN_DIR}/health-monitor-error.txt"
    sample=$(jq -cn \
      --argjson pod "$pod_json" --argjson nodes "$nodes_json" \
      --arg container "$TARGET_CONTAINER" --argjson epoch "$now" \
      --arg target_node "$TARGET_NODE" --arg driver_node "$DRIVER_NODE" '
      ([ $pod.status.containerStatuses[]? | select(.name==$container) ][0]) as $status
      | {schema_version:1,sample_epoch_s:$epoch,
         target:{name:$pod.metadata.name,uid:$pod.metadata.uid,ip:$pod.status.podIP,
           node:$pod.spec.nodeName,
           ready:any($pod.status.conditions[]?;.type=="Ready" and .status=="True"),
           restart_count:$status.restartCount,image_id:$status.imageID},
         nodes_ready:all(([$target_node,$driver_node]|unique[]); . as $name
           | any($nodes.items[]?; .metadata.name==$name
             and any(.status.conditions[]?;.type=="Ready" and .status=="True")))}')
    printf '%s\n' "$sample" >>"${RUN_DIR}/health-monitor.ndjson"
    if ! jq -e --arg uid "$target_uid" --arg ip "$target_ip" --arg node "$TARGET_NODE" \
        --arg image "$target_image_id" '
        .target.uid==$uid and .target.ip==$ip and .target.node==$node and .target.image_id==$image
        and .target.ready==true and .target.restart_count==0 and .nodes_ready==true' <<<"$sample" >/dev/null; then
      abort_from_observer "health monitor: target identity/readiness/restart/image or node readiness changed" "${RUN_DIR}/health-monitor-error.txt"
    fi
    if (( now >= end_s )); then break; fi
    sleep "$HEALTH_MONITOR_INTERVAL_SECONDS"
  done
}

quick_validate_completed_job() {
  local ordinal
  local job_name
  local phase
  local expected_slots
  local marker
  local report
  local allowed_json
  ordinal=$1
  job_name=$2
  phase=$3
  expected_slots=$4
  marker="${RUN_DIR}/drivers/j${ordinal}.quick-ok"
  report="${RUN_DIR}/drivers/j${ordinal}.raw"
  [[ ! -e "$marker" ]] || return 0
  "${k[@]}" logs -n "$NAMESPACE" job/"$job_name" >"$report" \
    || die "cannot collect completed driver report for ${job_name}"
  if [[ "$phase" == pre || "$phase" == post ]]; then
    allowed_json='["OK"]'
  else
    allowed_json='["OK","GRPC_RESOURCEEXHAUSTED"]'
  fi
  jq -s -e --argjson expected "$expected_slots" --argjson allowed "$allowed_json" '
    [.[]|select(.schema_version==2 and .probe=="sustained_exact_token_corpus")] as $reports
    | ($reports|length)==1 and ($reports[0] as $report
    | $report.load_model=="open_loop_deterministic_offered_rate"
    and $report.accounting.offered_slots==$expected
    and $report.accounting.offered_slots==($report.accounting.initiated_requests
      + $report.accounting.dropped_in_flight_limit + $report.accounting.dropped_schedule_late)
    and $report.accounting.initiated_requests==$report.accounting.completed_requests
    and $report.accounting.completed_requests==($report.accounting.completed_within_plateau
      + $report.accounting.completed_after_plateau)
    and $report.accounting.dropped_in_flight_limit==0
    and $report.accounting.dropped_schedule_late==0
    and ([$report.statuses_completed_total|to_entries[]|select(.value>0)|.key
      | select(. as $status|$allowed|index($status)|not)]|length)==0)' "$report" >/dev/null \
    || die "completed driver ${job_name} failed accounting or emitted an unexpected status"
  : >"$marker"
}

check_background_observers() {
  local context
  local now_s
  local index
  local pid
  local name
  local -a live_pids
  local -a live_names
  context=$1
  now_s=$(date -u +%s)
  live_pids=()
  live_names=()

  if [[ -n "$monitor_pid" ]] && ! kill -0 "$monitor_pid" 2>/dev/null; then
    if wait "$monitor_pid"; then
      monitor_pid=""
      (( now_s >= post_end_epoch_s )) \
        || die "${context}: target/node health monitor exited successfully before its observation window ended"
    else
      monitor_pid=""
      die "${context}: concurrent target/node health monitor failed"
    fi
  fi

  for index in "${!checkpoint_pids[@]}"; do
    pid=${checkpoint_pids[$index]}
    name=${checkpoint_names[$index]}
    if kill -0 "$pid" 2>/dev/null; then
      live_pids+=("$pid")
      live_names+=("$name")
      continue
    fi
    if wait "$pid"; then
      [[ -s "${RUN_DIR}/checkpoints/${name}.json" ]] \
        || die "${context}: checkpoint observer ${name} exited without its evidence artifact"
    else
      die "${context}: checkpoint observer ${name} failed"
    fi
  done
  checkpoint_pids=("${live_pids[@]}")
  checkpoint_names=("${live_names[@]}")
}

target_bound_deadline_epoch_ms=$(jq -r '.checkpoints[]|select(.name=="target-bound")|.completion_deadline_epoch_ms' "${RUN_DIR}/recovery-plan.json")
target_bound_deadline_epoch_s=$((target_bound_deadline_epoch_ms / 1000))
while IFS=$'\t' read -r checkpoint_name checkpoint_epoch checkpoint_deadline; do
  ( capture_checkpoint "$checkpoint_name" "$checkpoint_epoch" "$checkpoint_deadline" ) &
  checkpoint_pids+=("$!")
  checkpoint_names+=("$checkpoint_name")
done < <(jq -r '.checkpoints[]|[.name,.scheduled_epoch_ms,(.completion_deadline_epoch_ms//0)]|@tsv' "${RUN_DIR}/recovery-plan.json")
post_end_epoch_s=$((t0_epoch_ms / 1000 + 580))
( monitor_health "$post_end_epoch_s" ) &
monitor_pid=$!

# Catch immediate observer setup failures while there is still the full ARMED
# lead window to delete the absolute-start Jobs.  Every later polling iteration
# repeats this check; child EXIT guards independently request deletion on error.
sleep 1
check_background_observers "pre-T0 startup gate"

# The target-bound identity/cgroup observation is load-authorizing.  It must
# finish by T0-155s; absence, failure, or a stalled live command invokes cleanup
# with a T0-25s deadline to prove foreground deletion and zero labeled Pods.
target_bound_gate="${RUN_DIR}/checkpoints/target-bound-gate.json"
while [[ ! -s "$target_bound_gate" ]]; do
  check_background_observers "target-bound pre-T0 gate"
  (( $(date -u +%s) <= target_bound_deadline_epoch_s )) \
    || die "target-bound checkpoint missed its hard pre-T0 completion deadline"
  sleep 1
done
check_background_observers "target-bound pre-T0 completion gate"
jq -e --argjson deadline "$target_bound_deadline_epoch_ms" --argjson t0 "$t0_epoch_ms" '
  .schema_version==1 and .name=="target-bound" and .load_authorized==true
  and .completion_deadline_epoch_ms==$deadline
  and .completion_epoch_ms<=$deadline and .completion_epoch_ms<$t0' \
  "$target_bound_gate" >/dev/null \
  || die "target-bound checkpoint did not produce valid pre-T0 authorization evidence"

job_deadline_s=$((post_end_epoch_s + MAX_DRAIN_SECONDS))
while true; do
  check_background_observers "concurrent observer gate"
  jobs_json=$("${k[@]}" get jobs -n "$NAMESPACE" -l "benchmark.llm-d/run-id=${RECOVERY_RUN_ID}" -o json) \
    || die "cannot inspect recovery Jobs"
  job_count=$(jq '.items|length' <<<"$jobs_json")
  failed_count=$(jq '[.items[]|select((.status.failed//0)>0)]|length' <<<"$jobs_json")
  complete_count=$(jq '[.items[]|select(any(.status.conditions[]?;.type=="Complete" and .status=="True"))]|length' <<<"$jobs_json")
  now_epoch_ms=$(( $(date -u +%s) * 1000 ))
  overdue_incomplete=$(jq \
    --argjson now "$now_epoch_ms" --argjson max_drain "$MAX_DRAIN_SECONDS" \
    --slurpfile plan "${RUN_DIR}/recovery-plan.json" '
    [. as $live
     | $plan[0].jobs[] as $planned
     | select($now > ($planned.start_epoch_ms + $planned.duration_seconds*1000 + $max_drain*1000))
     | select(([$live.items[]
       | select(.metadata.name==$planned.name)
       | select(any(.status.conditions[]?;.type=="Complete" and .status=="True"))]|length) != 1)
     | $planned.name] | length' <<<"$jobs_json")
  (( failed_count == 0 )) || die "one or more recovery driver Jobs failed"
  (( job_count == 14 )) || die "recovery Job set changed during the cycle"
  (( overdue_incomplete == 0 )) || die "a recovery driver exceeded its phase-local 90s drain limit"
  while IFS=$'\t' read -r ordinal job_name phase expected_slots; do
    if jq -e --arg name "$job_name" '
        any(.items[]?; .metadata.name==$name
          and any(.status.conditions[]?;.type=="Complete" and .status=="True"))' \
        <<<"$jobs_json" >/dev/null; then
      quick_validate_completed_job "$ordinal" "$job_name" "$phase" "$expected_slots"
    fi
  done < <(jq -r '.jobs[]|[
    ((.ordinal|tostring|if length==1 then "0"+. else . end)),
    .name,.phase,.expected_slots]|@tsv' "${RUN_DIR}/recovery-plan.json")
  if (( complete_count == 14 )); then break; fi
  (( $(date -u +%s) <= job_deadline_s )) || die "one or more recovery Jobs exceeded the 90s drain limit"
  sleep 5
done

if [[ -n "$monitor_pid" ]]; then
  if ! wait "$monitor_pid"; then monitor_pid=""; die "target/node health monitor failed"; fi
  monitor_pid=""
fi
while (( ${#checkpoint_pids[@]} > 0 )); do
  check_background_observers "final observer gate"
  (( ${#checkpoint_pids[@]} == 0 )) || sleep 1
done
checkpoint_names=()

"${k[@]}" get jobs -n "$NAMESPACE" -l "benchmark.llm-d/run-id=${RECOVERY_RUN_ID}" -o json >"${RUN_DIR}/jobs-after.json"
"${k[@]}" get pods -n "$NAMESPACE" -l "benchmark.llm-d/run-id=${RECOVERY_RUN_ID}" -o json >"${RUN_DIR}/driver-pods-after.json"
while IFS=$'\t' read -r ordinal job_name; do
  if [[ ! -s "${RUN_DIR}/drivers/j${ordinal}.raw" ]]; then
    "${k[@]}" logs -n "$NAMESPACE" job/"$job_name" >"${RUN_DIR}/drivers/j${ordinal}.raw"
  fi
  jq -s -e '[.[]|select(.schema_version==2 and .probe=="sustained_exact_token_corpus")]
    | if length==1 then .[0] else error("expected exactly one final report") end' \
    "${RUN_DIR}/drivers/j${ordinal}.raw" >"${RUN_DIR}/drivers/j${ordinal}.json" \
    || die "driver ${job_name} did not emit exactly one final JSON report"
done < <(jq -r '.jobs[]|[((.ordinal|tostring|if length==1 then "0"+. else . end)),.name]|@tsv' "${RUN_DIR}/recovery-plan.json")

"${k[@]}" get pods -n "$NAMESPACE" -l "$TARGET_SELECTOR" -o json >"${RUN_DIR}/targets-after.json"

telemetry_start=$((t0_epoch_ms / 1000 - 30))
telemetry_end=$((post_end_epoch_s + 30))
telemetry_capture_after=$((telemetry_end + METRIC_SETTLE_SECONDS))
telemetry_delay=$((telemetry_capture_after - $(date -u +%s)))
if (( telemetry_delay > 0 )); then sleep "$telemetry_delay"; fi

expected_target_served=$(jq -s '[.[].statuses_completed_total.OK // 0] | add' "${RUN_DIR}"/drivers/j*.json)
counter_deadline=$(( $(date -u +%s) + TARGET_COUNTER_SETTLE_SECONDS ))
logged_target_served=""
while true; do
  "${k[@]}" logs -n "$NAMESPACE" "$target_pod" -c "$TARGET_CONTAINER" \
    --timestamps=true --since-time="$target_started_at" >"${RUN_DIR}/target-logs-full.txt" \
    || die "cannot capture final target counter/queue logs"
  logged_target_served=$(sed -n 's/.*llm-d-sc metrics: served=\([0-9][0-9]*\).*/\1/p' \
    "${RUN_DIR}/target-logs-full.txt" | tail -n 1)
  if [[ -n "$logged_target_served" && "$logged_target_served" == "$expected_target_served" ]]; then
    break
  fi
  if [[ -n "$logged_target_served" ]] && (( logged_target_served > expected_target_served )); then
    die "target served counter exceeds all driver OK completions; traffic attribution is contaminated"
  fi
  (( $(date -u +%s) < counter_deadline )) \
    || die "target served counter did not reconcile exactly with all driver OK completions"
  sleep 2
done

prom_host=$("${k[@]}" -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
auth_token=$("${k[@]}" whoami -t)
query_range() {
  local name
  local query
  name=$1
  query=$2
  curl -ksS --connect-timeout "$CURL_CONNECT_TIMEOUT_SECONDS" --max-time "$CURL_MAX_TIME_SECONDS" \
    --get -H "Authorization: Bearer ${auth_token}" \
    --data-urlencode "query=${query}" --data-urlencode "start=${telemetry_start}" \
    --data-urlencode "end=${telemetry_end}" --data-urlencode 'step=5' \
    "https://${prom_host}/api/v1/query_range" >"${RUN_DIR}/metrics/${name}.json"
  jq -e '.status=="success"' "${RUN_DIR}/metrics/${name}.json" >/dev/null \
    || die "telemetry query failed: ${name}"
}
query_range pod_cpu_otel "k8s_pod_cpu_usage{k8s_namespace_name=\"${NAMESPACE}\",k8s_pod_name=\"${target_pod}\"}"
query_range container_cpu_otel "container_cpu_usage{k8s_namespace_name=\"${NAMESPACE}\",k8s_pod_name=\"${target_pod}\",k8s_container_name=\"${TARGET_CONTAINER}\"}"
query_range container_cpu_cadvisor "sum by (pod)(rate(container_cpu_usage_seconds_total{namespace=\"${NAMESPACE}\",pod=\"${target_pod}\",container=\"${TARGET_CONTAINER}\"}[30s]))"
query_range throttle_ratio "sum by (pod)(rate(container_cpu_cfs_throttled_periods_total{namespace=\"${NAMESPACE}\",pod=\"${target_pod}\",container=\"${TARGET_CONTAINER}\"}[30s])) / sum by (pod)(rate(container_cpu_cfs_periods_total{namespace=\"${NAMESPACE}\",pod=\"${target_pod}\",container=\"${TARGET_CONTAINER}\"}[30s]))"
query_range memory_working_set "container_memory_working_set_bytes{namespace=\"${NAMESPACE}\",pod=\"${target_pod}\",container=\"${TARGET_CONTAINER}\"}"
query_range cpu_pressure_waiting "rate(container_pressure_cpu_waiting_seconds_total{namespace=\"${NAMESPACE}\",pod=\"${target_pod}\",container=\"${TARGET_CONTAINER}\"}[30s])"
query_range restarts "kube_pod_container_status_restarts_total{namespace=\"${NAMESPACE}\",pod=\"${target_pod}\",container=\"${TARGET_CONTAINER}\"}"
query_range pod_ready "kube_pod_status_ready{namespace=\"${NAMESPACE}\",pod=\"${target_pod}\",condition=\"true\"}"
query_range node_ready "kube_node_status_condition{condition=\"Ready\",status=\"true\",node=~\"${TARGET_NODE}|${DRIVER_NODE}\"}"
unset auth_token
jq -n --argjson start "$telemetry_start" --argjson end "$telemetry_end" \
  --argjson max_gap "$METRIC_MAX_GAP_SECONDS" \
  '{schema_version:1,start_epoch_s:$start,end_epoch_s:$end,step_seconds:5,max_gap_seconds:$max_gap,
    required:["pod_cpu_otel","container_cpu_otel","container_cpu_cadvisor","memory_working_set",
      "restarts","pod_ready","node_ready"],supporting:["throttle_ratio","cpu_pressure_waiting"]}' \
  >"${RUN_DIR}/telemetry-window.json"

"${k[@]}" get events -n "$NAMESPACE" -o json >"${RUN_DIR}/events-after.json"
"${k[@]}" get nodes "$TARGET_NODE" "$DRIVER_NODE" -o json >"${RUN_DIR}/nodes-after.json"

if ! python3 "$SUMMARY_RUNNER" "$RUN_DIR" --output "${RUN_DIR}/recovery-summary.json" \
    >"${RUN_DIR}/recovery-summary-stdout.txt" 2>"${RUN_DIR}/recovery-summary-stderr.txt"; then
  run_invalid=1
  last_error=$(tail -n 1 "${RUN_DIR}/recovery-summary-stderr.txt" 2>/dev/null || true)
  exit 6
fi
measurement_complete=1
final_decision=$(jq -r '.decision.status' "${RUN_DIR}/recovery-summary.json")
if [[ "$final_decision" != green ]]; then
  last_error="valid recovery measurement completed with ${final_decision} benchmark decision"
  exit 7
fi
cat "${RUN_DIR}/recovery-summary.json"
