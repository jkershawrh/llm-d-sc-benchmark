#!/usr/bin/env bash
set -euo pipefail

# Run a deterministic offered-rate ladder against direct target Pod IPs. This
# wrapper does not alter the semantic-classifier image or source. It delegates
# each measurement to arena-sc-inference-cell.sh and keeps open-loop results
# separate from the closed-loop worker/horizontal matrix.

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

KUBECONFIG_PATH=${KUBECONFIG_PATH:-/tmp/llm-d-sc-arena-kubeconfig}
NAMESPACE=${NAMESPACE:-llm-d-sc-scaleout}
DEPLOYMENT=${DEPLOYMENT:-classifier-target}
TARGET_SELECTOR=${TARGET_SELECTOR:-app.kubernetes.io/component=classifier-target}
TARGET_NODE=${TARGET_NODE:-gnr2.fm2aihpcsed.com}
DRIVER_NODE=${DRIVER_NODE:-rhgnr1}
OTEL_DAEMONSET=${OTEL_DAEMONSET:-llm-d-sc-otel}

SWEEP_RUN_ID=${SWEEP_RUN_ID:?set a DNS-safe unique SWEEP_RUN_ID}
OFFERED_RPS_STEPS=${OFFERED_RPS_STEPS:?set space-separated per-target offered RPS steps}
REPLICAS=${REPLICAS:?set REPLICAS}
SEQUENCE_BASE=${SEQUENCE_BASE:?set a globally unused SEQUENCE_BASE}
DRIVER_IMAGE=${DRIVER_IMAGE:?set the pinned benchmark-driver image digest}
TARGET_IMAGE=${TARGET_IMAGE:?set the expected pinned target image digest}
MODEL_SHA256=${MODEL_SHA256:?set MODEL_SHA256}

TOKENIZER_SHA256=${TOKENIZER_SHA256:-851ca67100d372ca3ae031a6abd168f53489eebfd7d89523f35c5c9b4d372c3c}
TOKEN_COUNT=${TOKEN_COUNT:-64}
CONNECTIONS=${CONNECTIONS:-1}
MAX_IN_FLIGHT=${MAX_IN_FLIGHT:-64}
CONCURRENCY=${CONCURRENCY:-$MAX_IN_FLIGHT}
DURATION_SECONDS=${DURATION_SECONDS:-60}
START_DELAY_SECONDS=${START_DELAY_SECONDS:-45}
MAX_ROWS_PER_ENDPOINT=${MAX_ROWS_PER_ENDPOINT:-10000}
DISPATCH_LATE_AFTER_MS=${DISPATCH_LATE_AFTER_MS:-1}
DROP_LATE_AFTER_MS=${DROP_LATE_AFTER_MS:-100}
RPC_TIMEOUT_MS=${RPC_TIMEOUT_MS:-30000}
QUIESCENCE_SECONDS=${QUIESCENCE_SECONDS:-30}
REPEATS=${REPEATS:-1}
PLAN_ONLY=${PLAN_ONLY:-0}
RESET_TARGETS=${RESET_TARGETS:-true}
ORDER_MODE=${ORDER_MODE:-randomized}
SWEEP_SEED=${SWEEP_SEED:-}
CAPTURE_TELEMETRY=${CAPTURE_TELEMETRY:-1}
METRIC_MAX_GAP_SECONDS=${METRIC_MAX_GAP_SECONDS:-10}
MAX_SCHEDULER_P99_LAG_MS=${MAX_SCHEDULER_P99_LAG_MS:-5}
MAX_SCHEDULE_DROP_RATIO=${MAX_SCHEDULE_DROP_RATIO:-0}
RESTORE_ORIGINAL_ON_EXIT=${RESTORE_ORIGINAL_ON_EXIT:-1}
DELETE_COMPLETED_JOBS=${DELETE_COMPLETED_JOBS:-1}
LOCK_NAME=${LOCK_NAME:-sc-benchmark-matrix-lock}
DRIVER_BUILD_SOURCE_SHA256=${DRIVER_BUILD_SOURCE_SHA256:-}
TOPOLOGY_PREFLIGHT_ENABLED=${TOPOLOGY_PREFLIGHT_ENABLED:-1}
TOPOLOGY_PREFLIGHT_RUNNER=${TOPOLOGY_PREFLIGHT_RUNNER:-${SCRIPT_DIR}/arena-sc-topology-preflight.py}
TOPOLOGY_PREFLIGHT_CONTAINER=${TOPOLOGY_PREFLIGHT_CONTAINER:-}
TOPOLOGY_PREFLIGHT_RESERVED_CPUS=${TOPOLOGY_PREFLIGHT_RESERVED_CPUS:-}

RESULT_ROOT=${RESULT_ROOT:-${REPO_ROOT}/results/open-loop-sweeps}
SWEEP_DIR=${SWEEP_DIR:-${RESULT_ROOT}/${SWEEP_RUN_ID}}
CELL_RESULT_ROOT=${CELL_RESULT_ROOT:-${SWEEP_DIR}/cells}
CELL_RUNNER=${CELL_RUNNER:-${SCRIPT_DIR}/arena-sc-inference-cell.sh}
METRICS_RUNNER=${METRICS_RUNNER:-${SCRIPT_DIR}/arena-sc-capture-thanos.sh}
SUMMARY_RUNNER=${SUMMARY_RUNNER:-${SCRIPT_DIR}/arena-sc-open-loop-summarize.py}

k=(oc --kubeconfig "$KUBECONFIG_PATH")
sweep_dir_owned=0
lock_acquired=0
cluster_mutated=0
sweep_complete=0
plan_only_complete=0
sweep_invalid=0
original_replicas=""
current_cell=preflight
active_run_id=""
last_error=""

die() {
  last_error=$*
  echo "ERROR: ${last_error}" >&2
  if (( sweep_dir_owned == 1 )); then
    printf '%s\n' "$last_error" >"${SWEEP_DIR}/sweep-error.txt"
  fi
  exit 2
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

require_access() {
  local verb=$1 resource=$2 namespace=${3:-} allowed
  if [[ -n "$namespace" ]]; then
    allowed=$("${k[@]}" auth can-i "$verb" "$resource" -n "$namespace")
  else
    allowed=$("${k[@]}" auth can-i "$verb" "$resource")
  fi
  [[ "$allowed" == yes ]] \
    || die "current identity cannot ${verb} ${resource}${namespace:+ in ${namespace}}"
}

assert_positive() {
  local name=$1 value=$2
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "${name} must be a positive integer"
}

cleanup() {
  cleanup_entry_status=$?
  local exit_status=$cleanup_entry_status cleanup_error="" status lock_owner=""
  trap - EXIT INT TERM ERR
  set +e

  if [[ -n "$active_run_id" ]]; then
    "${k[@]}" delete jobs -n "$NAMESPACE" \
      -l "benchmark.llm-d/run-id=${active_run_id}" \
      --ignore-not-found --cascade=foreground --wait=true --timeout=120s >/dev/null 2>&1 \
      || cleanup_error="failed to delete active driver Jobs"
    "${k[@]}" delete pods -n "$NAMESPACE" \
      -l "benchmark.llm-d/run-id=${active_run_id}" \
      --ignore-not-found --wait=true --timeout=120s >/dev/null 2>&1 \
      || cleanup_error="${cleanup_error:+${cleanup_error}; }failed to delete active driver Pods"
  fi

  if (( cluster_mutated == 1 && RESTORE_ORIGINAL_ON_EXIT == 1 )) \
      && [[ "$original_replicas" =~ ^[0-9]+$ ]]; then
    if ! "${k[@]}" scale deployment "$DEPLOYMENT" -n "$NAMESPACE" \
        --replicas="$original_replicas" >/dev/null 2>&1; then
      cleanup_error="${cleanup_error:+${cleanup_error}; }failed to restore original replica count"
    elif (( original_replicas > 0 )); then
      "${k[@]}" rollout status deployment/"$DEPLOYMENT" -n "$NAMESPACE" \
        --timeout=600s >/dev/null 2>&1 \
        || cleanup_error="${cleanup_error:+${cleanup_error}; }original replicas did not become Ready"
    elif [[ -n "$("${k[@]}" get pods -n "$NAMESPACE" -l "$TARGET_SELECTOR" -o name 2>/dev/null)" ]]; then
      "${k[@]}" wait --for=delete pod -n "$NAMESPACE" -l "$TARGET_SELECTOR" \
        --timeout=300s >/dev/null 2>&1 \
        || cleanup_error="${cleanup_error:+${cleanup_error}; }target Pods were not deleted during restore"
    fi
  fi

  if (( lock_acquired == 1 )); then
    lock_owner=$("${k[@]}" get configmap "$LOCK_NAME" -n "$NAMESPACE" \
      -o jsonpath='{.data.run-id}' 2>/dev/null)
    if [[ "$lock_owner" == "$SWEEP_RUN_ID" ]]; then
      "${k[@]}" delete configmap "$LOCK_NAME" -n "$NAMESPACE" \
        --wait=true --timeout=60s >/dev/null 2>&1 \
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
  elif (( sweep_complete == 1 )); then
    status=completed
  elif (( plan_only_complete == 1 )); then
    status=planned
  elif (( sweep_invalid == 1 )); then
    status=invalid
  else
    status=aborted
  fi
  if (( sweep_dir_owned == 1 )); then
    jq -n \
      --arg run_id "$SWEEP_RUN_ID" \
      --arg status "$status" \
      --arg current_cell "$current_cell" \
      --arg error "${last_error}${cleanup_error:+${last_error:+; }${cleanup_error}}" \
      --arg completed_at "$(date -u +%FT%TZ)" \
      --argjson exit_status "$exit_status" \
      '{schema_version:1,run_id:$run_id,status:$status,current_cell:$current_cell,
        exit_status:$exit_status,completed_at:$completed_at,
        error:(if $error == "" then null else $error end)}' \
      >"${SWEEP_DIR}/sweep-status.json"
  fi
  exit "$exit_status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap '[[ -n "$last_error" ]] || last_error="command failed at line ${LINENO}"' ERR

offered_slots_for() {
  local source=$1 whole fraction denominator numerator product slots digit
  [[ "$source" =~ ^([0-9]+)(\.([0-9]{0,9}))?$ ]] \
    || die "offered rate '${source}' must be an unsigned decimal with at most nine decimal places"
  whole=${BASH_REMATCH[1]}
  fraction=${BASH_REMATCH[3]:-}
  while [[ ${#whole} -gt 1 && ${whole:0:1} == 0 ]]; do whole=${whole:1}; done
  (( ${#whole} <= 10 )) || die "offered rate '${source}' exceeds 1,000,000,000"
  denominator=1
  for ((digit = 0; digit < ${#fraction}; digit++)); do denominator=$((denominator * 10)); done
  if [[ -n "$fraction" ]]; then
    fraction=$((10#$fraction))
  else
    fraction=0
  fi
  whole=$((10#$whole))
  numerator=$((whole * denominator + fraction))
  (( numerator > 0 && numerator <= 1000000000 * denominator )) \
    || die "offered rate '${source}' must be greater than zero and no more than 1,000,000,000"
  (( numerator <= 9223372036854775807 / DURATION_SECONDS )) \
    || die "offered rate '${source}' times DURATION_SECONDS overflows"
  product=$((numerator * DURATION_SECONDS))
  slots=$((product / denominator))
  (( product % denominator == 0 )) || slots=$((slots + 1))
  printf '%s\n' "$slots"
}

canonical_rate_for() {
  local source=$1 whole fraction denominator numerator left right remainder divisor
  [[ "$source" =~ ^([0-9]+)(\.([0-9]{0,9}))?$ ]] \
    || die "offered rate '${source}' must be an unsigned decimal with at most nine decimal places"
  whole=${BASH_REMATCH[1]}
  fraction=${BASH_REMATCH[3]:-}
  while [[ ${#whole} -gt 1 && ${whole:0:1} == 0 ]]; do whole=${whole:1}; done
  denominator=1
  for ((digit = 0; digit < ${#fraction}; digit++)); do denominator=$((denominator * 10)); done
  if [[ -n "$fraction" ]]; then fraction=$((10#$fraction)); else fraction=0; fi
  whole=$((10#$whole))
  numerator=$((whole * denominator + fraction))
  left=$numerator
  right=$denominator
  while (( right != 0 )); do
    remainder=$((left % right))
    left=$right
    right=$remainder
  done
  divisor=$left
  printf '%s/%s\n' "$((numerator / divisor))" "$((denominator / divisor))"
}

for command in jq cksum git; do require_command "$command"; done
if command -v sha256sum >/dev/null 2>&1; then
  sha256=(sha256sum)
elif command -v shasum >/dev/null 2>&1; then
  sha256=(shasum -a 256)
else
  die "required SHA-256 tool not found (sha256sum or shasum)"
fi

for pair in \
  "REPLICAS:$REPLICAS" "SEQUENCE_BASE:$SEQUENCE_BASE" \
  "CONNECTIONS:$CONNECTIONS" "MAX_IN_FLIGHT:$MAX_IN_FLIGHT" \
  "CONCURRENCY:$CONCURRENCY" "DURATION_SECONDS:$DURATION_SECONDS" \
  "START_DELAY_SECONDS:$START_DELAY_SECONDS" \
  "TOKEN_COUNT:$TOKEN_COUNT" "METRIC_MAX_GAP_SECONDS:$METRIC_MAX_GAP_SECONDS" \
  "MAX_ROWS_PER_ENDPOINT:$MAX_ROWS_PER_ENDPOINT" \
  "RPC_TIMEOUT_MS:$RPC_TIMEOUT_MS" "REPEATS:$REPEATS"; do
  assert_positive "${pair%%:*}" "${pair#*:}"
done
for pair in \
  "DISPATCH_LATE_AFTER_MS:$DISPATCH_LATE_AFTER_MS" \
  "DROP_LATE_AFTER_MS:$DROP_LATE_AFTER_MS" \
  "QUIESCENCE_SECONDS:$QUIESCENCE_SECONDS"; do
  [[ "${pair#*:}" =~ ^[0-9]+$ ]] || die "${pair%%:*} must be an unsigned integer"
done
(( DROP_LATE_AFTER_MS >= DISPATCH_LATE_AFTER_MS )) \
  || die "DROP_LATE_AFTER_MS must be at least DISPATCH_LATE_AFTER_MS"
(( TOKEN_COUNT >= 3 )) || die "TOKEN_COUNT must be at least 3"
[[ "$PLAN_ONLY" == 0 || "$PLAN_ONLY" == 1 ]] || die "PLAN_ONLY must be 0 or 1"
[[ "$RESET_TARGETS" == true || "$RESET_TARGETS" == false ]] \
  || die "RESET_TARGETS must be true or false"
for pair in \
  "CAPTURE_TELEMETRY:$CAPTURE_TELEMETRY" \
  "RESTORE_ORIGINAL_ON_EXIT:$RESTORE_ORIGINAL_ON_EXIT" \
  "DELETE_COMPLETED_JOBS:$DELETE_COMPLETED_JOBS" \
  "TOPOLOGY_PREFLIGHT_ENABLED:$TOPOLOGY_PREFLIGHT_ENABLED"; do
  [[ "${pair#*:}" == 0 || "${pair#*:}" == 1 ]] \
    || die "${pair%%:*} must be 0 or 1"
done
[[ "$ORDER_MODE" == randomized || "$ORDER_MODE" == as-given ]] \
  || die "ORDER_MODE must be randomized or as-given"
if [[ "$ORDER_MODE" == randomized ]]; then
  [[ "$SWEEP_SEED" =~ ^-?[0-9]+$ ]] \
    || die "SWEEP_SEED must be an integer when ORDER_MODE=randomized"
fi
jq -en --arg value "$MAX_SCHEDULER_P99_LAG_MS" \
  '($value | tonumber) >= 0' >/dev/null 2>&1 \
  || die "MAX_SCHEDULER_P99_LAG_MS must be a non-negative number"
jq -en --arg value "$MAX_SCHEDULE_DROP_RATIO" \
  '($value | tonumber) >= 0 and ($value | tonumber) <= 1' >/dev/null 2>&1 \
  || die "MAX_SCHEDULE_DROP_RATIO must be a number from 0 through 1"
[[ "$SWEEP_RUN_ID" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] \
  || die "SWEEP_RUN_ID must be a lowercase DNS label"
[[ "$DRIVER_IMAGE" =~ @sha256:[0-9a-f]{64}$ ]] \
  || die "DRIVER_IMAGE must be pinned by a full @sha256 digest"
[[ "$TARGET_IMAGE" =~ sha256:[0-9a-f]{64}$ ]] \
  || die "TARGET_IMAGE must end in a full sha256 digest"
[[ "$MODEL_SHA256" =~ ^[0-9a-f]{64}$ ]] || die "MODEL_SHA256 must be 64 lowercase hex characters"
[[ "$TOKENIZER_SHA256" =~ ^[0-9a-f]{64}$ ]] \
  || die "TOKENIZER_SHA256 must be 64 lowercase hex characters"
if [[ -n "$DRIVER_BUILD_SOURCE_SHA256" ]]; then
  [[ "$DRIVER_BUILD_SOURCE_SHA256" =~ ^[0-9a-f]{64}$ ]] \
    || die "DRIVER_BUILD_SOURCE_SHA256 must be 64 lowercase hex characters"
fi
[[ -x "$CELL_RUNNER" ]] || die "CELL_RUNNER is not executable: ${CELL_RUNNER}"
[[ -x "$SUMMARY_RUNNER" ]] || die "SUMMARY_RUNNER is not executable: ${SUMMARY_RUNNER}"
if (( CAPTURE_TELEMETRY == 1 )); then
  [[ -x "$METRICS_RUNNER" ]] || die "METRICS_RUNNER is not executable: ${METRICS_RUNNER}"
fi
if (( TOPOLOGY_PREFLIGHT_ENABLED == 1 )); then
  [[ -x "$TOPOLOGY_PREFLIGHT_RUNNER" ]] \
    || die "TOPOLOGY_PREFLIGHT_RUNNER is not executable: ${TOPOLOGY_PREFLIGHT_RUNNER}"
fi
[[ ! -e "$SWEEP_DIR" ]] || die "SWEEP_DIR already exists: ${SWEEP_DIR}"

rates=()
canonical_rates=()
set -f
for rate in $OFFERED_RPS_STEPS; do
  offered_slots_for "$rate" >/dev/null
  canonical=$(canonical_rate_for "$rate")
  if (( ${#canonical_rates[@]} > 0 )); then
    for seen in "${canonical_rates[@]}"; do
      [[ "$seen" != "$canonical" ]] \
        || die "OFFERED_RPS_STEPS contains a numerically duplicate rate: ${rate}"
    done
  fi
  rates+=("$rate")
  canonical_rates+=("$canonical")
done
set +f
(( ${#rates[@]} > 0 )) || die "OFFERED_RPS_STEPS must not be empty"

cell_count=$((${#rates[@]} * REPEATS))
endpoint_span=$((MAX_ROWS_PER_ENDPOINT + CONNECTIONS))
cell_stride=$((REPLICAS * endpoint_span))
reserved_end=$((SEQUENCE_BASE + cell_count * cell_stride))
(( reserved_end >= SEQUENCE_BASE && reserved_end <= 4611686018427387904 )) \
  || die "sequence reservation exceeds the exact-token generator capacity"

mkdir -p "$CELL_RESULT_ROOT"
sweep_dir_owned=1
plan=${SWEEP_DIR}/sweep-plan.tsv
printf 'order\trepetition\toffered_rps_per_target\tscheduled_slots_per_target\tsequence_base\trun_id\n' >"$plan"
order=0

append_plan_cell() {
  local repetition=$1 rate=$2 slots sequence cell_id
  order=$((order + 1))
  slots=$(offered_slots_for "$rate")
  (( slots <= MAX_ROWS_PER_ENDPOINT )) \
    || die "rate ${rate} needs ${slots} rows per endpoint; MAX_ROWS_PER_ENDPOINT=${MAX_ROWS_PER_ENDPOINT}"
  sequence=$((SEQUENCE_BASE + (order - 1) * cell_stride))
  cell_id=$(printf 'ol-%s-o%03d' "$SWEEP_RUN_ID" "$order")
  (( ${#cell_id} <= 50 )) || die "generated RUN_ID is too long: ${cell_id}"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$order" "$repetition" "$rate" \
    "$slots" "$sequence" "$cell_id" >>"$plan"
}

for ((repetition = 1; repetition <= REPEATS; repetition++)); do
  if [[ "$ORDER_MODE" == randomized ]]; then
    while IFS=$'\t' read -r _ _ rate; do
      append_plan_cell "$repetition" "$rate"
    done < <(
      for index in "${!rates[@]}"; do
        rate=${rates[$index]}
        key=$(printf '%s' "${SWEEP_SEED}:${repetition}:${canonical_rates[$index]}" \
          | cksum | awk '{print $1}')
        printf '%010u\t%04u\t%s\n' "$key" "$index" "$rate"
      done | LC_ALL=C sort -k1,1n -k2,2n
    )
  else
    for rate in "${rates[@]}"; do append_plan_cell "$repetition" "$rate"; done
  fi
done

[[ "$order" == "$cell_count" ]] || die "internal error: planned cell count mismatch"
duplicate_sequences=$(awk -F '\t' 'NR > 1 {print $5}' "$plan" \
  | sort -n | uniq -d | wc -l | awk '{print $1}')
[[ "$duplicate_sequences" == 0 ]] || die "internal error: duplicate sequence base"

probe_source=${DRIVER_PROBE_SOURCE:-${REPO_ROOT}/instrumentation/reference/src/bin/sustained-corpus-probe.rs}
[[ -s "$probe_source" ]] || die "missing probe source: ${probe_source}"
local_probe_source_sha256=$("${sha256[@]}" "$probe_source" | awk '{print $1}')
runtime_source_linkage_attested=false
if [[ -n "$DRIVER_BUILD_SOURCE_SHA256" \
      && "$DRIVER_BUILD_SOURCE_SHA256" == "$local_probe_source_sha256" ]]; then
  runtime_source_linkage_attested=true
fi

git -C "$REPO_ROOT" rev-parse HEAD >"${SWEEP_DIR}/git-head.txt"
git -C "$REPO_ROOT" status --porcelain=v1 >"${SWEEP_DIR}/git-status.txt"
harness_files=("$0" "$CELL_RUNNER" "$METRICS_RUNNER" "$SUMMARY_RUNNER" "$probe_source")
if (( TOPOLOGY_PREFLIGHT_ENABLED == 1 )); then
  harness_files+=("$TOPOLOGY_PREFLIGHT_RUNNER")
fi
"${sha256[@]}" "${harness_files[@]}" >"${SWEEP_DIR}/harness-sha256.txt"

jq -n \
  --arg run_id "$SWEEP_RUN_ID" \
  --arg created_at "$(date -u +%FT%TZ)" \
  --arg offered_rps_steps "$OFFERED_RPS_STEPS" \
  --arg namespace "$NAMESPACE" \
  --arg deployment "$DEPLOYMENT" \
  --arg target_selector "$TARGET_SELECTOR" \
  --arg target_node "$TARGET_NODE" \
  --arg driver_node "$DRIVER_NODE" \
  --arg otel_daemonset "$OTEL_DAEMONSET" \
  --arg driver_image "$DRIVER_IMAGE" \
  --arg target_image "$TARGET_IMAGE" \
  --arg model_sha256 "$MODEL_SHA256" \
  --arg tokenizer_sha256 "$TOKENIZER_SHA256" \
  --arg order_mode "$ORDER_MODE" \
  --arg sweep_seed "$SWEEP_SEED" \
  --arg reset_targets "$RESET_TARGETS" \
  --arg local_probe_source_sha256 "$local_probe_source_sha256" \
  --arg driver_build_source_sha256 "$DRIVER_BUILD_SOURCE_SHA256" \
  --arg topology_preflight_runner "$TOPOLOGY_PREFLIGHT_RUNNER" \
  --arg topology_preflight_container "$TOPOLOGY_PREFLIGHT_CONTAINER" \
  --arg topology_preflight_reserved_cpus "$TOPOLOGY_PREFLIGHT_RESERVED_CPUS" \
  --argjson runtime_source_linkage_attested "$runtime_source_linkage_attested" \
  --argjson topology_preflight_enabled "$TOPOLOGY_PREFLIGHT_ENABLED" \
  --argjson replicas "$REPLICAS" \
  --argjson repeats "$REPEATS" \
  --argjson concurrency "$CONCURRENCY" \
  --argjson connections "$CONNECTIONS" \
  --argjson duration_seconds "$DURATION_SECONDS" \
  --argjson start_delay_seconds "$START_DELAY_SECONDS" \
  --argjson quiescence_seconds "$QUIESCENCE_SECONDS" \
  --argjson token_count "$TOKEN_COUNT" \
  --argjson max_rows_per_endpoint "$MAX_ROWS_PER_ENDPOINT" \
  --argjson sequence_base "$SEQUENCE_BASE" \
  --argjson cell_stride "$cell_stride" \
  --argjson endpoint_span "$endpoint_span" \
  --argjson reserved_end "$reserved_end" \
  --argjson max_in_flight "$MAX_IN_FLIGHT" \
  --argjson dispatch_late "$DISPATCH_LATE_AFTER_MS" \
  --argjson drop_late "$DROP_LATE_AFTER_MS" \
  --argjson rpc_timeout "$RPC_TIMEOUT_MS" \
  --argjson capture_telemetry "$CAPTURE_TELEMETRY" \
  --argjson metric_max_gap_seconds "$METRIC_MAX_GAP_SECONDS" \
  --arg max_scheduler_p99_lag_ms "$MAX_SCHEDULER_P99_LAG_MS" \
  --arg max_schedule_drop_ratio "$MAX_SCHEDULE_DROP_RATIO" \
  '{schema_version:2,run_id:$run_id,created_at:$created_at,
    protocol:"deterministic_offered_rate_v1",
    load_scope:"per target; one independent scheduler per direct Pod IP",
    namespace:$namespace,deployment:$deployment,target_selector:$target_selector,target_node:$target_node,
    driver_node:$driver_node,
    topology:(if $target_node == $driver_node
      then ("same-node-direct-" + $target_node)
      else ("cross-node-direct-" + $target_node + "-from-" + $driver_node) end),
    offered_rps_steps:$offered_rps_steps,replicas:$replicas,repeats:$repeats,
    order:{mode:$order_mode,seed:(if $sweep_seed == "" then null else $sweep_seed end)},
    duration_seconds:$duration_seconds,start_delay_seconds:$start_delay_seconds,
    quiescence_seconds:$quiescence_seconds,
    target_lifecycle:{fresh_targets_per_cell:($reset_targets == "true")},
    concurrency:$concurrency,connections:$connections,token_count:$token_count,
    max_rows_per_endpoint:$max_rows_per_endpoint,driver_image:$driver_image,
    target_image:$target_image,model_sha256:$model_sha256,
    tokenizer_sha256:$tokenizer_sha256,
    source_attestation:{local_probe_source_sha256:$local_probe_source_sha256,
      driver_build_source_sha256:(if $driver_build_source_sha256 == "" then null else $driver_build_source_sha256 end),
      runtime_source_linkage_attested:$runtime_source_linkage_attested,
      note:(if $runtime_source_linkage_attested then
        "operator-supplied build source hash matches the local probe source"
        else "runtime behavior is attested by image digest and emitted protocol fields; exact source linkage is not attested" end)},
    sequence:{base:$sequence_base,endpoint_span:$endpoint_span,
      cell_stride:$cell_stride,reserved_end_exclusive:$reserved_end},
    scheduler:{max_in_flight_per_target:$max_in_flight,connections_per_target:$connections,
      dispatch_late_after_ms:$dispatch_late,drop_late_after_ms:$drop_late,
      rpc_timeout_ms:$rpc_timeout,
      attribution_gate:{max_dispatch_p99_lag_ms:($max_scheduler_p99_lag_ms|tonumber),
        max_schedule_drop_ratio:($max_schedule_drop_ratio|tonumber)}},
    telemetry:{required:($capture_telemetry == 1),otel_daemonset:$otel_daemonset,
      query_step_seconds:5,completeness_max_gap_seconds:$metric_max_gap_seconds,
      authoritative:["pod_cpu_otel","container_cpu_otel","memory_working_set",
        "restarts","pod_ready","node_ready","direct_cgroup"],
      supporting:["container_cpu_cadvisor","throttle_ratio","cpu_pressure_waiting"]},
    topology_preflight:{required:($topology_preflight_enabled == 1),
      fail_closed:true,runner:$topology_preflight_runner,
      selector:$target_selector,expected_pods:$replicas,
      container:(if $topology_preflight_container == "" then null else $topology_preflight_container end),
      reserved_cpu_overrides:(if $topology_preflight_reserved_cpus == "" then null else $topology_preflight_reserved_cpus end),
      phase:"after target readiness/provenance checks and before start-time calculation, sampling, or driver Job creation",
      per_cell_evidence:["topology-preflight-report.json","topology-preflight-execution.json",
        "topology-preflight-stdout.txt","topology-preflight-stderr.txt"]},
    recovery:{collected:false,
      limitation:"this sweep does not establish same-Pod functional recovery after overload"},
    limitations:{driver_resource_telemetry:false,
      per_request_timestamps_or_time_bins:false,
      cpu_sibling_topology_attestation:($topology_preflight_enabled == 1),
      note:(if $topology_preflight_enabled == 1 then
        "dispatch quality and live CPU-sibling placement are gated; driver resource telemetry and per-request time bins remain gaps"
        else
        "CPU-sibling placement gating was explicitly disabled; driver resource telemetry and per-request time bins also remain gaps" end)},
    interpretation:{offered_load:"scheduled slots, including explicit pre-initiation drops",
      useful_throughput:"OK completions within the plateau",
      latency_population:"OK completions within the plateau only",
      validity:"accounting, provenance, runtime invariance, scheduler attribution, and required telemetry are distinct from SC saturation signals"}}' \
  >"${SWEEP_DIR}/sweep-provenance.json"

if (( PLAN_ONLY == 1 )); then
  current_cell=plan-only
  plan_only_complete=1
  cat "$plan"
  exit 0
fi

require_command oc
if (( CAPTURE_TELEMETRY == 1 )); then require_command curl; fi

if (( REPLICAS >= 30 && START_DELAY_SECONDS < 180 )); then
  die "REPLICAS >= 30 requires START_DELAY_SECONDS >= 180 so all independent drivers can become ready"
fi

for spec in \
  "get:deployment/${DEPLOYMENT}" \
  "get:deployments.apps" \
  "watch:deployments.apps" \
  "patch:deployments.apps" \
  "get:deployments.apps/scale" \
  "update:deployments.apps/scale" \
  "get:pods" "list:pods" "watch:pods" "delete:pods" \
  "get:pods/log" "create:pods/exec" \
  "get:jobs.batch" "list:jobs.batch" "watch:jobs.batch" \
  "create:jobs.batch" "delete:jobs.batch" \
  "get:events" "list:events" \
  "get:configmaps" "create:configmaps" "delete:configmaps"; do
  require_access "${spec%%:*}" "${spec#*:}" "$NAMESPACE"
done
for verb in get list watch; do require_access "$verb" nodes; done
if (( CAPTURE_TELEMETRY == 1 )); then
  require_access get daemonsets.apps "$NAMESPACE"
  require_access get routes.route.openshift.io openshift-monitoring
fi

"${k[@]}" get deployment "$DEPLOYMENT" -n "$NAMESPACE" -o json \
  >"${SWEEP_DIR}/deployment-original.json"
original_replicas=$(jq -r '.spec.replicas // 0' "${SWEEP_DIR}/deployment-original.json")
[[ "$original_replicas" =~ ^[0-9]+$ ]] || die "could not determine original replica count"
for node in "$TARGET_NODE" "$DRIVER_NODE"; do
  "${k[@]}" get node "$node" -o json \
    | jq -e 'any(.status.conditions[]?; .type == "Ready" and .status == "True")' >/dev/null \
    || die "node ${node} is not Ready"
done
if (( CAPTURE_TELEMETRY == 1 )); then
  "${k[@]}" get daemonset "$OTEL_DAEMONSET" -n "$NAMESPACE" -o json \
    >"${SWEEP_DIR}/otel-daemonset-preflight.json"
  jq -e '(.status.desiredNumberScheduled // 0) > 0
    and .status.numberReady == .status.desiredNumberScheduled
    and (.status.numberUnavailable // 0) == 0' \
    "${SWEEP_DIR}/otel-daemonset-preflight.json" >/dev/null \
    || die "OTEL DaemonSet is not fully Ready"
fi

if ! "${k[@]}" create configmap "$LOCK_NAME" -n "$NAMESPACE" \
    --from-literal="run-id=${SWEEP_RUN_ID}" \
    --from-literal="kind=open-loop-sweep" \
    --from-literal="created-at=$(date -u +%FT%TZ)" >/dev/null; then
  "${k[@]}" get configmap "$LOCK_NAME" -n "$NAMESPACE" -o yaml >&2 || true
  die "another benchmark owns ${NAMESPACE}/${LOCK_NAME}"
fi
lock_acquired=1
"${k[@]}" version -o json >"${SWEEP_DIR}/oc-version.json"
"${k[@]}" whoami >"${SWEEP_DIR}/cluster-identity.txt"

while IFS=$'\t' read -r order repetition rate _ sequence cell_id; do
  [[ "$order" == order ]] && continue
  current_cell=$cell_id
  if (( order > 1 && QUIESCENCE_SECONDS > 0 )); then sleep "$QUIESCENCE_SECONDS"; fi
  active_run_id=$cell_id
  cluster_mutated=1
  set +e
  env \
    KUBECONFIG_PATH="$KUBECONFIG_PATH" NAMESPACE="$NAMESPACE" DEPLOYMENT="$DEPLOYMENT" \
    TARGET_SELECTOR="$TARGET_SELECTOR" \
    TARGET_NODE="$TARGET_NODE" DRIVER_NODE="$DRIVER_NODE" \
    REPLICAS="$REPLICAS" CONCURRENCY="$CONCURRENCY" CONNECTIONS="$CONNECTIONS" \
    DURATION_SECONDS="$DURATION_SECONDS" START_DELAY_SECONDS="$START_DELAY_SECONDS" \
    MAX_ROWS_PER_ENDPOINT="$MAX_ROWS_PER_ENDPOINT" SEQUENCE_BASE="$sequence" \
    RUN_ID="$cell_id" DRIVER_IMAGE="$DRIVER_IMAGE" TARGET_IMAGE="$TARGET_IMAGE" \
    MODEL_SHA256="$MODEL_SHA256" TOKENIZER_SHA256="$TOKENIZER_SHA256" \
    TOKEN_COUNT="$TOKEN_COUNT" RESULT_ROOT="$CELL_RESULT_ROOT" \
    RESET_TARGETS="$RESET_TARGETS" OFFERED_RPS="$rate" \
    MAX_IN_FLIGHT="$MAX_IN_FLIGHT" DISPATCH_LATE_AFTER_MS="$DISPATCH_LATE_AFTER_MS" \
    DROP_LATE_AFTER_MS="$DROP_LATE_AFTER_MS" RPC_TIMEOUT_MS="$RPC_TIMEOUT_MS" \
    TOPOLOGY_PREFLIGHT_ENABLED="$TOPOLOGY_PREFLIGHT_ENABLED" \
    TOPOLOGY_PREFLIGHT_RUNNER="$TOPOLOGY_PREFLIGHT_RUNNER" \
    TOPOLOGY_PREFLIGHT_CONTAINER="$TOPOLOGY_PREFLIGHT_CONTAINER" \
    TOPOLOGY_PREFLIGHT_RESERVED_CPUS="$TOPOLOGY_PREFLIGHT_RESERVED_CPUS" \
    "$CELL_RUNNER" >/dev/null
  cell_runner_exit=$?
  set -e
  cell_dir=${CELL_RESULT_ROOT}/${cell_id}
  if (( cell_runner_exit != 0 )); then
    if (( cell_runner_exit == 6 )) \
        && [[ -s "$cell_dir/topology-preflight-execution.json" ]] \
        && jq -e '.disposition == "invalid_pre_load" and .load_authorized == false' \
          "$cell_dir/topology-preflight-execution.json" >/dev/null 2>&1; then
      sweep_invalid=1
      die "${cell_id}: CPU-topology preflight invalidated the cell before driver load"
    fi
    die "${cell_id}: cell runner exited ${cell_runner_exit}"
  fi
  if (( CAPTURE_TELEMETRY == 1 )); then
    KUBECONFIG_PATH="$KUBECONFIG_PATH" "$METRICS_RUNNER" "$cell_dir" >/dev/null
  fi
  jq -e --arg rate "$rate" --arg run_id "$cell_id" --argjson sequence "$sequence" \
      --argjson topology_preflight_required "$TOPOLOGY_PREFLIGHT_ENABLED" '
    .load_model == "open_loop_deterministic_offered_rate"
    and .cell.run_id == $run_id
    and .cell.open_loop.offered_rps_per_target == $rate
    and .cell.sequence_base == $sequence
    and .accounting_valid
    and (.workers_late | not)
    and (.corpus_exhausted | not)
    and (.cell.topology_preflight.enabled == ($topology_preflight_required == 1))
    and (if $topology_preflight_required == 1 then
      (.cell.topology_preflight.required_by_caller == true
       and .cell.topology_preflight.load_authorized == true
       and .cell.topology_preflight.disposition == "pass"
       and .cell.topology_preflight.report_verdict == "PASS"
       and .cell.topology_preflight.placement_verdict == "PASS")
      else true end)
  ' "${CELL_RESULT_ROOT}/${cell_id}/summary.json" >/dev/null \
    || die "${cell_id}: open-loop summary gate failed"
  if (( DELETE_COMPLETED_JOBS == 1 )); then
    "${k[@]}" delete jobs -n "$NAMESPACE" \
      -l "benchmark.llm-d/run-id=${cell_id}" \
      --ignore-not-found --cascade=foreground --wait=true --timeout=120s >/dev/null
  fi
  active_run_id=""
done <"$plan"

"$SUMMARY_RUNNER" \
  --plan "$plan" \
  --cell-root "$CELL_RESULT_ROOT" \
  --provenance "${SWEEP_DIR}/sweep-provenance.json" \
  --output-json "${SWEEP_DIR}/sweep-summary.json" \
  --output-tsv "${SWEEP_DIR}/sweep-summary.tsv" \
  --max-scheduler-p99-lag-ms "$MAX_SCHEDULER_P99_LAG_MS" \
  --max-schedule-drop-ratio "$MAX_SCHEDULE_DROP_RATIO" \
  --telemetry-required "$CAPTURE_TELEMETRY" \
  --metric-max-gap-seconds "$METRIC_MAX_GAP_SECONDS"
current_cell=complete
sweep_complete=1
