#!/usr/bin/env bash
set -euo pipefail

# Orchestrate repeatable semantic-classifier inference matrices on Arena.
#
# This script is intentionally separate from arena-sc-inference-cell.sh. The
# cell runner owns one synchronized direct-Pod-IP measurement; this script owns
# experiment order, fresh target rollouts, worker-width changes, staged replica
# growth, recovery observation, provenance, and validity gates.
#
# Required inputs:
#   DRIVER_IMAGE, TARGET_IMAGE, MODEL_SHA256, SEQUENCE_BASE, MATRIX_SEED
#
# Common examples:
#   MATRIX_PHASES=worker WORKER_WIDTHS="1 2 4 8" \
#     WORKER_CONCURRENCIES="1 2 4 8 16" REPEATS=5 ... ./hack/arena-sc-inference-matrix.sh
#
#   MATRIX_PHASES=horizontal HORIZONTAL_WORKERS=4 HORIZONTAL_CONCURRENCY=4 \
#     HORIZONTAL_REPLICAS="1 3 5 7 10" REPEATS=5 ... ./hack/arena-sc-inference-matrix.sh
#
#   MATRIX_PHASES="worker horizontal" ... ./hack/arena-sc-inference-matrix.sh

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

KUBECONFIG_PATH=${KUBECONFIG_PATH:-/tmp/llm-d-sc-arena-kubeconfig}
NAMESPACE=${NAMESPACE:-llm-d-sc-scaleout}
DEPLOYMENT=${DEPLOYMENT:-classifier-target}
TARGET_SELECTOR=${TARGET_SELECTOR:-app.kubernetes.io/component=classifier-target}
TARGET_CONTAINER=${TARGET_CONTAINER:-llm-d-sc}
SERVICE_NAME=${SERVICE_NAME:-classifier-target}
TARGET_NODE=${TARGET_NODE:-gnr2.fm2aihpcsed.com}
DRIVER_NODE=${DRIVER_NODE:-rhgnr1}
WORKER_ENV_NAME=${WORKER_ENV_NAME:-LLM_D_SC_INFERENCE_WORKERS}
OTEL_DAEMONSET=${OTEL_DAEMONSET:-llm-d-sc-otel}

DRIVER_IMAGE=${DRIVER_IMAGE:?set the pinned benchmark-driver image digest}
TARGET_IMAGE=${TARGET_IMAGE:?set the expected pinned target image digest}
MODEL_SHA256=${MODEL_SHA256:?set MODEL_SHA256}
TOKENIZER_SHA256=${TOKENIZER_SHA256:-851ca67100d372ca3ae031a6abd168f53489eebfd7d89523f35c5c9b4d372c3c}
SEQUENCE_BASE=${SEQUENCE_BASE:?set a globally unused SEQUENCE_BASE}
MATRIX_SEED=${MATRIX_SEED:?set an integer MATRIX_SEED for reproducible randomization}

MATRIX_RUN_ID=${MATRIX_RUN_ID:-sc-knee-$(date -u +%Y%m%d%H%M%S)}
MATRIX_PHASES=${MATRIX_PHASES:-worker horizontal}
WORKER_WIDTHS=${WORKER_WIDTHS:-1 2 4 8}
WORKER_CONCURRENCIES=${WORKER_CONCURRENCIES:-1 2 4 8 16}
WORKER_REPLICAS=${WORKER_REPLICAS:-1}
HORIZONTAL_WORKERS=${HORIZONTAL_WORKERS:-4}
HORIZONTAL_CONCURRENCY=${HORIZONTAL_CONCURRENCY:-4}
HORIZONTAL_CONNECTIONS=${HORIZONTAL_CONNECTIONS:-$HORIZONTAL_CONCURRENCY}
HORIZONTAL_REPLICAS=${HORIZONTAL_REPLICAS:-1 3 5 7 10}
REPEATS=${REPEATS:-5}
RUN_BASELINE=${RUN_BASELINE:-1}
BASELINE_WORKERS=${BASELINE_WORKERS:-4}
BASELINE_REPLICAS=${BASELINE_REPLICAS:-1}
BASELINE_CONCURRENCY=${BASELINE_CONCURRENCY:-1}
BASELINE_CONNECTIONS=${BASELINE_CONNECTIONS:-$BASELINE_CONCURRENCY}

DURATION_SECONDS=${DURATION_SECONDS:-180}
START_DELAY_SECONDS=${START_DELAY_SECONDS:-45}
TOKEN_COUNT=${TOKEN_COUNT:-64}
MAX_ROWS_PER_ENDPOINT=${MAX_ROWS_PER_ENDPOINT:-10000}
MAX_SCALE_STEP=${MAX_SCALE_STEP:-2}
SCALE_SETTLE_SECONDS=${SCALE_SETTLE_SECONDS:-15}
QUIESCENCE_SECONDS=${QUIESCENCE_SECONDS:-60}
RECOVERY_CHECKPOINTS=${RECOVERY_CHECKPOINTS:-5 30 60 120}
RECOVERY_MAX_DELAY_SECONDS=${RECOVERY_MAX_DELAY_SECONDS:-15}
ROLLOUT_TIMEOUT_SECONDS=${ROLLOUT_TIMEOUT_SECONDS:-600}
DELETE_COMPLETED_JOBS=${DELETE_COMPLETED_JOBS:-1}
RESTORE_ORIGINAL_ON_SUCCESS=${RESTORE_ORIGINAL_ON_SUCCESS:-1}
PLAN_ONLY=${PLAN_ONLY:-0}
METRIC_STEP_SECONDS=${METRIC_STEP_SECONDS:-5}
METRIC_MAX_GAP_SECONDS=${METRIC_MAX_GAP_SECONDS:-10}
# The current cAdvisor-derived range queries use 30-second rate windows against
# a sparse source, so they are supporting attribution rather than health gates.
# The 65-second bound grades per-Pod coverage; complete OTEL/health telemetry
# and direct cgroup snapshots retain their strict fail-closed gates.
AUX_METRIC_MAX_GAP_SECONDS=${AUX_METRIC_MAX_GAP_SECONDS:-65}

RESULT_ROOT=${RESULT_ROOT:-${REPO_ROOT}/results}
MATRIX_DIR=${MATRIX_DIR:-${RESULT_ROOT}/matrices/${MATRIX_RUN_ID}}
CELL_RESULT_ROOT=${CELL_RESULT_ROOT:-${MATRIX_DIR}/cells}
CELL_RUNNER=${CELL_RUNNER:-${SCRIPT_DIR}/arena-sc-inference-cell.sh}
METRICS_RUNNER=${METRICS_RUNNER:-${SCRIPT_DIR}/arena-sc-capture-thanos.sh}
LOCK_NAME=${LOCK_NAME:-sc-benchmark-matrix-lock}

k=(oc --kubeconfig "$KUBECONFIG_PATH")
lock_acquired=0
cluster_mutated=0
matrix_complete=0
plan_only_complete=0
matrix_dir_owned=0
current_cell="preflight"
original_replicas=""
original_workers=""
original_workers_present=0
last_error=""
active_driver_run_id=""
active_recovery_pid=""

die() {
  last_error=$*
  echo "ERROR: ${last_error}" >&2
  if (( matrix_dir_owned == 1 )); then
    printf '%s\n' "$last_error" >"${MATRIX_DIR}/matrix-error.txt"
  fi
  exit 1
}

append_error() {
  if [[ -n "$last_error" ]]; then
    last_error="${last_error}; $1"
  else
    last_error=$1
  fi
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

assert_uint() {
  local name=$1 value=$2
  [[ "$value" =~ ^(0|[1-9][0-9]*)$ ]] \
    || die "${name} must be a base-10 unsigned integer without leading zeros; got ${value}"
}

assert_positive() {
  local name=$1 value=$2
  assert_uint "$name" "$value"
  (( value > 0 )) || die "${name} must be greater than zero"
}

normalize_list() {
  printf '%s\n' "${1//,/ }"
}

node_is_ready() {
  local node=$1
  "${k[@]}" get "node/${node}" -o json \
    | jq -e 'any(.status.conditions[]?; .type == "Ready" and .status == "True")' >/dev/null
}

telemetry_preflight() {
  local prom_host auth_token otel_json
  otel_json=$("${k[@]}" get daemonset/"$OTEL_DAEMONSET" -n "$NAMESPACE" -o json)
  jq -e '
    (.status.desiredNumberScheduled // 0) > 0
    and .status.numberReady == .status.desiredNumberScheduled
    and (.status.numberUnavailable // 0) == 0
  ' <<<"$otel_json" >/dev/null || die "OTEL DaemonSet is not fully Ready"
  jq . <<<"$otel_json" >"${MATRIX_DIR}/otel-daemonset-preflight.json"
  prom_host=$("${k[@]}" -n openshift-monitoring get route thanos-querier \
    -o jsonpath='{.spec.host}')
  [[ -n "$prom_host" ]] || die "Thanos route has no host"
  auth_token=$("${k[@]}" whoami -t)
  curl -ksS --connect-timeout 10 --max-time 30 --get \
    -H "Authorization: Bearer ${auth_token}" \
    --data-urlencode 'query=vector(1)' \
    "https://${prom_host}/api/v1/query" >"${MATRIX_DIR}/telemetry-preflight.json"
  unset auth_token
  jq -e '.status == "success" and (.data.result | length) == 1' \
    "${MATRIX_DIR}/telemetry-preflight.json" >/dev/null \
    || die "Thanos preflight query failed"
}

require_access() {
  local verb=$1 resource=$2 namespace=${3:-}
  local allowed
  if [[ -n "$namespace" ]]; then
    allowed=$("${k[@]}" auth can-i "$verb" "$resource" -n "$namespace")
  else
    allowed=$("${k[@]}" auth can-i "$verb" "$resource")
  fi
  [[ "$allowed" == yes ]] \
    || die "current identity cannot ${verb} ${resource}${namespace:+ in ${namespace}}"
}

target_pod_count() {
  "${k[@]}" get pods -n "$NAMESPACE" -l "$TARGET_SELECTOR" -o json \
    | jq '.items | length'
}

wait_for_target_deletion() {
  if (( $(target_pod_count) == 0 )); then
    return
  fi
  "${k[@]}" wait --for=delete pod -n "$NAMESPACE" -l "$TARGET_SELECTOR" \
    --timeout="${ROLLOUT_TIMEOUT_SECONDS}s" >/dev/null
}

set_workers() {
  local workers=$1
  "${k[@]}" set env deployment/"$DEPLOYMENT" -n "$NAMESPACE" \
    --containers="$TARGET_CONTAINER" "${WORKER_ENV_NAME}=${workers}" >/dev/null
}

restore_worker_setting() {
  if (( original_workers_present == 1 )); then
    "${k[@]}" set env deployment/"$DEPLOYMENT" -n "$NAMESPACE" \
      --containers="$TARGET_CONTAINER" "${WORKER_ENV_NAME}=${original_workers}" >/dev/null
  else
    "${k[@]}" set env deployment/"$DEPLOYMENT" -n "$NAMESPACE" \
      --containers="$TARGET_CONTAINER" "${WORKER_ENV_NAME}-" >/dev/null
  fi
}

delete_active_driver_jobs() {
  local delete_failed=0 job_delete_pid
  if [[ -n "$active_driver_run_id" ]]; then
    "${k[@]}" delete jobs -n "$NAMESPACE" \
      -l "benchmark.llm-d/run-id=${active_driver_run_id}" \
      --ignore-not-found --cascade=foreground --wait=true --timeout=120s >/dev/null &
    job_delete_pid=$!
    if ! "${k[@]}" delete pods -n "$NAMESPACE" \
      -l "benchmark.llm-d/run-id=${active_driver_run_id}" \
      --ignore-not-found --wait=true --timeout=120s >/dev/null; then
      delete_failed=1
    fi
    if ! wait "$job_delete_pid"; then
      delete_failed=1
    fi
    (( delete_failed == 0 )) || return 1
    active_driver_run_id=""
  fi
}

child_job_is_running() {
  local wanted_pid=$1 running_pid
  [[ "$wanted_pid" =~ ^[1-9][0-9]*$ ]] || return 1
  for running_pid in $(jobs -pr); do
    [[ "$running_pid" == "$wanted_pid" ]] && return 0
  done
  return 1
}

cancel_active_recovery() {
  local pid
  # Recovery must stop before target teardown so it cannot race cleanup with a
  # late health or Events API read. Only signal PIDs still owned by this shell;
  # that avoids a stale-PID reuse hazard if a child finished just before EXIT.
  pid=$active_recovery_pid
  if child_job_is_running "$pid"; then
    kill "$pid" 2>/dev/null || true
  fi
  if [[ "$pid" =~ ^[1-9][0-9]*$ ]]; then
    wait "$pid" 2>/dev/null || true
  fi
  active_recovery_pid=""
}

scale_in_steps() {
  local desired=$1 current=0 next
  while (( current < desired )); do
    next=$((current + MAX_SCALE_STEP))
    if (( next > desired )); then
      next=$desired
    fi
    "${k[@]}" scale deployment/"$DEPLOYMENT" -n "$NAMESPACE" \
      --replicas="$next" >/dev/null
    "${k[@]}" rollout status deployment/"$DEPLOYMENT" -n "$NAMESPACE" \
      --timeout="${ROLLOUT_TIMEOUT_SECONDS}s" >/dev/null
    current=$next
    if (( current < desired && SCALE_SETTLE_SECONDS > 0 )); then
      sleep "$SCALE_SETTLE_SECONDS"
      node_is_ready "$TARGET_NODE" || die "target node lost Ready while scaling to ${desired}"
      node_is_ready "$DRIVER_NODE" || die "driver node lost Ready while scaling to ${desired}"
    fi
  done
}

assert_target_health() {
  local expected_replicas=$1 expected_workers=$2 output=${3:-}
  local pods_json deployment_json
  pods_json=$("${k[@]}" get pods -n "$NAMESPACE" -l "$TARGET_SELECTOR" -o json)
  deployment_json=$("${k[@]}" get deployment/"$DEPLOYMENT" -n "$NAMESPACE" -o json)

  jq -e --argjson replicas "$expected_replicas" --arg node "$TARGET_NODE" \
    --arg digest "$TARGET_IMAGE" --arg container "$TARGET_CONTAINER" \
    '(.items | length) == $replicas
     and all(.items[];
       .metadata.deletionTimestamp == null
       and .status.phase == "Running"
       and .spec.nodeName == $node
       and any(.status.conditions[]?; .type == "Ready" and .status == "True")
       and ([.status.containerStatuses[]?.restartCount] | add // 0) == 0
       and any(.status.containerStatuses[]?;
         .name == $container and (.imageID | endswith($digest))))' \
    <<<"$pods_json" >/dev/null || die "target Pod health/provenance gate failed"

  jq -e --arg container "$TARGET_CONTAINER" --arg env "$WORKER_ENV_NAME" \
    --arg workers "$expected_workers" \
    'any(.spec.template.spec.containers[]?;
       .name == $container
       and any(.env[]?; .name == $env and .value == $workers))' \
    <<<"$deployment_json" >/dev/null || die "deployment worker-width gate failed"

  node_is_ready "$TARGET_NODE" || die "target node is not Ready"
  node_is_ready "$DRIVER_NODE" || die "driver node is not Ready"

  if [[ -n "$output" ]]; then
    jq . <<<"$pods_json" >"${output}-targets.json"
    jq . <<<"$deployment_json" >"${output}-deployment.json"
    "${k[@]}" get nodes "$TARGET_NODE" "$DRIVER_NODE" -o json >"${output}-nodes.json"
  fi
}

fresh_targets() {
  local workers=$1 replicas=$2 cell_dir=$3
  cluster_mutated=1
  "${k[@]}" scale deployment/"$DEPLOYMENT" -n "$NAMESPACE" --replicas=0 >/dev/null
  wait_for_target_deletion
  set_workers "$workers"
  scale_in_steps "$replicas"
  assert_target_health "$replicas" "$workers" "${cell_dir}/rollout"
  if (( QUIESCENCE_SECONDS > 0 )); then
    sleep "$QUIESCENCE_SECONDS"
  fi
  assert_target_health "$replicas" "$workers" "${cell_dir}/quiescent"
}

metric_series_complete() {
  local file=$1 label=$2 targets_file=$3 start=$4 end=$5 max_gap=${6:-$METRIC_MAX_GAP_SECONDS}
  jq -e --arg label "$label" --argjson start "$start" --argjson end "$end" \
    --argjson max_gap "$max_gap" --slurpfile targets "$targets_file" '
      def window_values:
        [.values[]? | select(.[0] >= $start and .[0] <= $end)];
      def complete($values):
        ($values | length) > 0
        and ($values[0][0] - $start) <= $max_gap
        and ($end - $values[-1][0]) <= $max_gap
        and ([range(1; $values | length) as $i |
              $values[$i][0] - $values[$i - 1][0]] | max // 0) <= $max_gap;
      . as $doc
      | [$targets[0].items[].metadata.name] as $pods
      | $doc.status == "success"
      and all($pods[];
        . as $pod
        | any($doc.data.result[]?;
            .metric[$label] == $pod and complete(window_values)))
    ' "$file" >/dev/null
}

metric_values_are() {
  local file=$1 label=$2 targets_file=$3 start=$4 end=$5 expected=$6
  jq -e --arg label "$label" --argjson start "$start" --argjson end "$end" \
    --argjson expected "$expected" --slurpfile targets "$targets_file" '
      . as $doc
      | [$targets[0].items[].metadata.name] as $pods
      | all($pods[];
          . as $pod
          | any($doc.data.result[]?;
              .metric[$label] == $pod
              and ([.values[]? | select(.[0] >= $start and .[0] <= $end)]) as $values
              | ($values | length) > 0
                and all($values[]; (.[1] | tonumber) == $expected)))
    ' "$file" >/dev/null
}

named_metric_series_complete() {
  local file=$1 label=$2 name=$3 start=$4 end=$5
  jq -e --arg label "$label" --arg name "$name" --argjson start "$start" \
    --argjson end "$end" --argjson max_gap "$METRIC_MAX_GAP_SECONDS" '
      def window_values:
        [.values[]? | select(.[0] >= $start and .[0] <= $end)];
      def complete($values):
        ($values | length) > 0
        and ($values[0][0] - $start) <= $max_gap
        and ($end - $values[-1][0]) <= $max_gap
        and ([range(1; $values | length) as $i |
              $values[$i][0] - $values[$i - 1][0]] | max // 0) <= $max_gap;
      .status == "success"
      and any(.data.result[]?;
        .metric[$label] == $name and complete(window_values))
    ' "$file" >/dev/null
}

validate_metrics() {
  local cell_dir=$1 start end target_file metric label auxiliary_quality_ndjson
  target_file=${cell_dir}/targets-before.json
  start=$(jq -r '.start_epoch_ms / 1000 | floor' "${cell_dir}/cell.json")
  end=$((start + DURATION_SECONDS))

  for spec in \
    pod_cpu_otel:k8s_pod_name \
    container_cpu_otel:k8s_pod_name \
    memory_working_set:pod \
    restarts:pod \
    pod_ready:pod; do
    metric=${spec%%:*}
    label=${spec#*:}
    [[ -s "${cell_dir}/metrics/${metric}.json" ]] \
      || die "${current_cell}: missing ${metric} telemetry"
    metric_series_complete "${cell_dir}/metrics/${metric}.json" "$label" \
      "$target_file" "$start" "$end" \
      || die "${current_cell}: incomplete ${metric} telemetry"
  done

  # Arena's cAdvisor counter/rate series are sparse even when the OTEL pod CPU,
  # Kubernetes health series, and direct in-container cgroup snapshots are
  # complete. Preserve those sources for attribution, but do not let missing
  # supporting samples masquerade as an SC health or capacity failure.
  auxiliary_quality_ndjson=${cell_dir}/metrics-auxiliary-quality.ndjson
  : >"$auxiliary_quality_ndjson"
  for spec in container_cpu_cadvisor:pod throttle_ratio:pod cpu_pressure_waiting:pod; do
    metric=${spec%%:*}
    label=${spec#*:}
    [[ -s "${cell_dir}/metrics/${metric}.json" ]] \
      || die "${current_cell}: missing ${metric} telemetry"
    jq -e '.status == "success"' "${cell_dir}/metrics/${metric}.json" >/dev/null \
      || die "${current_cell}: ${metric} telemetry query failed"
    jq -nc --arg metric "$metric" --arg label "$label" \
      --argjson start "$start" --argjson end "$end" \
      --argjson max_gap "$AUX_METRIC_MAX_GAP_SECONDS" \
      --slurpfile targets "$target_file" \
      --slurpfile document "${cell_dir}/metrics/${metric}.json" '
        def window_values:
          [.values[]? | select(.[0] >= $start and .[0] <= $end)];
        def complete($values):
          ($values | length) > 0
          and ($values[0][0] - $start) <= $max_gap
          and ($end - $values[-1][0]) <= $max_gap
          and ([range(1; $values | length) as $i |
                $values[$i][0] - $values[$i - 1][0]] | max // 0) <= $max_gap;
        ($targets[0].items | map(.metadata.name) | sort) as $pods
        | $document[0] as $doc
        | [$pods[] as $pod
           | select(any($doc.data.result[]?;
               .metric[$label] == $pod and complete(window_values)))
           | $pod] as $complete
        | {metric:$metric,authority:"supporting",query_status:$doc.status,
           expected_pods:($pods | length),complete_pods:($complete | length),
           coverage:(if ($pods | length) == 0 then 0
                     else (($complete | length) / ($pods | length)) end),
           complete_pod_names:$complete,
           incomplete_pod_names:[$pods[] as $pod
             | select(($complete | index($pod)) == null) | $pod],
           max_gap_seconds:$max_gap,
           gate:"recorded_not_capacity_gating"}' >>"$auxiliary_quality_ndjson"
  done
  jq -s '{schema_version:1,classification:"supporting_attribution",
          all_complete:all(.[];.complete_pods == .expected_pods),metrics:.}' \
    "$auxiliary_quality_ndjson" >"${cell_dir}/metrics-auxiliary-quality.json"

  metric_values_are "${cell_dir}/metrics/restarts.json" pod "$target_file" \
    "$start" "$end" 0 || die "${current_cell}: restart telemetry is non-zero"
  metric_values_are "${cell_dir}/metrics/pod_ready.json" pod "$target_file" \
    "$start" "$end" 1 || die "${current_cell}: target readiness dropped"

  [[ -s "${cell_dir}/metrics/node_ready.json" ]] \
    || die "${current_cell}: missing node readiness telemetry"
  for node in "$TARGET_NODE" "$DRIVER_NODE"; do
    named_metric_series_complete "${cell_dir}/metrics/node_ready.json" node "$node" \
      "$start" "$end" \
      || die "${current_cell}: incomplete readiness telemetry for node ${node}"
    jq -e --arg node "$node" --argjson start "$start" --argjson end "$end" '
      any(.data.result[]?;
        .metric.node == $node
        and ([.values[]? | select(.[0] >= $start and .[0] <= $end)]) as $values
        | ($values | length) > 0
          and all($values[]; (.[1] | tonumber) == 1))
    ' "${cell_dir}/metrics/node_ready.json" >/dev/null \
      || die "${current_cell}: node ${node} readiness telemetry failed"
  done
}

validate_cell_artifacts() {
  local cell_dir=$1 expected_replicas=$2 expected_concurrency=$3 expected_connections=$4
  local expected_sequence_base=$5 expected_workers=$6 expected_topology
  if [[ "$TARGET_NODE" == "$DRIVER_NODE" ]]; then
    expected_topology="same-node-direct-${TARGET_NODE}"
  else
    expected_topology="cross-node-direct-${TARGET_NODE}-from-${DRIVER_NODE}"
  fi

  for artifact in cell.json drivers.json summary.json targets-before.json \
    targets-after.json deployment-before.json nodes-before.json nodes-after.json \
    health-event-violations.json cgroup-summary.json \
    driver-jobs.json driver-pods.json recovery-anchor.json \
    recovery-timeline.ndjson metrics-summary.json; do
    [[ -s "${cell_dir}/${artifact}" ]] || die "${current_cell}: missing ${artifact}"
  done
  [[ -f "${cell_dir}/target-logs.txt" ]] \
    || die "${current_cell}: missing target-logs.txt"

  jq -e --argjson replicas "$expected_replicas" \
    --argjson concurrency "$expected_concurrency" \
    --argjson connections "$expected_connections" \
    --arg digest "$TARGET_IMAGE" --arg model "$MODEL_SHA256" \
    --arg tokenizer "$TOKENIZER_SHA256" --argjson token_count "$TOKEN_COUNT" \
    --argjson expected_sequence_base "$expected_sequence_base" \
    --argjson max_rows "$MAX_ROWS_PER_ENDPOINT" --arg topology "$expected_topology" \
    --argjson sequence_stride "$sequence_stride" \
    --slurpfile cell "${cell_dir}/cell.json" \
    --slurpfile targets "${cell_dir}/targets-before.json" '
      length == $replicas
      and all(.[];
        .schema_version == 1
        and .probe == "sustained_exact_token_corpus"
        and .generator_scheme == "alpha_bravo_lsb_identity_service_fill_v1"
        and .raw_latency_semantics ==
            "successful RPC RTTs completed within plateau; microseconds; sorted"
        and .concurrency == $concurrency
        and .connections == $connections
        and .warmup_requests == $connections
        and .candidate_rows == $max_rows
        and .target_image == $digest
        and .model_sha256 == $model
        and .tokenizer_sha256 == $tokenizer
        and .token_count_including_specials == $token_count
        and .topology == $topology
        and .start_epoch_ms == $cell[0].start_epoch_ms
        and .duration_seconds == $cell[0].duration_seconds
        and .corpus_mode == "generated"
        and (.selected_rows_blake3 | type) == "string"
        and (.selected_rows_blake3 | length) == 64
        and (.corpus_exhausted | not)
        and .workers_ready_epoch_ms < .start_epoch_ms
        and (.statuses_completed_within_plateau | to_entries |
             all(.key == "OK"))
        and (.drained_after_plateau | to_entries | all(.key == "OK"))
        and (.successful_rtt_raw_us | length) ==
            (.statuses_completed_within_plateau.OK // 0)
        and .successful_rtt_raw_us == (.successful_rtt_raw_us | sort)
        and .last_sequence ==
            (.first_sequence + .warmup_requests + .candidate_rows - 1))
      and ([.[].first_sequence] | unique | length) == $replicas
      and ([.[].first_sequence] | sort) ==
          [range(0; $replicas) |
            $expected_sequence_base + (. * ($max_rows + $connections))]
      and ([.[].last_sequence] | max) < ($expected_sequence_base + $sequence_stride)
      and ([.[].target] | sort) ==
          ([$targets[0].items[].status.podIP + ":50051"] | sort)
      and ([.[].selected_rows_blake3] | unique | length) == $replicas
    ' "${cell_dir}/drivers.json" >/dev/null \
    || die "${current_cell}: driver provenance/raw-result gate failed"

  jq -e --argjson replicas "$expected_replicas" '
      (.items | length) == $replicas
      and all(.items[];
        (.status.failed // 0) == 0
        and (.status.succeeded // 0) == 1
        and any(.status.conditions[]?;
          .type == "Complete" and .status == "True"))
    ' "${cell_dir}/driver-jobs.json" >/dev/null \
    || die "${current_cell}: driver Job completion gate failed"

  jq -e --argjson replicas "$expected_replicas" --arg node "$DRIVER_NODE" \
    --arg digest "${DRIVER_IMAGE##*@}" '
      (.items | length) == $replicas
      and all(.items[];
        .status.phase == "Succeeded"
        and .spec.nodeName == $node
        and ([.status.containerStatuses[]?.restartCount] | add // 0) == 0
        and any(.status.containerStatuses[]?.imageID; endswith($digest)))
    ' "${cell_dir}/driver-pods.json" >/dev/null \
    || die "${current_cell}: driver Pod placement/image gate failed"

  jq -e --argjson replicas "$expected_replicas" \
    --argjson duration "$DURATION_SECONDS" \
    --slurpfile targets "${cell_dir}/targets-before.json" \
    --slurpfile cell "${cell_dir}/cell.json" '
      ($cell[0].start_epoch_ms / 1000 | floor) as $plateau_start
      | ($plateau_start + $duration) as $plateau_end
      |
      length == $replicas
      and ([.[].pod_uid] | sort) ==
          ([$targets[0].items[].metadata.uid] | sort)
      and all(.[];
        .start_epoch_s >= ($plateau_start - 2)
        and .start_epoch_s <= ($plateau_start + 10)
        and .end_epoch_s >= ($plateau_end - 2)
        and .end_epoch_s <= ($plateau_end + 10)
        and .elapsed_seconds >= ($duration - 10)
        and .elapsed_seconds <= ($duration + 10)
        and .usage_usec_delta >= 0
        and .nr_periods_delta >= 0
        and .nr_throttled_delta >= 0
        and .throttled_usec_delta >= 0)
    ' "${cell_dir}/cgroup-summary.json" >/dev/null \
    || die "${current_cell}: cgroup CPU snapshot gate failed"

  jq -e --argjson replicas "$expected_replicas" --arg workers "$expected_workers" \
    --arg topology "$expected_topology" \
    --argjson sequence_base "$expected_sequence_base" \
    --argjson sequence_span "$((MAX_ROWS_PER_ENDPOINT + expected_connections))" '
      .initiated_within_plateau > 0
      and .completed_within_plateau == .ok_completed_within_plateau
      and .initiated_within_plateau ==
          (.completed_within_plateau + .drained_after_plateau)
      and .latency_us.samples == .ok_completed_within_plateau
      and (.corpus_exhausted | not)
      and (.workers_late | not)
      and .health_event_violations == 0
      and (.statuses | to_entries | all(.key == "OK"))
      and (.endpoint_rps | length) == $replicas
      and .cell.inference_workers == $workers
      and .cell.topology == $topology
      and .cell.sequence_base == $sequence_base
      and .cell.sequence_span_per_endpoint == $sequence_span
      and .cell.reserved_sequence_end_exclusive ==
          ($sequence_base + ($replicas * $sequence_span))
    ' "${cell_dir}/summary.json" >/dev/null \
    || die "${current_cell}: request validity gate failed"

  jq -e --slurpfile cell "${cell_dir}/cell.json" '
      .source == "driver_job_argument:--start-epoch-ms"
      and .start_epoch_ms == $cell[0].start_epoch_ms
      and .plateau_end_epoch ==
          (($cell[0].start_epoch_ms / 1000 | floor) + $cell[0].duration_seconds)
    ' "${cell_dir}/recovery-anchor.json" >/dev/null \
    || die "${current_cell}: recovery anchor disagrees with cell runner"

  jq -se --argjson count "$recovery_count" \
    --argjson max_delay "$RECOVERY_MAX_DELAY_SECONDS" \
    --slurpfile cell "${cell_dir}/cell.json" '
      (($cell[0].start_epoch_ms / 1000 | floor) +
       $cell[0].duration_seconds) as $plateau_end
      | length == $count
      and all(.[];
        .anchor == "plateau_end"
        and .plateau_start_epoch_ms == $cell[0].start_epoch_ms
        and .plateau_end_epoch == $plateau_end
        and .target_epoch == ($plateau_end + .requested_seconds)
        and .observation_started_epoch >= .target_epoch
        and .observed_epoch >= .observation_started_epoch
        and .observation_delay_seconds == (.observed_epoch - .target_epoch)
        and .observation_delay_seconds >= 0
        and .observation_delay_seconds <= $max_delay)
    ' "${cell_dir}/recovery-timeline.ndjson" >/dev/null \
    || die "${current_cell}: recovery timing gate failed"

  for checkpoint in $(normalize_list "$RECOVERY_CHECKPOINTS"); do
    for artifact in targets deployment nodes events; do
      [[ -s "${cell_dir}/recovery-${checkpoint}s-${artifact}.json" ]] \
        || die "${current_cell}: missing recovery ${checkpoint}s ${artifact} snapshot"
    done
  done

  jq -e --slurpfile before "${cell_dir}/targets-before.json" '
      ([.items[] | [.metadata.uid, .status.podIP]] | sort) ==
      ([$before[0].items[] | [.metadata.uid, .status.podIP]] | sort)
      and all(.items[];
        .metadata.deletionTimestamp == null
        and .status.phase == "Running"
        and any(.status.conditions[]?; .type == "Ready" and .status == "True")
        and ([.status.containerStatuses[]?.restartCount] | add // 0) == 0)
    ' "${cell_dir}/targets-after.json" >/dev/null \
    || die "${current_cell}: target identity/health changed during plateau"

  validate_metrics "$cell_dir"
  [[ -s "${cell_dir}/metrics-auxiliary-quality.json" ]] \
    || die "${current_cell}: missing metrics-auxiliary-quality.json"
}

matching_recovery_job_count() {
  local targets_file=$1
  jq -r --slurpfile targets "$targets_file" '
    ($targets[0].items | map(.metadata.uid)) as $target_uids
    | [.items[]?
       | select(.metadata.annotations["benchmark.llm-d/target-uid"] as $uid
                | $target_uids | index($uid))]
    | length
  '
}

extract_plateau_start_epoch_ms() {
  local targets_file=$1 expected_duration=$2
  jq -er --arg duration "$expected_duration" --slurpfile targets "$targets_file" '
    def one_arg($name):
      (.spec.template.spec.containers[0].args // []) as $args
      | [range(0; (($args | length) - 1)) as $i
         | select($args[$i] == $name)
         | $args[$i + 1]]
      | if length == 1 then .[0] else null end;
    ($targets[0].items | map(.metadata.uid)) as $target_uids
    | [.items[]?
       | select(.metadata.annotations["benchmark.llm-d/target-uid"] as $uid
                | $target_uids | index($uid))
       | {start:(one_arg("--start-epoch-ms")),
          duration:(one_arg("--duration-seconds"))}] as $jobs
    | if ($jobs | length) == 0 then
        error("no current-target driver Jobs")
      elif (all($jobs[];
              ((.start // "") | test("^(0|[1-9][0-9]*)$"))
              and .duration == $duration
              and ((.start | tonumber) % 1000 == 0)))
           and (($jobs | map(.start) | unique | length) == 1) then
        $jobs[0].start
      else
        error("driver Jobs disagree on a valid start/duration anchor")
      end
  '
}

discover_plateau_start_epoch_ms() {
  local cell_dir=$1 run_id=$2 jobs_json matching_count start_epoch_ms runner_status
  local discovery_errors=${cell_dir}/recovery-anchor-discovery-errors.txt
  : >"$discovery_errors"
  while :; do
    if [[ -s "${cell_dir}/targets-before.json" ]]; then
      if jobs_json=$("${k[@]}" get jobs -n "$NAMESPACE" \
          -l "benchmark.llm-d/run-id=${run_id}" -o json 2>>"$discovery_errors"); then
        matching_count=$(matching_recovery_job_count \
          "${cell_dir}/targets-before.json" <<<"$jobs_json") \
          || die "${current_cell}: could not inspect driver Jobs for recovery anchor"
        if (( matching_count > 0 )); then
          start_epoch_ms=$(extract_plateau_start_epoch_ms \
            "${cell_dir}/targets-before.json" "$DURATION_SECONDS" <<<"$jobs_json") \
            || die "${current_cell}: driver Job recovery anchor is invalid"
          printf '%s\n' "$start_epoch_ms"
          return 0
        fi
      fi
    fi
    if [[ -s "${cell_dir}/cell-runner-exit-status.txt" ]]; then
      runner_status=$(sed -n '1p' "${cell_dir}/cell-runner-exit-status.txt")
      die "${current_cell}: cell runner exited with status ${runner_status} before publishing a recovery anchor"
    fi
    sleep 1
  done
}

sleep_until_epoch() {
  local target_epoch=$1 now remaining
  # Bash 3.2 defers a trapped signal while waiting for one long external sleep.
  # One-second increments bound recovery-collector cancellation latency without
  # changing the integer-second checkpoint clock.
  while :; do
    now=$(date -u +%s)
    remaining=$((target_epoch - now))
    (( remaining > 0 )) || return 0
    sleep 1
  done
}

capture_recovery() {
  local cell_dir=$1 replicas=$2 workers=$3 run_id=$4
  local checkpoint target_epoch now delta observation_started_epoch actual_epoch
  local previous=-1 plateau_start_epoch_ms plateau_start_epoch origin_epoch
  local recovery_prefix
  plateau_start_epoch_ms=$(discover_plateau_start_epoch_ms "$cell_dir" "$run_id")
  assert_uint RECOVERY_START_EPOCH_MS "$plateau_start_epoch_ms"
  plateau_start_epoch=$((plateau_start_epoch_ms / 1000))
  origin_epoch=$((plateau_start_epoch + DURATION_SECONDS))
  [[ -s "${cell_dir}/events-before.json" ]] \
    || die "${current_cell}: missing pre-cell Events baseline for recovery"

  jq -n --arg source "driver_job_argument:--start-epoch-ms" \
    --argjson start_epoch_ms "$plateau_start_epoch_ms" \
    --argjson plateau_end_epoch "$origin_epoch" \
    '{schema_version:1,source:$source,start_epoch_ms:$start_epoch_ms,
      plateau_end_epoch:$plateau_end_epoch}' >"${cell_dir}/recovery-anchor.json"
  : >"${cell_dir}/recovery-timeline.ndjson"
  for checkpoint in $(normalize_list "$RECOVERY_CHECKPOINTS"); do
    assert_uint RECOVERY_CHECKPOINT "$checkpoint"
    (( checkpoint > previous )) || die "RECOVERY_CHECKPOINTS must strictly increase"
    target_epoch=$((origin_epoch + checkpoint))
    now=$(date -u +%s)
    delta=$((target_epoch - now))
    if (( delta > 0 )); then
      sleep_until_epoch "$target_epoch"
    elif (( -delta > RECOVERY_MAX_DELAY_SECONDS )); then
      die "${current_cell}: recovery ${checkpoint}s anchor was discovered too late"
    fi

    observation_started_epoch=$(date -u +%s)
    recovery_prefix=${cell_dir}/recovery-${checkpoint}s
    assert_target_health "$replicas" "$workers" "$recovery_prefix"
    "${k[@]}" get events -n "$NAMESPACE" -o json \
      >"${recovery_prefix}-events.json"
    jq -e --slurpfile before "${cell_dir}/targets-before.json" '
      ([.items[].metadata.uid] | sort) ==
      ([$before[0].items[].metadata.uid] | sort)
    ' "${recovery_prefix}-targets.json" >/dev/null \
      || die "${current_cell}: target identity changed during recovery"
    # events-after.json is intentionally not the baseline here: the cell runner
    # writes it during post-processing and it can be newer than the 5s sample.
    # The pre-cell baseline keeps every plateau/recovery Warning delta visible;
    # the cell validity gate already rejects those same plateau deltas.
    jq -e --slurpfile before "${cell_dir}/events-before.json" \
      --slurpfile targets "${cell_dir}/targets-before.json" '
        ($targets[0].items | map(.metadata.uid)) as $target_uids
        | ($before[0].items |
           map({key:.metadata.uid,value:(.count // 1)}) | from_entries) as $before_counts
        | [ .items[]?
            | select(.involvedObject.uid as $uid | $target_uids | index($uid))
            | (.count // 1) as $after_count
            | ($before_counts[.metadata.uid] // 0) as $before_count
            | select($after_count > $before_count)
            | select(.type == "Warning" or .reason == "Unhealthy") ]
        | length == 0
      ' "${recovery_prefix}-events.json" >/dev/null \
      || die "${current_cell}: target Warning/Unhealthy event during recovery"
    actual_epoch=$(date -u +%s)
    jq -nc --argjson plateau_start_epoch_ms "$plateau_start_epoch_ms" \
      --argjson plateau_end_epoch "$origin_epoch" \
      --argjson requested_seconds "$checkpoint" \
      --argjson target_epoch "$target_epoch" \
      --argjson observation_started_epoch "$observation_started_epoch" \
      --argjson observed_epoch "$actual_epoch" \
      '{anchor:"plateau_end",plateau_start_epoch_ms:$plateau_start_epoch_ms,
        plateau_end_epoch:$plateau_end_epoch,requested_seconds:$requested_seconds,
        target_epoch:$target_epoch,observation_started_epoch:$observation_started_epoch,
        observed_epoch:$observed_epoch,observation_delay_seconds:
          ($observed_epoch - $target_epoch)}' >>"${cell_dir}/recovery-timeline.ndjson"
    (( observation_started_epoch - target_epoch >= 0 \
       && actual_epoch - target_epoch <= RECOVERY_MAX_DELAY_SECONDS )) \
      || die "${current_cell}: recovery ${checkpoint}s observation was outside its timing budget"
    previous=$checkpoint
  done
}

restore_original_state() {
  "${k[@]}" scale deployment/"$DEPLOYMENT" -n "$NAMESPACE" --replicas=0 >/dev/null
  wait_for_target_deletion
  restore_worker_setting
  if (( original_replicas > 0 )); then
    scale_in_steps "$original_replicas"
  fi
}

cleanup() {
  local exit_code=$? restore_code=0 final_status=aborted cleanup_error=""
  local driver_cleanup_failed=0 state_cleanup_failed=0
  trap - EXIT
  trap '' INT TERM
  set +e

  cancel_active_recovery
  if (( exit_code != 0 )); then
    if ! delete_active_driver_jobs; then
      driver_cleanup_failed=1
      cleanup_error="failed to delete active driver Jobs for ${active_driver_run_id}"
      append_error "$cleanup_error"
    fi
  fi
  if (( cluster_mutated == 1 )); then
    if (( exit_code == 0 && matrix_complete == 1 && RESTORE_ORIGINAL_ON_SUCCESS == 1 )); then
      (set -e; restore_original_state)
      restore_code=$?
      if (( restore_code != 0 )); then
        state_cleanup_failed=1
        exit_code=$restore_code
        last_error="failed to restore original target deployment state"
        "${k[@]}" scale deployment/"$DEPLOYMENT" -n "$NAMESPACE" --replicas=0 >/dev/null
      fi
    elif (( exit_code != 0 )); then
      if ! "${k[@]}" scale deployment/"$DEPLOYMENT" -n "$NAMESPACE" \
        --replicas=0 >/dev/null; then
        state_cleanup_failed=1
      fi
      (wait_for_target_deletion) || state_cleanup_failed=1
      restore_worker_setting || state_cleanup_failed=1
    fi
  fi

  if (( state_cleanup_failed == 1 )); then
    append_error "failed to prove target cleanup/restoration"
  fi

  if (( lock_acquired == 1 && driver_cleanup_failed == 0 && state_cleanup_failed == 0 )); then
    if ! "${k[@]}" delete configmap "$LOCK_NAME" -n "$NAMESPACE" \
      --wait=true --timeout=60s >/dev/null; then
      (( exit_code == 0 )) && exit_code=1
      append_error "failed to delete benchmark lock ${NAMESPACE}/${LOCK_NAME}"
    fi
  elif (( lock_acquired == 1 )); then
    append_error "benchmark lock retained for operator intervention"
  fi

  if (( exit_code == 0 && matrix_complete == 1 )); then
    final_status=completed
  elif (( exit_code == 0 && plan_only_complete == 1 )); then
    final_status=planned
  fi
  if (( matrix_dir_owned == 1 )) && [[ -d "$MATRIX_DIR" ]]; then
    if [[ -n "$last_error" ]]; then
      printf '%s\n' "$last_error" >"${MATRIX_DIR}/matrix-error.txt"
    fi
    jq -n --arg run_id "$MATRIX_RUN_ID" --arg cell "$current_cell" \
      --argjson exit_code "$exit_code" --arg completed_at "$(date -u +%FT%TZ)" \
      --arg status "$final_status" --arg error "$last_error" \
      '{schema_version:1,run_id:$run_id,status:$status,exit_code:$exit_code,
        last_cell:$cell,completed_at:$completed_at,
        error:(if $error == "" then null else $error end)}' >"${MATRIX_DIR}/matrix-status.json"
  fi
  exit "$exit_code"
}

# Used by fixture/unit checks to load the gate functions without planning or
# touching a cluster. Normal executions leave this unset.
if [[ ${MATRIX_LIBRARY_ONLY:-0} == 1 ]]; then
  return 0 2>/dev/null || exit 0
fi

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for command in oc jq curl cksum sort awk sed git seq tail wc uniq date mkdir; do
  require_command "$command"
done
[[ -x "$CELL_RUNNER" ]] || die "cell runner is not executable: ${CELL_RUNNER}"
[[ -x "$METRICS_RUNNER" ]] || die "metrics runner is not executable: ${METRICS_RUNNER}"
[[ "$MATRIX_RUN_ID" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] \
  || die "MATRIX_RUN_ID must be a DNS-safe lowercase label"
(( ${#MATRIX_RUN_ID} <= 28 )) || die "MATRIX_RUN_ID must be at most 28 characters"
[[ "$DRIVER_IMAGE" =~ @sha256:[0-9a-f]{64}$ ]] \
  || die "DRIVER_IMAGE must be an immutable image reference ending in @sha256:<64 hex>"
[[ "$TARGET_IMAGE" =~ ^sha256:[0-9a-f]{64}$ ]] \
  || die "TARGET_IMAGE must be sha256:<64 hex>"
[[ "$MODEL_SHA256" =~ ^[0-9a-f]{64}$ ]] || die "MODEL_SHA256 must be 64 lowercase hex"
[[ "$TOKENIZER_SHA256" =~ ^[0-9a-f]{64}$ ]] \
  || die "TOKENIZER_SHA256 must be 64 lowercase hex"
[[ "$TARGET_CONTAINER" == llm-d-sc ]] \
  || die "TARGET_CONTAINER must remain llm-d-sc; the cell runner contract is fixed"
[[ "$WORKER_ENV_NAME" == LLM_D_SC_INFERENCE_WORKERS ]] \
  || die "WORKER_ENV_NAME must remain LLM_D_SC_INFERENCE_WORKERS"
[[ "$METRIC_STEP_SECONDS" == 5 ]] \
  || die "METRIC_STEP_SECONDS must remain 5; the telemetry collector uses a fixed 5-second query step"
for node in "$TARGET_NODE" "$DRIVER_NODE"; do
  [[ "$node" == gnr2.fm2aihpcsed.com || "$node" == rhgnr1 ]] \
    || die "node ${node} is outside the Arena telemetry contract"
done

for pair in \
  "SEQUENCE_BASE:$SEQUENCE_BASE" "MATRIX_SEED:$MATRIX_SEED" "REPEATS:$REPEATS" \
  "DURATION_SECONDS:$DURATION_SECONDS" "START_DELAY_SECONDS:$START_DELAY_SECONDS" \
  "TOKEN_COUNT:$TOKEN_COUNT" "MAX_ROWS_PER_ENDPOINT:$MAX_ROWS_PER_ENDPOINT" \
  "MAX_SCALE_STEP:$MAX_SCALE_STEP" "SCALE_SETTLE_SECONDS:$SCALE_SETTLE_SECONDS" \
  "QUIESCENCE_SECONDS:$QUIESCENCE_SECONDS" \
  "ROLLOUT_TIMEOUT_SECONDS:$ROLLOUT_TIMEOUT_SECONDS" \
  "RECOVERY_MAX_DELAY_SECONDS:$RECOVERY_MAX_DELAY_SECONDS"; do
  assert_uint "${pair%%:*}" "${pair#*:}"
done
assert_positive REPEATS "$REPEATS"
assert_positive DURATION_SECONDS "$DURATION_SECONDS"
assert_positive MAX_ROWS_PER_ENDPOINT "$MAX_ROWS_PER_ENDPOINT"
assert_positive MAX_SCALE_STEP "$MAX_SCALE_STEP"
(( TOKEN_COUNT >= 3 )) || die "TOKEN_COUNT must be at least 3 for generated mode"
identity_bits=$((TOKEN_COUNT - 2))
if (( identity_bits >= 63 )); then
  generator_capacity=9223372036854775808
  generator_capacity_limit=9223372036854775807
else
  generator_capacity=$((1 << identity_bits))
  generator_capacity_limit=$generator_capacity
fi
assert_positive ROLLOUT_TIMEOUT_SECONDS "$ROLLOUT_TIMEOUT_SECONDS"
assert_positive RECOVERY_MAX_DELAY_SECONDS "$RECOVERY_MAX_DELAY_SECONDS"
assert_positive METRIC_STEP_SECONDS "$METRIC_STEP_SECONDS"
assert_positive METRIC_MAX_GAP_SECONDS "$METRIC_MAX_GAP_SECONDS"
assert_positive AUX_METRIC_MAX_GAP_SECONDS "$AUX_METRIC_MAX_GAP_SECONDS"
for pair in "RUN_BASELINE:$RUN_BASELINE" \
  "DELETE_COMPLETED_JOBS:$DELETE_COMPLETED_JOBS" \
  "RESTORE_ORIGINAL_ON_SUCCESS:$RESTORE_ORIGINAL_ON_SUCCESS" \
  "PLAN_ONLY:$PLAN_ONLY"; do
  [[ "${pair#*:}" == 0 || "${pair#*:}" == 1 ]] \
    || die "${pair%%:*} must be 0 or 1"
done

recovery_previous=-1
recovery_count=0
for checkpoint in $(normalize_list "$RECOVERY_CHECKPOINTS"); do
  assert_positive RECOVERY_CHECKPOINT "$checkpoint"
  (( checkpoint > recovery_previous )) \
    || die "RECOVERY_CHECKPOINTS must strictly increase"
  recovery_previous=$checkpoint
  recovery_count=$((recovery_count + 1))
done
(( recovery_count > 0 )) || die "RECOVERY_CHECKPOINTS must not be empty"

[[ ! -e "$MATRIX_DIR" ]] \
  || die "MATRIX_DIR already exists; choose a new MATRIX_RUN_ID: ${MATRIX_DIR}"
mkdir -p "$MATRIX_DIR" "$CELL_RESULT_ROOT"
matrix_dir_owned=1
catalog=${MATRIX_DIR}/cell-catalog.tsv
schedule=${MATRIX_DIR}/matrix-plan.tsv
printf 'phase\tworkers\treplicas\tconcurrency\tconnections\n' >"$catalog"

phase_count=0
for phase in $(normalize_list "$MATRIX_PHASES"); do
  case "$phase" in
    worker)
      phase_count=$((phase_count + 1))
      assert_positive WORKER_REPLICAS "$WORKER_REPLICAS"
      for workers in $(normalize_list "$WORKER_WIDTHS"); do
        assert_positive WORKER_WIDTH "$workers"
        for concurrency in $(normalize_list "$WORKER_CONCURRENCIES"); do
          assert_positive WORKER_CONCURRENCY "$concurrency"
          printf 'worker\t%s\t%s\t%s\t%s\n' "$workers" "$WORKER_REPLICAS" \
            "$concurrency" "$concurrency" >>"$catalog"
        done
      done
      ;;
    horizontal)
      phase_count=$((phase_count + 1))
      assert_positive HORIZONTAL_WORKERS "$HORIZONTAL_WORKERS"
      assert_positive HORIZONTAL_CONCURRENCY "$HORIZONTAL_CONCURRENCY"
      assert_positive HORIZONTAL_CONNECTIONS "$HORIZONTAL_CONNECTIONS"
      for replicas in $(normalize_list "$HORIZONTAL_REPLICAS"); do
        assert_positive HORIZONTAL_REPLICA "$replicas"
        printf 'horizontal\t%s\t%s\t%s\t%s\n' "$HORIZONTAL_WORKERS" "$replicas" \
          "$HORIZONTAL_CONCURRENCY" "$HORIZONTAL_CONNECTIONS" >>"$catalog"
      done
      ;;
    *) die "unknown MATRIX_PHASES entry: ${phase}" ;;
  esac
done
(( phase_count > 0 )) || die "MATRIX_PHASES did not select any cells"

if (( RUN_BASELINE == 1 )); then
  assert_positive BASELINE_WORKERS "$BASELINE_WORKERS"
  assert_positive BASELINE_REPLICAS "$BASELINE_REPLICAS"
  assert_positive BASELINE_CONCURRENCY "$BASELINE_CONCURRENCY"
  assert_positive BASELINE_CONNECTIONS "$BASELINE_CONNECTIONS"
fi

catalog_cells=$(($(wc -l <"$catalog") - 1))
assert_positive CATALOG_CELLS "$catalog_cells"
planned_slots=$((catalog_cells * REPEATS + RUN_BASELINE))
max_replicas=$(awk -F '\t' 'NR > 1 {if ($3 > max) max=$3} END {print max+0}' "$catalog")
max_connections=$(awk -F '\t' 'NR > 1 {if ($5 > max) max=$5} END {print max+0}' "$catalog")
if (( RUN_BASELINE == 1 && BASELINE_REPLICAS > max_replicas )); then
  max_replicas=$BASELINE_REPLICAS
fi
if (( RUN_BASELINE == 1 && BASELINE_CONNECTIONS > max_connections )); then
  max_connections=$BASELINE_CONNECTIONS
fi
endpoint_sequence_span=$((MAX_ROWS_PER_ENDPOINT + max_connections))
sequence_stride=$((max_replicas * endpoint_sequence_span))
SEQUENCE_BASE_B=${SEQUENCE_BASE_B:-$((SEQUENCE_BASE + (planned_slots + 1) * sequence_stride))}
assert_uint SEQUENCE_BASE_B "$SEQUENCE_BASE_B"
reserved_shard_span=$((planned_slots * sequence_stride))
(( reserved_shard_span > 0 )) || die "generated sequence reservation overflowed"
shard_a_reserved_end=$((SEQUENCE_BASE + reserved_shard_span))
shard_b_reserved_end=$((SEQUENCE_BASE_B + reserved_shard_span))
(( shard_a_reserved_end >= SEQUENCE_BASE && shard_a_reserved_end <= generator_capacity_limit )) \
  || die "sequence shard A exceeds generated exact-token capacity"
(( shard_b_reserved_end >= SEQUENCE_BASE_B && shard_b_reserved_end <= generator_capacity_limit )) \
  || die "sequence shard B exceeds generated exact-token capacity"
if ! (( SEQUENCE_BASE_B >= shard_a_reserved_end \
        || SEQUENCE_BASE >= shard_b_reserved_end )); then
  die "SEQUENCE_BASE and SEQUENCE_BASE_B do not reserve disjoint shard ranges"
fi

printf 'order\tphase\trepetition\tworkers\treplicas\tconcurrency\tconnections\tshard\tsequence_base\trun_id\n' >"$schedule"
order=0
slot_a=0
slot_b=0

append_schedule_cell() {
  local phase=$1 repetition=$2 workers=$3 replicas=$4 concurrency=$5 connections=$6
  local shard base slot sequence cell_id
  order=$((order + 1))
  if (( repetition == 0 || repetition % 2 == 1 )); then
    shard=A
    base=$SEQUENCE_BASE
    slot=$slot_a
    slot_a=$((slot_a + 1))
  else
    shard=B
    base=$SEQUENCE_BASE_B
    slot=$slot_b
    slot_b=$((slot_b + 1))
  fi
  sequence=$((base + slot * sequence_stride))
  cell_id=$(printf 'm-%s-o%04d-w%s-r%s-c%s' "$MATRIX_RUN_ID" "$order" \
    "$workers" "$replicas" "$concurrency")
  (( ${#cell_id} <= 54 )) || die "generated cell RUN_ID is too long: ${cell_id}"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$order" "$phase" "$repetition" "$workers" "$replicas" "$concurrency" \
    "$connections" "$shard" "$sequence" "$cell_id" >>"$schedule"
}

if (( RUN_BASELINE == 1 )); then
  append_schedule_cell baseline 0 "$BASELINE_WORKERS" "$BASELINE_REPLICAS" \
    "$BASELINE_CONCURRENCY" "$BASELINE_CONNECTIONS"
fi

for repetition in $(seq 1 "$REPEATS"); do
  for phase in $(normalize_list "$MATRIX_PHASES"); do
    phase_file=${MATRIX_DIR}/.phase-${phase}-${repetition}.tsv
    keyed_file=${phase_file}.keyed
    awk -F '\t' -v phase="$phase" 'NR > 1 && $1 == phase {print}' "$catalog" >"$phase_file"
    : >"$keyed_file"
    while IFS=$'\t' read -r row_phase workers replicas concurrency connections; do
      key=$(printf '%s' "${MATRIX_SEED}:${phase}:${repetition}:${workers}:${replicas}:${concurrency}:${connections}" \
        | cksum | awk '{print $1}')
      printf '%010u\t%s\t%s\t%s\t%s\t%s\n' "$key" "$row_phase" "$workers" \
        "$replicas" "$concurrency" "$connections" >>"$keyed_file"
    done <"$phase_file"
    LC_ALL=C sort -k1,1n -k2,6 "$keyed_file" >"${keyed_file}.sorted"
    while IFS=$'\t' read -r _ row_phase workers replicas concurrency connections; do
      append_schedule_cell "$row_phase" "$repetition" "$workers" "$replicas" \
        "$concurrency" "$connections"
    done <"${keyed_file}.sorted"
  done
done

[[ $(($(wc -l <"$schedule") - 1)) == "$planned_slots" ]] \
  || die "internal error: schedule cell count mismatch"
duplicate_sequence_bases=$(awk -F '\t' 'NR > 1 {print $9}' "$schedule" \
  | sort -n | uniq -d | wc -l | awk '{print $1}')
duplicate_run_ids=$(awk -F '\t' 'NR > 1 {print $10}' "$schedule" \
  | sort | uniq -d | wc -l | awk '{print $1}')
[[ "$duplicate_sequence_bases" == 0 ]] \
  || die "internal error: overlapping sequence bases"
[[ "$duplicate_run_ids" == 0 ]] \
  || die "internal error: duplicate cell run IDs"

if (( PLAN_ONLY == 1 )); then
  current_cell=plan-only
  plan_only_complete=1
  cat "$schedule"
  exit 0
fi

require_access get "deployment/${DEPLOYMENT}" "$NAMESPACE"
require_access get deployments.apps "$NAMESPACE"
require_access watch deployments.apps "$NAMESPACE"
require_access patch deployments.apps "$NAMESPACE"
require_access get deployments.apps/scale "$NAMESPACE"
require_access update deployments.apps/scale "$NAMESPACE"
for verb in get list watch; do
  require_access "$verb" pods "$NAMESPACE"
  require_access "$verb" jobs.batch "$NAMESPACE"
  require_access "$verb" replicasets.apps "$NAMESPACE"
done
require_access get pods/log "$NAMESPACE"
require_access create pods/exec "$NAMESPACE"
require_access delete pods "$NAMESPACE"
require_access create jobs.batch "$NAMESPACE"
require_access delete jobs.batch "$NAMESPACE"
require_access get events "$NAMESPACE"
require_access list events "$NAMESPACE"
require_access get endpointslices.discovery.k8s.io "$NAMESPACE"
require_access list endpointslices.discovery.k8s.io "$NAMESPACE"
require_access create configmaps "$NAMESPACE"
require_access delete configmaps "$NAMESPACE"
require_access get configmaps "$NAMESPACE"
require_access get nodes
require_access list nodes
require_access watch nodes
require_access get routes.route.openshift.io openshift-monitoring
require_access get daemonsets.apps "$NAMESPACE"
node_is_ready "$TARGET_NODE" || die "target node is not Ready at preflight"
node_is_ready "$DRIVER_NODE" || die "driver node is not Ready at preflight"
telemetry_preflight

if ! "${k[@]}" create configmap "$LOCK_NAME" -n "$NAMESPACE" \
  --from-literal="run-id=${MATRIX_RUN_ID}" \
  --from-literal="created-at=$(date -u +%FT%TZ)" >/dev/null; then
  "${k[@]}" get configmap "$LOCK_NAME" -n "$NAMESPACE" -o yaml >&2 || true
  die "another benchmark matrix owns ${NAMESPACE}/${LOCK_NAME}"
fi
lock_acquired=1

"${k[@]}" get deployment/"$DEPLOYMENT" -n "$NAMESPACE" -o json \
  >"${MATRIX_DIR}/deployment-original.json"
"${k[@]}" get nodes "$TARGET_NODE" "$DRIVER_NODE" -o json \
  >"${MATRIX_DIR}/nodes-original.json"
"${k[@]}" version -o json >"${MATRIX_DIR}/oc-version.json"
"${k[@]}" whoami >"${MATRIX_DIR}/cluster-identity.txt"
"${k[@]}" get endpointslices.discovery.k8s.io -n "$NAMESPACE" \
  -l "kubernetes.io/service-name=${SERVICE_NAME}" -o json \
  >"${MATRIX_DIR}/endpointslices-original.json"
git -C "$REPO_ROOT" rev-parse HEAD >"${MATRIX_DIR}/git-head.txt"
git -C "$REPO_ROOT" status --porcelain=v1 >"${MATRIX_DIR}/git-status.txt"
cksum "$0" "$CELL_RUNNER" "$METRICS_RUNNER" >"${MATRIX_DIR}/harness-cksum.txt"

original_replicas=$(jq -r '.spec.replicas // 0' "${MATRIX_DIR}/deployment-original.json")
jq -e --arg container "$TARGET_CONTAINER" '
  (.spec.template.spec.containers | length) == 1
  and .spec.template.spec.containers[0].name == $container
' "${MATRIX_DIR}/deployment-original.json" >/dev/null \
  || die "the current cell contract requires exactly one ${TARGET_CONTAINER} container"
original_worker_entry=$(jq -c --arg container "$TARGET_CONTAINER" --arg env "$WORKER_ENV_NAME" '
  [.spec.template.spec.containers[]? | select(.name == $container) | .env[]? |
   select(.name == $env)][0] // null' "${MATRIX_DIR}/deployment-original.json")
if [[ "$original_worker_entry" != null ]]; then
  original_workers_present=1
  jq -e 'has("value") and (has("valueFrom") | not)' <<<"$original_worker_entry" >/dev/null \
    || die "cannot safely restore valueFrom-based ${WORKER_ENV_NAME}"
  original_workers=$(jq -r '.value' <<<"$original_worker_entry")
fi

jq -n \
  --arg run_id "$MATRIX_RUN_ID" --arg created_at "$(date -u +%FT%TZ)" \
  --arg namespace "$NAMESPACE" --arg deployment "$DEPLOYMENT" \
  --arg otel_daemonset "$OTEL_DAEMONSET" \
  --arg target_node "$TARGET_NODE" --arg driver_node "$DRIVER_NODE" \
  --arg target_image "$TARGET_IMAGE" --arg driver_image "$DRIVER_IMAGE" \
  --arg model_sha256 "$MODEL_SHA256" --arg tokenizer_sha256 "$TOKENIZER_SHA256" \
  --arg matrix_seed "$MATRIX_SEED" --arg phases "$MATRIX_PHASES" \
  --arg recovery_checkpoints "$RECOVERY_CHECKPOINTS" \
  --argjson repeats "$REPEATS" --argjson duration_seconds "$DURATION_SECONDS" \
  --argjson start_delay_seconds "$START_DELAY_SECONDS" \
  --argjson token_count "$TOKEN_COUNT" --argjson sequence_base_a "$SEQUENCE_BASE" \
  --argjson sequence_base_b "$SEQUENCE_BASE_B" --argjson sequence_stride "$sequence_stride" \
  --argjson endpoint_sequence_span "$endpoint_sequence_span" \
  --arg generator_capacity "$generator_capacity" \
  --argjson max_rows_per_endpoint "$MAX_ROWS_PER_ENDPOINT" \
  --argjson metric_max_gap_seconds "$METRIC_MAX_GAP_SECONDS" \
  --argjson aux_metric_max_gap_seconds "$AUX_METRIC_MAX_GAP_SECONDS" \
  --argjson recovery_max_delay_seconds "$RECOVERY_MAX_DELAY_SECONDS" \
  --argjson original_replicas "$original_replicas" \
  --arg original_workers "$original_workers" \
  '{schema_version:1,run_id:$run_id,created_at:$created_at,namespace:$namespace,
    deployment:$deployment,target_node:$target_node,driver_node:$driver_node,
    topology:(if $target_node == $driver_node
              then ("same-node-direct-" + $target_node)
              else ("cross-node-direct-" + $target_node + "-from-" + $driver_node) end),
    target_image:$target_image,driver_image:$driver_image,model_sha256:$model_sha256,
    tokenizer_sha256:$tokenizer_sha256,matrix_seed:$matrix_seed,phases:$phases,
    repeats:$repeats,duration_seconds:$duration_seconds,
    start_delay_seconds:$start_delay_seconds,token_count:$token_count,
    sequence:{shard_a:$sequence_base_a,shard_b:$sequence_base_b,stride:$sequence_stride,
      endpoint_span:$endpoint_sequence_span,
      max_rows_per_endpoint:$max_rows_per_endpoint,
      generated_capacity_exclusive:$generator_capacity},
    telemetry:{otel_daemonset:$otel_daemonset,query_step_seconds:5,
      authoritative:["pod_cpu_otel","container_cpu_otel","memory_working_set",
        "restarts","pod_ready","node_ready","direct_cgroup"],
      supporting:["container_cpu_cadvisor","throttle_ratio","cpu_pressure_waiting"],
      health_max_gap_seconds:$metric_max_gap_seconds,
      auxiliary_max_gap_seconds:$aux_metric_max_gap_seconds},
    recovery:{anchor:"plateau_end",checkpoints_seconds:$recovery_checkpoints,
      max_observation_delay_seconds:$recovery_max_delay_seconds},
    original_state:{replicas:$original_replicas,workers:$original_workers}}' \
  >"${MATRIX_DIR}/matrix-provenance.json"

execution_plan=${MATRIX_DIR}/.execution-plan.tsv
tail -n +2 "$schedule" >"$execution_plan"
while IFS=$'\t' read -r order phase repetition workers replicas \
  concurrency connections shard sequence cell_id; do
  current_cell=$cell_id
  cell_dir=${CELL_RESULT_ROOT}/${cell_id}
  mkdir -p "$cell_dir"

  if [[ "$TARGET_NODE" == "$DRIVER_NODE" ]]; then
    cell_topology="same-node-direct-${TARGET_NODE}"
  else
    cell_topology="cross-node-direct-${TARGET_NODE}-from-${DRIVER_NODE}"
  fi
  jq -n --arg run_id "$cell_id" --arg matrix_run_id "$MATRIX_RUN_ID" \
    --arg phase "$phase" --arg shard "$shard" --arg topology "$cell_topology" \
    --argjson order "$order" --argjson repetition "$repetition" \
    --argjson workers "$workers" --argjson replicas "$replicas" \
    --argjson concurrency "$concurrency" --argjson connections "$connections" \
    --argjson sequence_base "$sequence" \
    '{schema_version:1,run_id:$run_id,matrix_run_id:$matrix_run_id,phase:$phase,
      order:$order,repetition:$repetition,worker_width:$workers,
      inference_workers:($workers | tostring),replicas:$replicas,
      concurrency:$concurrency,connections:$connections,shard:$shard,
      sequence_base:$sequence_base,topology:$topology}' >"${cell_dir}/matrix-cell.json"

  fresh_targets "$workers" "$replicas" "$cell_dir"
  "${k[@]}" get endpointslices.discovery.k8s.io -n "$NAMESPACE" \
    -l "kubernetes.io/service-name=${SERVICE_NAME}" -o json \
    >"${cell_dir}/endpointslices-before.json"

  active_driver_run_id=$cell_id
  # Start the recovery clock before the runner publishes its first driver Job.
  # Keep the runner in the foreground so INT/TERM behavior remains identical to
  # the pre-concurrency harness while recovery samples run independently of its
  # r10+ sequential log and API post-processing.
  (
    trap - EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    capture_recovery "$cell_dir" "$replicas" "$workers" "$cell_id"
  ) 2>"${cell_dir}/recovery-capture-error.txt" &
  active_recovery_pid=$!

  cell_runner_status=0
  if env KUBECONFIG_PATH="$KUBECONFIG_PATH" NAMESPACE="$NAMESPACE" \
    DEPLOYMENT="$DEPLOYMENT" TARGET_SELECTOR="$TARGET_SELECTOR" \
    TARGET_NODE="$TARGET_NODE" DRIVER_NODE="$DRIVER_NODE" REPLICAS="$replicas" \
    CONCURRENCY="$concurrency" CONNECTIONS="$connections" \
    DURATION_SECONDS="$DURATION_SECONDS" START_DELAY_SECONDS="$START_DELAY_SECONDS" \
    MAX_ROWS_PER_ENDPOINT="$MAX_ROWS_PER_ENDPOINT" SEQUENCE_BASE="$sequence" \
    RUN_ID="$cell_id" DRIVER_IMAGE="$DRIVER_IMAGE" TARGET_IMAGE="$TARGET_IMAGE" \
    MODEL_SHA256="$MODEL_SHA256" TOKENIZER_SHA256="$TOKENIZER_SHA256" \
    TOKEN_COUNT="$TOKEN_COUNT" RESULT_ROOT="$CELL_RESULT_ROOT" RESET_TARGETS=false \
    "$CELL_RUNNER"; then
    :
  else
    cell_runner_status=$?
  fi
  printf '%s\n' "$cell_runner_status" >"${cell_dir}/cell-runner-exit-status.txt"
  if (( cell_runner_status != 0 )); then
    cancel_active_recovery
    die "${cell_id}: cell runner failed"
  fi

  "${k[@]}" get jobs -n "$NAMESPACE" \
    -l "benchmark.llm-d/run-id=${cell_id}" -o json >"${cell_dir}/driver-jobs.json"
  "${k[@]}" get pods -n "$NAMESPACE" \
    -l "benchmark.llm-d/run-id=${cell_id}" -o json >"${cell_dir}/driver-pods.json"
  recovery_status=0
  if wait "$active_recovery_pid"; then
    :
  else
    recovery_status=$?
  fi
  active_recovery_pid=""
  if (( recovery_status != 0 )); then
    recovery_detail=$(tail -n 1 "${cell_dir}/recovery-capture-error.txt" 2>/dev/null || true)
    die "${cell_id}: concurrent recovery capture failed${recovery_detail:+: ${recovery_detail}}"
  fi
  if ! KUBECONFIG_PATH="$KUBECONFIG_PATH" "$METRICS_RUNNER" "$cell_dir"; then
    die "${cell_id}: telemetry capture failed"
  fi
  "${k[@]}" get endpointslices.discovery.k8s.io -n "$NAMESPACE" \
    -l "kubernetes.io/service-name=${SERVICE_NAME}" -o json \
    >"${cell_dir}/endpointslices-after.json"

  validate_cell_artifacts "$cell_dir" "$replicas" "$concurrency" "$connections" \
    "$sequence" "$workers"

  jq -nc --slurpfile expected "${cell_dir}/matrix-cell.json" \
    --slurpfile summary "${cell_dir}/summary.json" \
    --slurpfile metrics "${cell_dir}/metrics-summary.json" \
    --slurpfile auxiliary "${cell_dir}/metrics-auxiliary-quality.json" \
    '{expected:$expected[0],summary:$summary[0],metrics:$metrics[0],
      auxiliary_metrics:$auxiliary[0],valid:true}' \
    >>"${MATRIX_DIR}/matrix-results.ndjson"

  if (( DELETE_COMPLETED_JOBS == 1 )); then
    delete_active_driver_jobs \
      || die "${cell_id}: failed to delete completed driver Jobs"
  else
    active_driver_run_id=""
  fi
done <"$execution_plan"

completed_cells=$(wc -l <"${MATRIX_DIR}/matrix-results.ndjson" | awk '{print $1}')
(( completed_cells == planned_slots )) \
  || die "only ${completed_cells}/${planned_slots} matrix cells completed"
current_cell=complete
matrix_complete=1

jq -s '{schema_version:1,cells:.,all_valid:all(.[];.valid)}' \
  "${MATRIX_DIR}/matrix-results.ndjson" >"${MATRIX_DIR}/matrix-results.json"
jq '{cells:(.cells | length),all_valid}' "${MATRIX_DIR}/matrix-results.json"
