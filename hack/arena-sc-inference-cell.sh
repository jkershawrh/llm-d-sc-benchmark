#!/usr/bin/env bash
set -euo pipefail

# Run one synchronized, fixed-duration, direct-endpoint SC inference cell.
# The target Deployment must already use the desired image, worker width,
# resources, and placement. This script scales it, verifies provenance/health,
# gives every endpoint a disjoint generated exact-token range, and retains raw
# driver output. It never routes measured traffic through a Service.

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

KUBECONFIG_PATH=${KUBECONFIG_PATH:-/tmp/llm-d-sc-arena-kubeconfig}
NAMESPACE=${NAMESPACE:-llm-d-sc-scaleout}
DEPLOYMENT=${DEPLOYMENT:-classifier-target}
TARGET_SELECTOR=${TARGET_SELECTOR:-app.kubernetes.io/component=classifier-target}
TARGET_NODE=${TARGET_NODE:-gnr2.fm2aihpcsed.com}
DRIVER_NODE=${DRIVER_NODE:-rhgnr1}
REPLICAS=${REPLICAS:?set REPLICAS}
CONCURRENCY=${CONCURRENCY:?set CONCURRENCY}
CONNECTIONS=${CONNECTIONS:-$CONCURRENCY}
DURATION_SECONDS=${DURATION_SECONDS:-60}
START_DELAY_SECONDS=${START_DELAY_SECONDS:-45}
MAX_ROWS_PER_ENDPOINT=${MAX_ROWS_PER_ENDPOINT:-10000}
SEQUENCE_BASE=${SEQUENCE_BASE:?set a globally unused SEQUENCE_BASE}
RUN_ID=${RUN_ID:?set a DNS-safe unique RUN_ID}
DRIVER_IMAGE=${DRIVER_IMAGE:?set the pinned benchmark-driver image digest}
TARGET_IMAGE=${TARGET_IMAGE:?set the expected pinned target image digest}
MODEL_SHA256=${MODEL_SHA256:?set MODEL_SHA256}
TOKENIZER_SHA256=${TOKENIZER_SHA256:-851ca67100d372ca3ae031a6abd168f53489eebfd7d89523f35c5c9b4d372c3c}
TOKEN_COUNT=${TOKEN_COUNT:-64}
RESULT_ROOT=${RESULT_ROOT:-${REPO_ROOT}/results}
RESULT_DIR=${RESULT_ROOT}/${RUN_ID}
RESET_TARGETS=${RESET_TARGETS:-true}
if [[ -n "${OFFERED_RPS:-}" ]]; then
  topology_preflight_default=1
else
  topology_preflight_default=0
fi
TOPOLOGY_PREFLIGHT_ENABLED=${TOPOLOGY_PREFLIGHT_ENABLED:-$topology_preflight_default}
TOPOLOGY_PREFLIGHT_RUNNER=${TOPOLOGY_PREFLIGHT_RUNNER:-${SCRIPT_DIR}/arena-sc-topology-preflight.py}
TOPOLOGY_PREFLIGHT_CONTAINER=${TOPOLOGY_PREFLIGHT_CONTAINER:-}
TOPOLOGY_PREFLIGHT_RESERVED_CPUS=${TOPOLOGY_PREFLIGHT_RESERVED_CPUS:-}

if [[ "$TOPOLOGY_PREFLIGHT_ENABLED" != 0 && "$TOPOLOGY_PREFLIGHT_ENABLED" != 1 ]]; then
  echo "TOPOLOGY_PREFLIGHT_ENABLED must be 0 or 1" >&2
  exit 2
fi
if (( TOPOLOGY_PREFLIGHT_ENABLED == 1 )) && [[ ! -x "$TOPOLOGY_PREFLIGHT_RUNNER" ]]; then
  echo "TOPOLOGY_PREFLIGHT_RUNNER is not executable: ${TOPOLOGY_PREFLIGHT_RUNNER}" >&2
  exit 2
fi
if command -v sha256sum >/dev/null 2>&1; then
  sha256=(sha256sum)
elif command -v shasum >/dev/null 2>&1; then
  sha256=(shasum -a 256)
elif (( TOPOLOGY_PREFLIGHT_ENABLED == 1 )); then
  echo "topology preflight evidence requires sha256sum or shasum" >&2
  exit 2
fi

sha256_path() {
  "${sha256[@]}" "$1" | awk '{print $1}'
}

# Closed loop remains the default. Supplying OFFERED_RPS opts this cell into
# deterministic open-loop scheduling; the remaining knobs are intentionally
# ignored unless OFFERED_RPS is present so a misspelled/partial configuration
# cannot silently change the established closed-loop protocol.
LOAD_MODEL=closed_loop
driver_mode_args_json='[]'
open_loop_cell_json='null'
if [[ -n "${OFFERED_RPS:-}" ]]; then
  LOAD_MODEL=open_loop_deterministic_offered_rate
  MAX_IN_FLIGHT=${MAX_IN_FLIGHT:-$CONCURRENCY}
  DISPATCH_LATE_AFTER_MS=${DISPATCH_LATE_AFTER_MS:-1}
  DROP_LATE_AFTER_MS=${DROP_LATE_AFTER_MS:-100}
  RPC_TIMEOUT_MS=${RPC_TIMEOUT_MS:-30000}

  [[ "$DURATION_SECONDS" =~ ^[1-9][0-9]*$ && "$MAX_ROWS_PER_ENDPOINT" =~ ^[1-9][0-9]*$ ]] || {
    echo "DURATION_SECONDS and MAX_ROWS_PER_ENDPOINT must be positive integers in open-loop mode" >&2
    exit 2
  }

  [[ "$OFFERED_RPS" =~ ^([0-9]+)(\.([0-9]{0,9}))?$ ]] || {
    echo "OFFERED_RPS must be an unsigned decimal with at most nine decimal places" >&2
    exit 2
  }
  offered_whole=${BASH_REMATCH[1]}
  offered_fraction=${BASH_REMATCH[3]:-}
  while [[ ${#offered_whole} -gt 1 && ${offered_whole:0:1} == 0 ]]; do
    offered_whole=${offered_whole:1}
  done
  (( ${#offered_whole} <= 10 )) || {
    echo "OFFERED_RPS cannot exceed 1,000,000,000" >&2
    exit 2
  }
  offered_denominator=1
  for ((digit = 0; digit < ${#offered_fraction}; digit++)); do
    offered_denominator=$((offered_denominator * 10))
  done
  if [[ -n "$offered_fraction" ]]; then
    offered_fraction_value=$((10#$offered_fraction))
  else
    offered_fraction_value=0
  fi
  offered_whole_value=$((10#$offered_whole))
  offered_numerator=$((offered_whole_value * offered_denominator + offered_fraction_value))
  (( offered_numerator > 0 && offered_numerator <= 1000000000 * offered_denominator )) || {
    echo "OFFERED_RPS must be greater than zero and no more than 1,000,000,000" >&2
    exit 2
  }
  for pair in \
    "MAX_IN_FLIGHT:$MAX_IN_FLIGHT" \
    "DISPATCH_LATE_AFTER_MS:$DISPATCH_LATE_AFTER_MS" \
    "DROP_LATE_AFTER_MS:$DROP_LATE_AFTER_MS" \
    "RPC_TIMEOUT_MS:$RPC_TIMEOUT_MS"; do
    [[ "${pair#*:}" =~ ^[0-9]+$ ]] || {
      echo "${pair%%:*} must be an unsigned integer" >&2
      exit 2
    }
  done
  (( MAX_IN_FLIGHT > 0 && RPC_TIMEOUT_MS > 0 )) || {
    echo "MAX_IN_FLIGHT and RPC_TIMEOUT_MS must be positive" >&2
    exit 2
  }
  (( DROP_LATE_AFTER_MS >= DISPATCH_LATE_AFTER_MS )) || {
    echo "DROP_LATE_AFTER_MS must be at least DISPATCH_LATE_AFTER_MS" >&2
    exit 2
  }
  (( offered_numerator <= 9223372036854775807 / DURATION_SECONDS )) || {
    echo "OFFERED_RPS multiplied by DURATION_SECONDS overflows the scheduler preflight" >&2
    exit 2
  }
  offered_slot_numerator=$((offered_numerator * DURATION_SECONDS))
  offered_slots_per_endpoint=$((offered_slot_numerator / offered_denominator))
  if (( offered_slot_numerator % offered_denominator != 0 )); then
    offered_slots_per_endpoint=$((offered_slots_per_endpoint + 1))
  fi
  (( offered_slots_per_endpoint <= MAX_ROWS_PER_ENDPOINT )) || {
    echo "open-loop schedule needs ${offered_slots_per_endpoint} rows per endpoint; MAX_ROWS_PER_ENDPOINT=${MAX_ROWS_PER_ENDPOINT}" >&2
    exit 2
  }

  driver_mode_args_json=$(jq -cn \
    --arg driver_image "$DRIVER_IMAGE" \
    --arg offered_rps "$OFFERED_RPS" \
    --arg max_in_flight "$MAX_IN_FLIGHT" \
    --arg dispatch_late "$DISPATCH_LATE_AFTER_MS" \
    --arg drop_late "$DROP_LATE_AFTER_MS" \
    --arg rpc_timeout "$RPC_TIMEOUT_MS" \
    '["--driver-image",$driver_image,
      "--offered-rps",$offered_rps,
      "--max-in-flight",$max_in_flight,
      "--dispatch-late-after-ms",$dispatch_late,
      "--drop-late-after-ms",$drop_late,
      "--rpc-timeout-ms",$rpc_timeout]')
  open_loop_cell_json=$(jq -cn \
    --arg offered_rps "$OFFERED_RPS" \
    --argjson offered_slots "$offered_slots_per_endpoint" \
    --argjson max_in_flight "$MAX_IN_FLIGHT" \
    --argjson dispatch_late "$DISPATCH_LATE_AFTER_MS" \
    --argjson drop_late "$DROP_LATE_AFTER_MS" \
    --argjson rpc_timeout "$RPC_TIMEOUT_MS" \
    '{offered_rps_per_target:$offered_rps,
      scheduled_slots_per_target:$offered_slots,
      max_in_flight_per_target:$max_in_flight,
      dispatch_late_after_ms:$dispatch_late,
      drop_late_after_ms:$drop_late,
      rpc_timeout_ms:$rpc_timeout,
      load_scope:"one independent scheduler per direct Pod IP"}')
elif [[ -n "${MAX_IN_FLIGHT:-}${DISPATCH_LATE_AFTER_MS:-}${DROP_LATE_AFTER_MS:-}${RPC_TIMEOUT_MS:-}" ]]; then
  echo "open-loop tuning variables require OFFERED_RPS" >&2
  exit 2
fi

if [[ "$TARGET_NODE" == "$DRIVER_NODE" ]]; then
  TOPOLOGY="same-node-direct-${TARGET_NODE}"
else
  TOPOLOGY="cross-node-direct-${TARGET_NODE}-from-${DRIVER_NODE}"
fi

k=(oc --kubeconfig "$KUBECONFIG_PATH")
mkdir -p "$RESULT_DIR/drivers"
mkdir -p "$RESULT_DIR/cgroup"

for node in "$TARGET_NODE" "$DRIVER_NODE"; do
  "${k[@]}" wait --for=condition=Ready "node/${node}" --timeout=60s >/dev/null
done

if [[ "$RESET_TARGETS" == "true" ]]; then
  "${k[@]}" scale deployment "$DEPLOYMENT" -n "$NAMESPACE" --replicas=0 >/dev/null
  existing_pods=$("${k[@]}" get pod -n "$NAMESPACE" -l "$TARGET_SELECTOR" -o name)
  if [[ -n "$existing_pods" ]]; then
    "${k[@]}" wait --for=delete pod -n "$NAMESPACE" -l "$TARGET_SELECTOR" --timeout=300s >/dev/null
  fi
elif [[ "$RESET_TARGETS" != "false" ]]; then
  echo "RESET_TARGETS must be true or false" >&2
  exit 2
fi

"${k[@]}" scale deployment "$DEPLOYMENT" -n "$NAMESPACE" --replicas="$REPLICAS" >/dev/null
"${k[@]}" rollout status deployment/"$DEPLOYMENT" -n "$NAMESPACE" --timeout=600s >/dev/null

pods_json=$("${k[@]}" get pods -n "$NAMESPACE" -l "$TARGET_SELECTOR" -o json)
deployment_json=$("${k[@]}" get deployment "$DEPLOYMENT" -n "$NAMESPACE" -o json)
ready_count=$(jq '[.items[] | select(.status.phase == "Running") | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length' <<<"$pods_json")
if [[ "$ready_count" != "$REPLICAS" ]]; then
  echo "expected ${REPLICAS} Ready targets; found ${ready_count}" >&2
  exit 2
fi
if [[ $(jq '[.items[].status.containerStatuses[]?.restartCount] | add // 0' <<<"$pods_json") != 0 ]]; then
  echo "target restart count is non-zero before cell" >&2
  exit 2
fi
if [[ $(jq --arg node "$TARGET_NODE" '[.items[] | select(.spec.nodeName != $node)] | length' <<<"$pods_json") != 0 ]]; then
  echo "one or more targets are not on fixed serving node ${TARGET_NODE}" >&2
  exit 2
fi
if [[ $(jq --arg digest "$TARGET_IMAGE" '[.items[].status.containerStatuses[]?.imageID | select(endswith($digest) | not)] | length' <<<"$pods_json") != 0 ]]; then
  echo "target image digest mismatch" >&2
  exit 2
fi

jq . <<<"$pods_json" >"$RESULT_DIR/targets-before.json"
jq . <<<"$deployment_json" >"$RESULT_DIR/deployment-before.json"
"${k[@]}" get nodes -o json >"$RESULT_DIR/nodes-before.json"
"${k[@]}" get events -n "$NAMESPACE" -o json >"$RESULT_DIR/events-before.json"

# This is deliberately after target readiness, identity, placement, restart,
# and image checks, but before computing the measurement start time, launching
# the cgroup sampler, or creating any driver Job.  A non-zero runner exit, an
# invalid report, or a report for a different target identity denies load.
topology_preflight_cell_json=$(jq -cn \
  --argjson enabled "$TOPOLOGY_PREFLIGHT_ENABLED" \
  '{enabled:($enabled == 1),required_by_caller:false,load_authorized:($enabled == 0),
    disposition:(if $enabled == 1 then "pending" else "disabled" end)}')
if (( TOPOLOGY_PREFLIGHT_ENABLED == 1 )); then
  topology_report="$RESULT_DIR/topology-preflight-report.json"
  topology_stdout="$RESULT_DIR/topology-preflight-stdout.txt"
  topology_stderr="$RESULT_DIR/topology-preflight-stderr.txt"
  topology_execution="$RESULT_DIR/topology-preflight-execution.json"
  topology_args=(
    live
    --kubeconfig "$KUBECONFIG_PATH"
    --namespace "$NAMESPACE"
    --selector "$TARGET_SELECTOR"
    --expected-pods "$REPLICAS"
    --format json
  )
  if [[ -n "$TOPOLOGY_PREFLIGHT_CONTAINER" ]]; then
    topology_args+=(--container "$TOPOLOGY_PREFLIGHT_CONTAINER")
  fi
  if [[ -n "$TOPOLOGY_PREFLIGHT_RESERVED_CPUS" ]]; then
    set -f
    for reserved_override in $TOPOLOGY_PREFLIGHT_RESERVED_CPUS; do
      topology_args+=(--reserved-cpus "$reserved_override")
    done
    set +f
  fi

  set +e
  "$TOPOLOGY_PREFLIGHT_RUNNER" "${topology_args[@]}" \
    >"$topology_stdout" 2>"$topology_stderr"
  topology_runner_exit=$?
  set -e

  topology_report_json_valid=false
  topology_report_gate_valid=false
  topology_identity_match=false
  topology_report_sha256=""
  if jq -s -e 'length == 1 and (.[0] | type == "object")' \
      "$topology_stdout" >/dev/null 2>&1; then
    jq -s '.[0]' "$topology_stdout" >"$topology_report"
    topology_report_json_valid=true
    topology_report_sha256=$(sha256_path "$topology_report")
    if jq -e --argjson expected "$REPLICAS" '
        .schema_version == 1
        and .verdict == "PASS"
        and .placement_verdict == "PASS"
        and .gate_passed == true
        and .exit_code == 0
        and .snapshot.capture.mode == "live-read-only"
        and .summary.pods == $expected
        and .summary.pods_validated == $expected
        and .summary.placement_violations == 0
        and .summary.invalid_reasons == 0
        and .summary.gate_ineligibility_reasons == 0
      ' "$topology_report" >/dev/null; then
      topology_report_gate_valid=true
    fi
    if jq -e --slurpfile targets "$RESULT_DIR/targets-before.json" '
        ([.pods[] | [.name,.uid,.node]] | sort) ==
        ([$targets[0].items[] | [.metadata.name,.metadata.uid,.spec.nodeName]] | sort)
      ' "$topology_report" >/dev/null; then
      topology_identity_match=true
    fi
  fi
  topology_stdout_sha256=$(sha256_path "$topology_stdout")
  topology_stderr_sha256=$(sha256_path "$topology_stderr")

  topology_load_authorized=false
  if (( topology_runner_exit == 0 )) \
      && [[ "$topology_report_json_valid" == true \
            && "$topology_report_gate_valid" == true \
            && "$topology_identity_match" == true ]]; then
    topology_load_authorized=true
  fi
  jq -n \
    --arg runner "$TOPOLOGY_PREFLIGHT_RUNNER" \
    --arg report_file "topology-preflight-report.json" \
    --arg stdout_file "topology-preflight-stdout.txt" \
    --arg stderr_file "topology-preflight-stderr.txt" \
    --arg report_sha256 "$topology_report_sha256" \
    --arg stdout_sha256 "$topology_stdout_sha256" \
    --arg stderr_sha256 "$topology_stderr_sha256" \
    --argjson runner_exit "$topology_runner_exit" \
    --argjson report_json_valid "$topology_report_json_valid" \
    --argjson report_gate_valid "$topology_report_gate_valid" \
    --argjson identity_match "$topology_identity_match" \
    --argjson load_authorized "$topology_load_authorized" \
    '{schema_version:1,gate:"cpu_topology_pre_load",enabled:true,
      runner:$runner,runner_exit_code:$runner_exit,
      report_file:(if $report_json_valid then $report_file else null end),
      raw_stdout_file:$stdout_file,stderr_file:$stderr_file,
      evidence_sha256:{
        report:(if $report_sha256 == "" then null else $report_sha256 end),
        raw_stdout:$stdout_sha256,stderr:$stderr_sha256},
      report_json_valid:$report_json_valid,report_gate_valid:$report_gate_valid,
      target_identity_match:$identity_match,load_authorized:$load_authorized,
      disposition:(if $load_authorized then "pass" else "invalid_pre_load" end)}' \
    >"$topology_execution"
  topology_execution_sha256=$(sha256_path "$topology_execution")

  topology_preflight_cell_json=$(jq -cn \
    --slurpfile execution "$topology_execution" \
    --slurpfile report "$topology_report" \
    --arg execution_sha256 "$topology_execution_sha256" \
    '($execution[0] + {
      required_by_caller:true,
      execution_sha256:$execution_sha256,
      report_verdict:($report[0].verdict // null),
      placement_verdict:($report[0].placement_verdict // null),
      report_summary:($report[0].summary // null)
    })' 2>/dev/null || jq -cn --slurpfile execution "$topology_execution" \
      --arg execution_sha256 "$topology_execution_sha256" \
      '$execution[0] + {required_by_caller:true,execution_sha256:$execution_sha256,report_verdict:null,
        placement_verdict:null,report_summary:null}')
  if [[ "$topology_load_authorized" != true ]]; then
    echo "CPU-topology preflight denied load; inspect ${topology_execution}" >&2
    exit 6
  fi
fi

targets=()
while IFS= read -r target; do
  targets+=("$target")
done < <(jq -r '.items | sort_by(.metadata.name)[] | [.metadata.name,.metadata.uid,.status.podIP] | @tsv' <<<"$pods_json")
start_epoch_ms=$(( ($(date -u +%s) + START_DELAY_SECONDS) * 1000 ))
created_epoch_ms=$(( $(date -u +%s) * 1000 ))
sequence_span=$((MAX_ROWS_PER_ENDPOINT + CONNECTIONS))

capture_cgroup_snapshot() {
  local phase=$1 target target_pod target_uid target_ip output
  local -a snapshot_pids=()
  for target in "${targets[@]}"; do
    IFS=$'\t' read -r target_pod target_uid target_ip <<<"$target"
    output="$RESULT_DIR/cgroup/${target_pod}-${phase}.txt"
    (
      echo "local_before_epoch_s $(date -u +%s)"
      "${k[@]}" exec -n "$NAMESPACE" "$target_pod" -- sh -c '
        cat /sys/fs/cgroup/cpu.stat
        cpus=$(cat /sys/fs/cgroup/cpuset.cpus.effective)
        printf "cpuset_cpus_effective %s\n" "$cpus"
        printf "cpu_max "
        cat /sys/fs/cgroup/cpu.max
        first_cpu=${cpus%%[-,]*}
        printf "first_cpu %s\n" "$first_cpu"
        printf "scaling_cur_freq_khz "
        cat "/sys/devices/system/cpu/cpu${first_cpu}/cpufreq/scaling_cur_freq" 2>/dev/null || printf "unavailable\n"
      '
      echo "local_after_epoch_s $(date -u +%s)"
    ) >"$output" &
    snapshot_pids+=("$!")
  done
  for target in "${snapshot_pids[@]}"; do
    wait "$target"
  done
}

(
  plateau_start_s=$((start_epoch_ms / 1000))
  plateau_end_s=$((plateau_start_s + DURATION_SECONDS))
  delay_s=$((plateau_start_s - $(date -u +%s)))
  if (( delay_s > 0 )); then sleep "$delay_s"; fi
  capture_cgroup_snapshot start
  delay_s=$((plateau_end_s - $(date -u +%s)))
  if (( delay_s > 0 )); then sleep "$delay_s"; fi
  capture_cgroup_snapshot end
) &
cgroup_sampler_pid=$!

for index in "${!targets[@]}"; do
  IFS=$'\t' read -r target_pod target_uid target_ip <<<"${targets[$index]}"
  ordinal=$((index + 1))
  job="sc-${RUN_ID}-${ordinal}"
  sequence_base=$((SEQUENCE_BASE + index * sequence_span))
  "${k[@]}" delete job "$job" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null
  "${k[@]}" create job "$job" -n "$NAMESPACE" --image="$DRIVER_IMAGE" --dry-run=client -o json \
    | jq \
      --arg run "$RUN_ID" \
      --arg target_pod "$target_pod" \
      --arg target_uid "$target_uid" \
      --arg target_ip "$target_ip" \
      --arg target_node "$TARGET_NODE" \
      --arg driver_node "$DRIVER_NODE" \
      --arg target_image "$TARGET_IMAGE" \
      --arg model "$MODEL_SHA256" \
      --arg tokenizer "$TOKENIZER_SHA256" \
      --arg topology "$TOPOLOGY" \
      --arg start "$start_epoch_ms" \
      --arg duration "$DURATION_SECONDS" \
      --arg concurrency "$CONCURRENCY" \
      --arg connections "$CONNECTIONS" \
      --arg max_rows "$MAX_ROWS_PER_ENDPOINT" \
      --arg sequence_base "$sequence_base" \
      --arg token_count "$TOKEN_COUNT" \
      --argjson driver_mode_args "$driver_mode_args_json" \
      '.metadata.labels += {"benchmark.llm-d/run-id":$run,"benchmark.llm-d/component":"inference-driver"}
       | .metadata.annotations += {"benchmark.llm-d/target-pod":$target_pod,"benchmark.llm-d/target-uid":$target_uid}
       | .spec.backoffLimit=0
       | .spec.ttlSecondsAfterFinished=86400
       | .spec.template.metadata.labels += {"benchmark.llm-d/run-id":$run,"benchmark.llm-d/component":"inference-driver"}
       | .spec.template.spec.nodeSelector={"kubernetes.io/hostname":$driver_node}
       | .spec.template.spec.securityContext={"runAsNonRoot":true,"seccompProfile":{"type":"RuntimeDefault"}}
       | .spec.template.spec.containers[0].command=["/usr/local/bin/llm-d-sc-sustained-corpus-probe"]
       | .spec.template.spec.containers[0].args=[
           "--target",($target_ip+":50051"),
           "--token-count",$token_count,
           "--sequence-base",$sequence_base,
           "--max-rows",$max_rows,
           "--tokenizer-sha256",$tokenizer,
           "--concurrency",$concurrency,
           "--connections",$connections,
           "--warmup-requests",$connections,
           "--duration-seconds",$duration,
           "--start-epoch-ms",$start,
           "--target-image",$target_image,
           "--model-sha256",$model,
           "--topology",$topology,
           "--raw-latencies"
         ] + $driver_mode_args
       | .spec.template.spec.containers[0].resources={"requests":{"cpu":"500m","memory":"256Mi"},"limits":{"cpu":"4","memory":"1Gi"}}
       | .spec.template.spec.containers[0].securityContext={"allowPrivilegeEscalation":false,"readOnlyRootFilesystem":true,"capabilities":{"drop":["ALL"]}}' \
    | "${k[@]}" apply -f - >/dev/null
done

drain_allowance_seconds=120
if [[ "$LOAD_MODEL" == open_loop_deterministic_offered_rate ]]; then
  requested_drain_allowance=$(((RPC_TIMEOUT_MS + 999) / 1000 + 90))
  if (( requested_drain_allowance > drain_allowance_seconds )); then
    drain_allowance_seconds=$requested_drain_allowance
  fi
fi
timeout_seconds=$((START_DELAY_SECONDS + DURATION_SECONDS + drain_allowance_seconds))
if ! "${k[@]}" wait --for=condition=complete job -n "$NAMESPACE" \
  -l "benchmark.llm-d/run-id=${RUN_ID}" --timeout="${timeout_seconds}s" >/dev/null; then
  "${k[@]}" get jobs,pods -n "$NAMESPACE" -l "benchmark.llm-d/run-id=${RUN_ID}" -o wide >&2
  exit 3
fi
wait "$cgroup_sampler_pid"

driver_log_pids=()
for ordinal in $(seq 1 "$REPLICAS"); do
  job="sc-${RUN_ID}-${ordinal}"
  "${k[@]}" logs -n "$NAMESPACE" job/"$job" >"$RESULT_DIR/drivers/${job}.json" &
  driver_log_pids+=("$!")
done
for driver_log_pid in "${driver_log_pids[@]}"; do
  wait "$driver_log_pid"
done

"${k[@]}" get pods -n "$NAMESPACE" -l "$TARGET_SELECTOR" -o json >"$RESULT_DIR/targets-after.json"
"${k[@]}" get nodes -o json >"$RESULT_DIR/nodes-after.json"
"${k[@]}" get events -n "$NAMESPACE" -o json >"$RESULT_DIR/events-after.json"
"${k[@]}" logs -n "$NAMESPACE" -l "$TARGET_SELECTOR" --prefix --tail=-1 >"$RESULT_DIR/target-logs.txt"
jq -s . "$RESULT_DIR"/drivers/*.json >"$RESULT_DIR/drivers.json"

if [[ "$LOAD_MODEL" == open_loop_deterministic_offered_rate ]]; then
  if ! jq -e \
    --argjson replicas "$REPLICAS" \
    --argjson concurrency "$CONCURRENCY" \
    --argjson connections "$CONNECTIONS" \
    --argjson duration "$DURATION_SECONDS" \
    --argjson max_rows "$MAX_ROWS_PER_ENDPOINT" \
    --argjson expected_slots "$offered_slots_per_endpoint" \
    --argjson max_in_flight "$MAX_IN_FLIGHT" \
    --argjson dispatch_late "$DISPATCH_LATE_AFTER_MS" \
    --argjson drop_late "$DROP_LATE_AFTER_MS" \
    --argjson rpc_timeout "$RPC_TIMEOUT_MS" \
    --arg offered_rps "$OFFERED_RPS" \
    --arg driver_image "$DRIVER_IMAGE" \
    --arg target_image "$TARGET_IMAGE" \
    --arg model "$MODEL_SHA256" \
    --arg tokenizer "$TOKENIZER_SHA256" \
    --arg topology "$TOPOLOGY" \
    --argjson token_count "$TOKEN_COUNT" \
    --argjson start_epoch_ms "$start_epoch_ms" \
    --argjson sequence_base "$SEQUENCE_BASE" \
    --argjson sequence_span "$sequence_span" \
    --slurpfile targets "$RESULT_DIR/targets-before.json" '
      length == $replicas
      and all(.[];
        .schema_version == 2
        and .probe == "sustained_exact_token_corpus"
        and .load_model == "open_loop_deterministic_offered_rate"
        and .open_loop.protocol_version == "deterministic_offered_rate_v1"
        and .open_loop.driver_image == $driver_image
        and .open_loop.offered_rate.requested_decimal == $offered_rps
        and .open_loop.max_in_flight == $max_in_flight
        and .open_loop.dispatch_late_after_ms == $dispatch_late
        and .open_loop.drop_late_after_ms == $drop_late
        and .open_loop.rpc_timeout_ms == $rpc_timeout
        and .open_loop.raw_rtt_collection == "always enabled in open-loop mode"
        and .target_image == $target_image
        and .model_sha256 == $model
        and .tokenizer_sha256 == $tokenizer
        and .topology == $topology
        and .token_count_including_specials == $token_count
        and .corpus_mode == "generated"
        and .generator_scheme == "alpha_bravo_lsb_identity_service_fill_v1"
        and .connections == $connections
        and .closed_loop_concurrency_argument == $concurrency
        and .warmup_requests == $connections
        and .candidate_rows == $max_rows
        and .scheduled_plateau_rows == $expected_slots
        and .start_epoch_ms == $start_epoch_ms
        and .duration_seconds == $duration
        and .scheduler_ready_epoch_ms < .start_epoch_ms
        and (.corpus_exhausted | not)
        and .claimed_plateau_rows == .accounting.initiated_requests
        and .accounting.offered_slots == $expected_slots
        and .accounting.offered_slots ==
          (.accounting.initiated_requests + .accounting.dropped_before_initiation_total)
        and .accounting.initiated_requests == .accounting.completed_requests
        and .accounting.completed_requests ==
          (.accounting.completed_within_plateau + .accounting.completed_after_plateau)
        and .accounting.completed_within_plateau ==
          ([.statuses_completed_within_plateau[]] | add // 0)
        and .accounting.completed_after_plateau ==
          ([.drained_after_plateau[]] | add // 0)
        and (.successful_rtt_raw_us | length) ==
          (.statuses_completed_within_plateau.OK // 0)
        and .successful_rtt_raw_us == (.successful_rtt_raw_us | sort)
        and ([.rtt_raw_us_by_status[][]] | length) == .accounting.completed_requests
        and (.selected_rows_blake3 | type) == "string"
        and (.selected_rows_blake3 | length) == 64
        and (.scheduled_rows_blake3 | type) == "string"
        and (.scheduled_rows_blake3 | length) == 64
        and .last_sequence ==
          (.first_sequence + .warmup_requests + .candidate_rows - 1))
      and ([.[].first_sequence] | sort) ==
        [range(0; $replicas) | $sequence_base + (. * $sequence_span)]
      and ([.[].target] | sort) ==
        ([$targets[0].items[].status.podIP + ":50051"] | sort)
      and ([.[].selected_rows_blake3] | unique | length) == $replicas
      and ([.[].scheduled_rows_blake3] | unique | length) == $replicas
    ' "$RESULT_DIR/drivers.json" >/dev/null; then
    echo "open-loop driver provenance/accounting gate failed" >&2
    exit 5
  fi
fi

for target in "${targets[@]}"; do
  IFS=$'\t' read -r target_pod target_uid target_ip <<<"$target"
  start_file="$RESULT_DIR/cgroup/${target_pod}-start.txt"
  end_file="$RESULT_DIR/cgroup/${target_pod}-end.txt"
  start_epoch=$(awk '$1 == "local_before_epoch_s" {print $2}' "$start_file")
  end_epoch=$(awk '$1 == "local_before_epoch_s" {print $2}' "$end_file")
  start_usage=$(awk '$1 == "usage_usec" {print $2}' "$start_file")
  end_usage=$(awk '$1 == "usage_usec" {print $2}' "$end_file")
  start_periods=$(awk '$1 == "nr_periods" {print $2}' "$start_file")
  end_periods=$(awk '$1 == "nr_periods" {print $2}' "$end_file")
  start_throttled=$(awk '$1 == "nr_throttled" {print $2}' "$start_file")
  end_throttled=$(awk '$1 == "nr_throttled" {print $2}' "$end_file")
  start_throttled_usec=$(awk '$1 == "throttled_usec" {print $2}' "$start_file")
  end_throttled_usec=$(awk '$1 == "throttled_usec" {print $2}' "$end_file")
  start_cpuset=$(awk '$1 == "cpuset_cpus_effective" {print $2}' "$start_file")
  end_cpuset=$(awk '$1 == "cpuset_cpus_effective" {print $2}' "$end_file")
  start_cpu_max=$(awk '$1 == "cpu_max" {$1=""; sub(/^ /,""); print}' "$start_file")
  end_cpu_max=$(awk '$1 == "cpu_max" {$1=""; sub(/^ /,""); print}' "$end_file")
  start_frequency=$(awk '$1 == "scaling_cur_freq_khz" {print $2}' "$start_file")
  end_frequency=$(awk '$1 == "scaling_cur_freq_khz" {print $2}' "$end_file")
  jq -n \
    --arg pod "$target_pod" \
    --arg uid "$target_uid" \
    --argjson start_epoch "$start_epoch" \
    --argjson end_epoch "$end_epoch" \
    --argjson start_usage "$start_usage" \
    --argjson end_usage "$end_usage" \
    --argjson start_periods "$start_periods" \
    --argjson end_periods "$end_periods" \
    --argjson start_throttled "$start_throttled" \
    --argjson end_throttled "$end_throttled" \
    --argjson start_throttled_usec "$start_throttled_usec" \
    --argjson end_throttled_usec "$end_throttled_usec" \
    --arg start_cpuset "$start_cpuset" \
    --arg end_cpuset "$end_cpuset" \
    --arg start_cpu_max "$start_cpu_max" \
    --arg end_cpu_max "$end_cpu_max" \
    --arg start_frequency "$start_frequency" \
    --arg end_frequency "$end_frequency" \
    '{pod:$pod,pod_uid:$uid,start_epoch_s:$start_epoch,end_epoch_s:$end_epoch,
      elapsed_seconds:($end_epoch-$start_epoch),
      cpuset_cpus_effective:{start:$start_cpuset,end:$end_cpuset},
      cpu_max:{start:$start_cpu_max,end:$end_cpu_max},
      scaling_cur_freq_khz:{
        start:(try ($start_frequency|tonumber) catch null),
        end:(try ($end_frequency|tonumber) catch null)},
      usage_usec_delta:($end_usage-$start_usage),
      average_cpu_cores:(($end_usage-$start_usage)/(($end_epoch-$start_epoch)*1000000)),
      nr_periods_delta:($end_periods-$start_periods),
      nr_throttled_delta:($end_throttled-$start_throttled),
      throttled_usec_delta:($end_throttled_usec-$start_throttled_usec),
      throttled_period_ratio:(if ($end_periods-$start_periods)>0 then
        (($end_throttled-$start_throttled)/($end_periods-$start_periods)) else null end)}'
done | jq -s . >"$RESULT_DIR/cgroup-summary.json"

jq -n \
  --slurpfile before "$RESULT_DIR/events-before.json" \
  --slurpfile after "$RESULT_DIR/events-after.json" \
  --slurpfile targets "$RESULT_DIR/targets-before.json" \
  --argjson plateau_start "$((start_epoch_ms / 1000))" \
  --argjson plateau_end "$((start_epoch_ms / 1000 + DURATION_SECONDS))" \
  'def event_epoch: sub("\\.[0-9]+Z$";"Z") | fromdateiso8601;
   ($targets[0].items | map(.metadata.uid)) as $target_uids
   | ($before[0].items | map({key:.metadata.uid,value:(.count // 1)}) | from_entries) as $before_counts
   | [$after[0].items[]
      | select(.involvedObject.uid as $uid | $target_uids | index($uid))
      | (.count // 1) as $after_count
      | ($before_counts[.metadata.uid] // 0) as $before_count
      | select($after_count > $before_count)
      | select(.type == "Warning" or .reason == "Unhealthy")
      | (.eventTime // .firstTimestamp // .metadata.creationTimestamp) as $first_timestamp
      | (.series.lastObservedTime // .lastTimestamp // .eventTime // .metadata.creationTimestamp) as $last_timestamp
      | ($last_timestamp | event_epoch) as $last_epoch
      | {event_uid:.metadata.uid,type,reason,message,
         involved_object:.involvedObject.name,
         first_timestamp:$first_timestamp,
         last_timestamp:$last_timestamp,
         last_observed_phase:(if $last_epoch < $plateau_start then "pre_plateau"
           elif $last_epoch <= $plateau_end then "plateau" else "post_plateau" end),
         new_occurrences:($after_count - $before_count)}]' \
  >"$RESULT_DIR/health-event-violations.json"

after_ready_count=$(jq '[.items[] | select(.status.phase == "Running") | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length' "$RESULT_DIR/targets-after.json")
after_restarts=$(jq '[.items[].status.containerStatuses[]?.restartCount] | add // 0' "$RESULT_DIR/targets-after.json")
before_uids=$(jq -c '[.items[].metadata.uid] | sort' "$RESULT_DIR/targets-before.json")
after_uids=$(jq -c '[.items[].metadata.uid] | sort' "$RESULT_DIR/targets-after.json")
if [[ "$after_ready_count" != "$REPLICAS" || "$after_restarts" != 0 || "$before_uids" != "$after_uids" ]]; then
  echo "target health or identity changed during cell" >&2
  exit 4
fi
for node in "$TARGET_NODE" "$DRIVER_NODE"; do
  if ! jq -e --arg node "$node" '.items[] | select(.metadata.name == $node) | any(.status.conditions[]?; .type == "Ready" and .status == "True")' "$RESULT_DIR/nodes-after.json" >/dev/null; then
    echo "node ${node} was not Ready after cell" >&2
    exit 4
  fi
done

completed_epoch_ms=$(( $(date -u +%s) * 1000 ))
inference_workers=$(jq -r '[.spec.template.spec.containers[0].env[]? | select(.name == "LLM_D_SC_INFERENCE_WORKERS") | .value][0] // "unset"' "$RESULT_DIR/deployment-before.json")
rayon_threads=$(jq -r '[.spec.template.spec.containers[0].env[]? | select(.name == "RAYON_NUM_THREADS") | .value][0] // "unset"' "$RESULT_DIR/deployment-before.json")
candle_threads=$(jq -r '[.spec.template.spec.containers[0].env[]? | select(.name == "CANDLE_NUM_THREADS") | .value][0] // "unset"' "$RESULT_DIR/deployment-before.json")
qos_class=$(jq -r '.items[0].status.qosClass // "unknown"' "$RESULT_DIR/targets-before.json")
cpu_request=$(jq -r '.spec.template.spec.containers[0].resources.requests.cpu // "unset"' "$RESULT_DIR/deployment-before.json")
cpu_limit=$(jq -r '.spec.template.spec.containers[0].resources.limits.cpu // "unset"' "$RESULT_DIR/deployment-before.json")
memory_request=$(jq -r '.spec.template.spec.containers[0].resources.requests.memory // "unset"' "$RESULT_DIR/deployment-before.json")
memory_limit=$(jq -r '.spec.template.spec.containers[0].resources.limits.memory // "unset"' "$RESULT_DIR/deployment-before.json")
jq -n \
  --arg run_id "$RUN_ID" \
  --arg namespace "$NAMESPACE" \
  --arg deployment "$DEPLOYMENT" \
  --arg target_node "$TARGET_NODE" \
  --arg driver_node "$DRIVER_NODE" \
  --arg target_image "$TARGET_IMAGE" \
  --arg driver_image "$DRIVER_IMAGE" \
  --arg model_sha256 "$MODEL_SHA256" \
  --arg tokenizer_sha256 "$TOKENIZER_SHA256" \
  --arg topology "$TOPOLOGY" \
  --arg load_model "$LOAD_MODEL" \
  --arg inference_workers "$inference_workers" \
  --arg rayon_threads "$rayon_threads" \
  --arg candle_threads "$candle_threads" \
  --arg qos_class "$qos_class" \
  --arg cpu_request "$cpu_request" \
  --arg cpu_limit "$cpu_limit" \
  --arg memory_request "$memory_request" \
  --arg memory_limit "$memory_limit" \
  --argjson replicas "$REPLICAS" \
  --argjson concurrency "$CONCURRENCY" \
  --argjson connections "$CONNECTIONS" \
  --argjson duration_seconds "$DURATION_SECONDS" \
  --argjson sequence_base "$SEQUENCE_BASE" \
  --argjson sequence_span "$sequence_span" \
  --argjson reserved_sequence_end_exclusive "$((SEQUENCE_BASE + REPLICAS * sequence_span))" \
  --argjson start_epoch_ms "$start_epoch_ms" \
  --argjson created_epoch_ms "$created_epoch_ms" \
  --argjson completed_epoch_ms "$completed_epoch_ms" \
  --argjson open_loop "$open_loop_cell_json" \
  --argjson topology_preflight "$topology_preflight_cell_json" \
  '{schema_version:1,run_id:$run_id,namespace:$namespace,deployment:$deployment,
    target_node:$target_node,driver_node:$driver_node,target_image:$target_image,
    driver_image:$driver_image,model_sha256:$model_sha256,tokenizer_sha256:$tokenizer_sha256,
    topology:$topology,load_model:$load_model,open_loop:$open_loop,
    topology_preflight:$topology_preflight,
    inference_workers:$inference_workers,
    runtime_threads:{rayon:$rayon_threads,candle:$candle_threads},qos_class:$qos_class,
    resources:{requests:{cpu:$cpu_request,memory:$memory_request},limits:{cpu:$cpu_limit,memory:$memory_limit}},
    replicas:$replicas,concurrency_per_target:$concurrency,connections_per_target:$connections,
    sequence_base:$sequence_base,sequence_span_per_endpoint:$sequence_span,
    reserved_sequence_end_exclusive:$reserved_sequence_end_exclusive,
    duration_seconds:$duration_seconds,start_epoch_ms:$start_epoch_ms,
    created_epoch_ms:$created_epoch_ms,completed_epoch_ms:$completed_epoch_ms}' \
  >"$RESULT_DIR/cell.json"

jq -n \
  --slurpfile cell "$RESULT_DIR/cell.json" \
  --slurpfile drivers "$RESULT_DIR/drivers.json" \
  --slurpfile health_events "$RESULT_DIR/health-event-violations.json" \
  --slurpfile cgroup "$RESULT_DIR/cgroup-summary.json" \
  'def distribution($values):
     ($values | sort) as $samples
     | ($samples | length) as $count
     | if $count == 0 then null else {
         samples:$count,
         min:$samples[0],
         p50:$samples[(((($count * 0.50) | ceil) - 1) | if . < 0 then 0 else . end)],
         p95:$samples[(((($count * 0.95) | ceil) - 1) | if . < 0 then 0 else . end)],
         p99:$samples[(((($count * 0.99) | ceil) - 1) | if . < 0 then 0 else . end)],
         max:$samples[-1]
       } end;
   ([$drivers[0][].successful_rtt_raw_us[]?] | sort) as $latencies
   | ($latencies | length) as $latency_count
   | ([$drivers[0][].dispatch_lag_raw_us[]?] | sort) as $dispatch_lags
   | ([$drivers[0][].dropped_in_flight_lag_raw_us[]?] | sort) as $in_flight_drop_lags
   | ([$drivers[0][].dropped_schedule_lag_raw_us[]?] | sort) as $schedule_drop_lags
   | ([$drivers[0][].statuses_completed_within_plateau | to_entries[]]
      | group_by(.key) | map({key:.[0].key,value:(map(.value)|add)}) | from_entries) as $statuses
   | ([$drivers[0][].drained_after_plateau | to_entries[].value] | add // 0) as $drained
   | ([$drivers[0][].accounting.offered_slots] | add // null) as $offered
   | ([$drivers[0][].accounting.dropped_before_initiation_total] | add // null) as $dropped
   | ([$drivers[0][].accounting.dropped_in_flight_limit] | add // null) as $dropped_in_flight
   | ([$drivers[0][].accounting.dropped_schedule_late] | add // null) as $dropped_schedule
   | ([$drivers[0][].accounting.dispatch_late_slots] | add // null) as $dispatch_late
   | ([$drivers[0][].accounting.initiated_late] | add // null) as $initiated_late
   | ([$drivers[0][].accounting.completed_requests] | add // null) as $completed_after_drain
   | ([$drivers[0][].claimed_plateau_rows] | add // 0) as $initiated
   | ([$drivers[0][].statuses_completed_within_plateau | to_entries[].value] | add // 0) as $completed
   | ([$drivers[0][].statuses_completed_within_plateau.OK] | add // 0) as $ok
   | {cell:$cell[0],load_model:$cell[0].load_model,
    offered_slots:$offered,
    dropped_before_initiation:$dropped,
    dropped_in_flight_limit:$dropped_in_flight,
    dropped_schedule_late:$dropped_schedule,
    dispatch_late_slots:$dispatch_late,
    initiated_late:$initiated_late,
    initiated_within_plateau:([$drivers[0][].claimed_plateau_rows] | add // 0),
    completed_within_plateau:$completed,
    completed_after_drain:$completed_after_drain,
    ok_completed_within_plateau:$ok,
    error_completed_within_plateau:($completed-$ok),
    drained_after_plateau:$drained,
    aggregate_offered_rps:(if $offered == null then null else ($offered/$cell[0].duration_seconds) end),
    aggregate_initiated_rps:($initiated/$cell[0].duration_seconds),
    aggregate_useful_rps:($ok/$cell[0].duration_seconds),
    endpoint_rps:[$drivers[0][].useful_requests_per_second],
    endpoint_offered_rps:[$drivers[0][].offered_requests_per_second?],
    offered_acceptance_ratio:(if $offered == null or $offered == 0 then null else ($initiated/$offered) end),
    offered_success_ratio:(if $offered == null or $offered == 0 then null else ($ok/$offered) end),
    accounting_valid:(if $offered == null then null else
      ($offered == ($initiated + $dropped)
       and $initiated == $completed_after_drain
       and $completed_after_drain == ($completed + $drained)) end),
    corpus_exhausted:any($drivers[0][];.corpus_exhausted),
    workers_late:(if $cell[0].load_model == "closed_loop" then
      any($drivers[0][];.workers_ready_epoch_ms >= .start_epoch_ms)
      else any($drivers[0][];.scheduler_ready_epoch_ms >= .start_epoch_ms) end),
    health_event_violations:($health_events[0] | length),
    health_event_violations_by_phase:{
      pre_plateau:([$health_events[0][] | select(.last_observed_phase == "pre_plateau")] | length),
      plateau:([$health_events[0][] | select(.last_observed_phase == "plateau")] | length),
      post_plateau:([$health_events[0][] | select(.last_observed_phase == "post_plateau")] | length)
    },
    cgroup_cpu:$cgroup[0],
    latency_us:distribution($latencies),
    dispatch_lag_us:distribution($dispatch_lags),
    dropped_in_flight_lag_us:distribution($in_flight_drop_lags),
    dropped_schedule_lag_us:distribution($schedule_drop_lags),
    statuses:$statuses,
    service_clean:($completed == $ok and $drained == 0
      and ($health_events[0] | length) == 0
      and ($dropped == null or $dropped == 0))}' \
  >"$RESULT_DIR/summary.json"

if [[ "$LOAD_MODEL" == closed_loop ]]; then
  validity_filter='.initiated_within_plateau > 0
    and .completed_within_plateau == .ok_completed_within_plateau
    and .latency_us.samples == .ok_completed_within_plateau
    and (.corpus_exhausted | not) and (.workers_late | not)
    and .health_event_violations == 0 and (.statuses | keys == ["OK"])'
else
  validity_filter='.offered_slots > 0
    and .accounting_valid
    and ((.latency_us.samples // 0) == .ok_completed_within_plateau)
    and (.corpus_exhausted | not) and (.workers_late | not)'
fi
if ! jq -e "$validity_filter" "$RESULT_DIR/summary.json" >/dev/null; then
  echo "driver validity gate failed" >&2
  cat "$RESULT_DIR/summary.json" >&2
  exit 5
fi

cat "$RESULT_DIR/summary.json"
