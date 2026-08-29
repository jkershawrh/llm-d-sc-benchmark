#!/usr/bin/env bash
set -euo pipefail

KUBECONFIG_PATH=${KUBECONFIG_PATH:-/tmp/llm-d-sc-arena-kubeconfig}
NAMESPACE=${NAMESPACE:-llm-d-sc-scaleout}
REQUESTS=${REQUESTS:-100000}
CONCURRENCY=${CONCURRENCY:-100}
CONNECTIONS=${CONNECTIONS:-100}
REPETITIONS=${REPETITIONS:-3}
CONDITIONS=${CONDITIONS:-ABC}
MATRIX_RUN_ID=${MATRIX_RUN_ID:-}
DRIVER_IMAGE=${DRIVER_IMAGE:-image-registry.openshift-image-registry.svc:5000/llm-d-sc-gremlins/llm-d-sc-gremlin@sha256:37377f46ae5f408607fc31a8ccf1aead27b61a8a338d2eeeeda015e914a9e87a}

k=(kubectl --kubeconfig "$KUBECONFIG_PATH")

wait_nodes() {
  "${k[@]}" wait --for=condition=Ready node/gnr2.fm2aihpcsed.com node/rhgnr1 --timeout=90s >/dev/null
}

scale_target() {
  local deployment=$1 replicas=$2
  "${k[@]}" scale deployment "$deployment" -n "$NAMESPACE" --replicas="$replicas" >/dev/null
  if [[ "$replicas" == 0 ]]; then
    "${k[@]}" wait --for=delete pod -n "$NAMESPACE" -l "app.kubernetes.io/name=${deployment/classifier-target/llm-d-sc-scaleout}" --timeout=120s >/dev/null 2>&1 || true
  else
    "${k[@]}" rollout status deployment/"$deployment" -n "$NAMESPACE" --timeout=180s >/dev/null
  fi
}

collectors_off() {
  "${k[@]}" patch daemonset llm-d-sc-otel -n "$NAMESPACE" --type=merge \
    -p '{"spec":{"template":{"spec":{"nodeSelector":{"benchmark.llm-d/telemetry":"disabled"}}}}}' >/dev/null
  for _ in {1..60}; do
    [[ $("${k[@]}" get daemonset llm-d-sc-otel -n "$NAMESPACE" -o jsonpath='{.status.numberReady}') == 0 ]] && return
    sleep 1
  done
  return 1
}

collectors_on() {
  "${k[@]}" patch daemonset llm-d-sc-otel -n "$NAMESPACE" --type=json \
    -p '[{"op":"remove","path":"/spec/template/spec/nodeSelector/benchmark.llm-d~1telemetry"}]' \
    >/dev/null 2>&1 || true
  "${k[@]}" apply -f deploy/arena-otel-infra.yaml >/dev/null
  "${k[@]}" rollout status daemonset/llm-d-sc-otel -n "$NAMESPACE" --timeout=180s >/dev/null
  [[ $("${k[@]}" get daemonset llm-d-sc-otel -n "$NAMESPACE" -o jsonpath='{.status.numberReady}') == 2 ]]
}

run_cell() {
  local condition=$1 repetition=$2 target=$3
  local condition_lower
  condition_lower=$(printf '%s' "$condition" | tr '[:upper:]' '[:lower:]')
  local suffix=""
  [[ -n "$MATRIX_RUN_ID" ]] && suffix="-${MATRIX_RUN_ID}"
  local name="otel-${condition_lower}${suffix}-r${repetition}"
  "${k[@]}" delete job "$name" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null
  "${k[@]}" create job "$name" -n "$NAMESPACE" --image="$DRIVER_IMAGE" --dry-run=client -o json -- \
    /usr/local/bin/llm-d-sc-connection-probe \
    --target "$target" --topology "arena-otel-${condition}${suffix}-r${repetition}" \
    --context-bytes 256 --concurrency "$CONCURRENCY" --connections "$CONNECTIONS" --requests "$REQUESTS" \
    | jq '.spec.backoffLimit=0
      | .spec.ttlSecondsAfterFinished=86400
      | .spec.template.spec.nodeSelector={"kubernetes.io/hostname":"gnr2.fm2aihpcsed.com"}
      | .spec.template.spec.securityContext={"runAsNonRoot":true,"seccompProfile":{"type":"RuntimeDefault"}}
      | .spec.template.spec.containers[0].resources={"requests":{"cpu":"2","memory":"512Mi"},"limits":{"cpu":"8","memory":"2Gi"}}
      | .spec.template.spec.containers[0].securityContext={"allowPrivilegeEscalation":false,"readOnlyRootFilesystem":true,"capabilities":{"drop":["ALL"]}}' \
    | "${k[@]}" apply -f - >/dev/null
  "${k[@]}" wait --for=condition=complete job/"$name" -n "$NAMESPACE" --timeout=300s >/dev/null
  printf 'MATRIX_RESULT condition=%s repetition=%s ' "$condition" "$repetition"
  "${k[@]}" logs job/"$name" -n "$NAMESPACE" | jq -c .
  wait_nodes
}

set_trace_ratio() {
  local ratio=$1
  "${k[@]}" set env deployment/classifier-otel-candidate -n "$NAMESPACE" \
    "LLM_D_SC_TRACE_SAMPLE_RATIO=$ratio" >/dev/null
  "${k[@]}" rollout status deployment/classifier-otel-candidate -n "$NAMESPACE" --timeout=240s >/dev/null
}

trap 'collectors_on >/dev/null 2>&1 || true; scale_target classifier-target 1 >/dev/null 2>&1 || true; scale_target classifier-otel-candidate 1 >/dev/null 2>&1 || true; set_trace_ratio 0 >/dev/null 2>&1 || true' EXIT

wait_nodes
if [[ "$CONDITIONS" == *A* ]]; then
  scale_target classifier-otel-candidate 0
  scale_target classifier-target 1
  collectors_off
  for repetition in $(seq 1 "$REPETITIONS"); do
    run_cell A "$repetition" classifier-target.llm-d-sc-scaleout.svc:50051
  done
fi

if [[ "$CONDITIONS" == *B* ]]; then
  scale_target classifier-otel-candidate 0
  scale_target classifier-target 1
  collectors_on
  for repetition in $(seq 1 "$REPETITIONS"); do
    run_cell B "$repetition" classifier-target.llm-d-sc-scaleout.svc:50051
  done
fi

if [[ "$CONDITIONS" == *C* ]]; then
  scale_target classifier-target 0
  scale_target classifier-otel-candidate 1
  collectors_on
  for repetition in $(seq 1 "$REPETITIONS"); do
    run_cell C "$repetition" classifier-otel-candidate.llm-d-sc-scaleout.svc:50051
  done
fi

if [[ "$CONDITIONS" == *D* ]]; then
  scale_target classifier-target 0
  scale_target classifier-otel-candidate 1
  collectors_on
  set_trace_ratio 0.01
  for repetition in $(seq 1 "$REPETITIONS"); do
    run_cell D "$repetition" classifier-otel-candidate.llm-d-sc-scaleout.svc:50051
  done
fi

if [[ "$CONDITIONS" == *E* ]]; then
  scale_target classifier-target 0
  scale_target classifier-otel-candidate 1
  collectors_on
  set_trace_ratio 0.10
  for repetition in $(seq 1 "$REPETITIONS"); do
    run_cell E "$repetition" classifier-otel-candidate.llm-d-sc-scaleout.svc:50051
  done
fi

printf 'MATRIX_COMPLETE requests=%s concurrency=%s connections=%s repetitions=%s\n' \
  "$REQUESTS" "$CONCURRENCY" "$CONNECTIONS" "$REPETITIONS"
