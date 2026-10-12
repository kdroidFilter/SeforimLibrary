#!/usr/bin/env bash
# Decides, before anything is downloaded, which tag this run announces and on which
# channels. Writes tag, proceed, send and channels (JSON) to $GITHUB_OUTPUT.
set -euo pipefail
: "${EVENT_NAME:?}" "${GITHUB_REPOSITORY:?}" "${GITHUB_OUTPUT:?}" "${GITHUB_RUN_ID:?}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tag_re='^v[1-9][0-9]*-[0-9]{14}$'

stop() {
  echo "::notice::$1 — nothing to announce."
  { echo "proceed=false"; echo "tag=${tag:-}"; echo "send=false"; echo "channels=[]"; } >> "$GITHUB_OUTPUT"
  exit 0
}

# A release run's own handoff names the tag it published: pipeline-result-run-<id>-<attempt>.
# Sets $tag; not run in a subshell, so stop() ends the whole gate.
set_tag_published_by_run() {
  local handoff="pipeline-result-run-$WORKFLOW_RUN_ID-$WORKFLOW_RUN_ATTEMPT" dir err
  dir="$(mktemp -d)" err="$dir/err"
  if ! gh api "repos/$GITHUB_REPOSITORY/releases/tags/$handoff" --jq .tag_name > /dev/null 2> "$err"; then
    grep -q 'HTTP 404' "$err" || { echo "::error::cannot read $handoff: $(tr '\n' ' ' < "$err")"; exit 1; }
    stop "release run $WORKFLOW_RUN_ID:$WORKFLOW_RUN_ATTEMPT wrote no $handoff (it published nothing)"
  fi
  gh release download "$handoff" -R "$GITHUB_REPOSITORY" -D "$dir" -p pipeline-result.json -p pipeline-result.sha256
  [ "$(sha256sum "$dir/pipeline-result.json" | cut -d' ' -f1)" = "$(tr -d '[:space:]' < "$dir/pipeline-result.sha256")" ] || {
    echo "::error::$handoff: pipeline-result.json does not match its sha256"; exit 1; }
  local status
  status="$(jq -er --argjson run "$WORKFLOW_RUN_ID" --argjson attempt "$WORKFLOW_RUN_ATTEMPT" \
    'select(.child_run_id == $run and .child_run_attempt == $attempt) | .status' "$dir/pipeline-result.json")" || {
    echo "::error::$handoff names another run"; exit 1; }
  case "$status" in
    reused) stop "release run $WORKFLOW_RUN_ID:$WORKFLOW_RUN_ATTEMPT reused an existing release" ;;
    published) tag="$(jq -er .release_tag "$dir/pipeline-result.json")" ;;
    *) echo "::error::$handoff has unknown status '$status'"; exit 1 ;;
  esac
}

case "$EVENT_NAME" in
  workflow_dispatch)
    tag="${INPUT_TAG:?}" send=true
    if [ "${DRY_RUN:?}" = true ]; then send=false; fi ;;
  workflow_run) send=true; set_tag_published_by_run ;;
  *) echo "::error::unexpected event $EVENT_NAME"; exit 1 ;;
esac
[[ "$tag" =~ $tag_re ]] || { echo "::error::'$tag' is not a v<N>-<timestamp> release tag"; exit 1; }

state="$(gh api "repos/$GITHUB_REPOSITORY/releases/tags/$tag" --jq '"\(.draft) \(.prerelease)"')"
if [ "$state" != "false false" ]; then
  # Only a manual dispatch names the tag by hand; an automatic trigger just has nothing to do.
  [ "$EVENT_NAME" = workflow_dispatch ] && { echo "::error::$tag is not a published (non-draft, non-pre-release) release"; exit 1; }
  stop "$tag is not a published final release (draft prerelease: $state)"
fi

channels=()
IFS=',' read -r -a requested <<< "${INPUT_CHANNELS:-forum,yemot,release}"
for channel in "${requested[@]}"; do
  channel="$(tr -d '[:space:]' <<<"$channel")"
  [ -n "$channel" ] || continue
  case "$channel" in forum|yemot|release) ;; *) echo "::error::unknown channel '$channel' (allowed: forum,yemot,release)"; exit 1 ;; esac
  if [ "$send" = true ]; then
    # A plain assignment, so a failing check stops the gate instead of reading as "not sent".
    decision="$(TAG="$tag" CHANNEL="$channel" bash "$here/channel_decision.sh")"
    case "$decision" in
      skip) continue ;;
      proceed) ;;
      *) echo "::error::channel_decision.sh printed '$decision' for $channel"; exit 1 ;;
    esac
  fi
  channels+=("$channel")
done
[ "${#channels[@]}" -gt 0 ] || stop "every requested channel already announced $tag"

json="$(printf '%s\n' "${channels[@]}" | jq -Rsc 'split("\n") | map(select(. != ""))')"
echo "announcing $tag on $json (send=$send)"
{ echo "proceed=true"; echo "tag=$tag"; echo "send=$send"; echo "channels=$json"; } >> "$GITHUB_OUTPUT"
