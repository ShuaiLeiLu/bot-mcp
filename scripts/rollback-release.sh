#!/usr/bin/env bash
set -Eeuo pipefail

release_sha=${1:?release SHA is required}
if [[ ! ${release_sha} =~ ^[0-9a-f]{40}$ ]]; then
    echo "invalid release SHA" >&2
    exit 2
fi

base_dir=/opt/bot-mcp
shared_dir=${base_dir}/shared
release_dir=${base_dir}/releases/${release_sha}
if [[ ! -d ${release_dir} || ! -f ${release_dir}/.release.env || ! -f ${shared_dir}/.env ]]; then
    echo "requested release is unavailable" >&2
    exit 3
fi

docker compose \
    --project-name bot-mcp \
    --env-file "${shared_dir}/.env" \
    --env-file "${release_dir}/.release.env" \
    --file "${release_dir}/compose.yaml" \
    --file "${release_dir}/compose.deploy.yaml" \
    up -d --remove-orphans

for _ in $(seq 1 60); do
    status=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' sub2api-scheduler-mcp 2>/dev/null || true)
    if [[ ${status} == healthy ]]; then
        temporary_link=${base_dir}/.current.${release_sha}
        ln -sfn -- "${release_dir}" "${temporary_link}"
        mv -Tf -- "${temporary_link}" "${base_dir}/current"
        printf '%s\n' "${release_sha}" > "${base_dir}/deployed-sha"
        echo "rolled back to ${release_sha}"
        exit 0
    fi
    sleep 2
done

docker logs --tail 120 sub2api-scheduler-mcp >&2 || true
exit 4
