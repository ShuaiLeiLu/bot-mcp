#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

release_sha=${1:?release SHA is required}
archive_path=${2:?archive path is required}
expected_sha256=${3:?archive checksum is required}

if [[ ! ${release_sha} =~ ^[0-9a-f]{40}$ ]]; then
    echo "invalid release SHA" >&2
    exit 2
fi
if [[ ! ${expected_sha256} =~ ^[0-9a-f]{64}$ ]]; then
    echo "invalid archive checksum" >&2
    exit 2
fi

base_dir=/opt/bot-mcp
incoming_dir=${base_dir}/incoming
releases_dir=${base_dir}/releases
shared_dir=${base_dir}/shared

mkdir -p "${incoming_dir}" "${releases_dir}" "${shared_dir}"
archive_path=$(realpath -m -- "${archive_path}")
case "${archive_path}" in
    "${incoming_dir}"/*) ;;
    *) echo "archive must be inside ${incoming_dir}" >&2; exit 2 ;;
esac

printf '%s  %s\n' "${expected_sha256}" "${archive_path}" | sha256sum --check --status

release_dir=${releases_dir}/${release_sha}
if [[ ! -d ${release_dir} ]]; then
    temporary_dir=${releases_dir}/.${release_sha}.tmp.$$
    mkdir -p "${temporary_dir}"
    tar -xzf "${archive_path}" -C "${temporary_dir}" --no-same-owner
    for required in compose.yaml compose.deploy.yaml Dockerfile pyproject.toml uv.lock; do
        if [[ ! -f ${temporary_dir}/${required} ]]; then
            echo "release archive is missing ${required}" >&2
            exit 3
        fi
    done
    mv -- "${temporary_dir}" "${release_dir}"
fi

if [[ ! -f ${shared_dir}/.env ]]; then
    echo "${shared_dir}/.env is not configured" >&2
    exit 4
fi
ln -sfn -- "${shared_dir}/.env" "${release_dir}/.env"
printf 'SUB2API_MCP_IMAGE_TAG=%s\n' "${release_sha}" > "${release_dir}/.release.env"

previous_release=$(readlink -f -- "${base_dir}/current" 2>/dev/null || true)

compose() {
    docker compose \
        --project-name bot-mcp \
        --env-file "${shared_dir}/.env" \
        --env-file "${release_dir}/.release.env" \
        --file "${release_dir}/compose.yaml" \
        --file "${release_dir}/compose.deploy.yaml" \
        "$@"
}

rollback() {
    if [[ -n ${previous_release} && -d ${previous_release} && -f ${previous_release}/.release.env ]]; then
        docker compose \
            --project-name bot-mcp \
            --env-file "${shared_dir}/.env" \
            --env-file "${previous_release}/.release.env" \
            --file "${previous_release}/compose.yaml" \
            --file "${previous_release}/compose.deploy.yaml" \
            up -d --remove-orphans || true
    fi
}
trap rollback ERR

compose config --quiet
compose build --pull
compose up -d --remove-orphans

healthy=false
for _ in $(seq 1 60); do
    status=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' sub2api-scheduler-mcp 2>/dev/null || true)
    if [[ ${status} == healthy ]]; then
        healthy=true
        break
    fi
    if [[ ${status} == unhealthy || ${status} == exited || ${status} == dead ]]; then
        break
    fi
    sleep 2
done
if [[ ${healthy} != true ]]; then
    docker logs --tail 120 sub2api-scheduler-mcp >&2 || true
    exit 5
fi

temporary_link=${base_dir}/.current.${release_sha}
ln -sfn -- "${release_dir}" "${temporary_link}"
mv -Tf -- "${temporary_link}" "${base_dir}/current"
printf '%s\n' "${release_sha}" > "${base_dir}/deployed-sha"

trap - ERR
rm -f -- "${archive_path}"
echo "deployed ${release_sha}"
