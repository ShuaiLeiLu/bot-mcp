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
previous_sha=
if [[ -n ${previous_release} ]]; then
    previous_sha=$(basename -- "${previous_release}")
    printf '%s\n' "${previous_sha}" > "${release_dir}/previous-sha"
fi

data_volume=bot-mcp_sub2api_mcp_data
backup_file=/data/predeploy-${release_sha}.db
backup_created=false
current_image=$(docker inspect --format '{{.Config.Image}}' sub2api-scheduler-mcp 2>/dev/null || true)
if [[ -n ${current_image} ]]; then
    docker volume inspect "${data_volume}" >/dev/null
    docker run --rm \
        --volume "${data_volume}:/data" \
        --entrypoint /opt/sub2api-mcp/venv/bin/python \
        "${current_image}" \
        -c 'import os,sqlite3,sys; src=sys.argv[1]; dst=sys.argv[2]; assert os.path.isfile(src), src; os.path.exists(dst) and os.remove(dst); source=sqlite3.connect(src); backup=sqlite3.connect(dst); source.backup(backup); backup.close(); source.close()' \
        /data/sub2api-mcp.db "${backup_file}"
    backup_created=true
fi

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
        compose stop || true
        if [[ ${backup_created} == true ]]; then
            if ! docker run --rm \
                --volume "${data_volume}:/data" \
                --entrypoint /opt/sub2api-mcp/venv/bin/python \
                "${current_image}" \
                -c 'import os,sqlite3,sys; src=sys.argv[1]; dst=sys.argv[2]; assert os.path.isfile(src), src; [os.remove(dst+s) for s in ("-wal","-shm") if os.path.exists(dst+s)]; backup=sqlite3.connect(src); target=sqlite3.connect(dst); backup.backup(target); target.close(); backup.close()' \
                "${backup_file}" /data/sub2api-mcp.db; then
                echo "database restore failed; previous release was not restarted" >&2
                return
            fi
        fi
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
compose build
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
