# netaminity-agent

[![Build status](https://img.shields.io/github/actions/workflow/status/exeteres/netaminity-agent/ci.yml)](https://github.com/exeteres/netaminity-agent/actions)

This project is a fork of [ekzhang/bore](https://github.com/ekzhang/bore) made mostly for [netaminity](https://github.com/exeteres/netaminity) project, while you can still use it as a standalone tool.
Refer to the upstream repository for the original project documentation and baseline behavior.

## Fork Changes

- Applies upstream PR [ekzhang/bore#189](https://github.com/ekzhang/bore/pull/189), adding configurable control-port support.
- Adds `--control-port` and `BORE_CONTROL_PORT` for both `bore local` and `bore server`.
- Adds opt-in tunnel integrity checks and HTTP health endpoints. See [Reliability](docs/RELIABILITY.md).
- Publishes container images to GitHub Container Registry under `ghcr.io/exeteres/netaminity-agent`.

## Versions

Fork releases use the version format `{original-version}-na.{counter}`. Current version: `0.6.0-na.4`

## Images

Container image:

```shell
ghcr.io/exeteres/netaminity-agent:0.6.0-na.4
```

Run the image:

```shell
docker run -it --init --rm --network host ghcr.io/exeteres/netaminity-agent:0.6.0-na.4 <ARGS>
```

Release images are built by `.github/workflows/docker.yml` for tags matching `v*.*.*-na.*`.
