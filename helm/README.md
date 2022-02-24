# MaxMQ Helm Chart

[![license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This chart is a lightweight way to deploy the MaxMQ on Kubernetes cluster using
the Helm package manager.

## Requirements

* [Kubernetes](https://kubernetes.io/) 1.23+
* [Helm](https://helm.sh/) 3.8.0+

## Installing the Chart

To install the chart with the release name `my-maxmq`:

#### From GitHub

```shell
git clone https://github.com/gsalomao/maxmq
helm install my-maxmq maxmq/helm
```

## Uninstalling the Chart

To uninstall/delete the `my-maxmq` deployment:

```shell
helm delete my-maxmq
```

## Configuration

The following table lists the configurable parameters of the MaxMQ chart and
their default values.

| Parameter             | Description                                             | Default        |
|-----------------------|---------------------------------------------------------|----------------|
| `nameOverride`        | Override the name of the chart                          | `""`           |
| `fullnameOverride`    | Override the full name of the chart                     | `""`           |
| `image.repository`    | The MaxMQ image name                                    | `maxmq`        |
| `image.pullPolicy`    | The image pull policy                                   | `IfNotPresent` |
| `image.pullSecrets `  | The image pull secrets                                  | `[]`           |
| `service.type`        | Kubernetes Service type                                 | `ClusterIP`    |
| `service.mqtt`        | Port for MQTT                                           | `1883`         |
| `service.annotations` | Service annotations                                     | `{}`           |
| `maxmqConfig`         | MaxMQ configuration                                     | `{}`           |
| `podAnnotations`      | Configurable annotations applied to all Kibana pods     | `{}`           |
| `podSecurityContext`  | Allows you to set the securityContext for the pod       | `{}`           |
| `securityContext`     | Allows you to set the securityContext for the container | `{}`           |
| `resources`           | CPU/Memory resource requests/limits                     | `{}`           |
| `nodeSelector`        | Node labels for pod assignment                          | `{}`           |
| `tolerations`         | Toleration labels for pod assignment                    | `[]`           |
| `affinity`            | Map of node/pod affinities                              | `{}`           |
