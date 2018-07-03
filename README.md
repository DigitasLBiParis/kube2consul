kube2consul
===========

Install
-------

Get the binary directly from GitHub releases or download the code and compile it with `make`. It requires Go 1.8 or later.


Usage
-----
Kube2consul runs in a kubernetes cluster by default. It is able to work out of cluster if an absolute path to the kubeconfig file is provided.

| Command line option | Environment option   | Default value             |
| ------------------- | -------------------- | ------------------------- |
| `-consul-api`       | `K2C_CONSUL_API`     | `"127.0.0.1:8500"`        |
| `-consul-tag`       | `K2C_CONSUL_TAG`     | `"kube2consul"`           |
| `-consul-token`     | `K2C_CONSUL_TOKEN`   | `""`                      |
| `-kubeconfig`       | `K2C_KUBECONFIG`     | `""`                      |
| `-kubernetes-api`   | `K2C_KUBERNETES_API` | `""`                      |
| `-resync-period`    | `K2C_RESYNC_PERIOD`  | `30`                      |
| `-explicit`         | `K2C_EXPLICIT`       | `false`                   |
| `-debug`            | `K2C_DEBUG`          | `false`                   |

Kube2consul is able to detect any endpoint update on k8s and add/remove it to/from consul.
It can also read service annotations, below an example of kube service with kube2consul compatible annotations

```yaml
apiVersion: v1
kind: Service
metadata:
  name: nginx-svc
  annotations:
    SERVICE_80_NAME: nginx-http
    SERVICE_443_NAME: nginx-https
    SERVICE_443_TAGS: "enable_tls, key=value"
    SERVICE_8080_IGNORE: "true"
  labels:
    run: svc-nginx
spec:
  type: NodePort
  ports:
  - port: 80
    protocol: TCP
    name: http
  - port: 443
    protocol: TCP
    name: https
  - port: 8080
    protocol: TCP
    name: ignore
  selector:
    run: my-nginx
```
