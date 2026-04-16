# K8s Deployment etcd Value — Protobuf Structure

Example object: `/registry/deployments/workloads/web-frontend` (3,491B)

Captured from a 5-node minikube cluster with realistic workloads (352 writes observed).

## Full Structure

```
Outer Wrapper (3,491B)
├── field 1: TypeMeta                           23B   NEVER changes
│   ├── field 1: apiVersion                      9B   "apps/v1"
│   └── field 2: kind                           12B   "Deployment"
├── field 2: Deployment                      3,460B
│   ├── field 1: ObjectMeta                  2,734B
│   │   ├── field 1:  name                      14B   NEVER changes       "web-frontend"
│   │   ├── field 2:  generateName               2B   NEVER changes       ""
│   │   ├── field 3:  namespace                  11B   NEVER changes       "workloads"
│   │   ├── field 4:  selfLink                    2B   NEVER changes       ""
│   │   ├── field 5:  uid                        38B   NEVER changes       "fd183980-..."
│   │   ├── field 6:  resourceVersion             2B   NEVER changes       ""
│   │   ├── field 7:  generation                  2B   changes 6.3%        varint=3
│   │   ├── field 8:  creationTimestamp           10B   NEVER changes
│   │   │   ├── field 1: seconds                  6B   varint=1775904786
│   │   │   └── field 2: nanos                    2B   varint=0
│   │   ├── field 12: annotations               784B   changes 1.7%        (x2 entries)
│   │   │   ├── [entry 1]                        40B
│   │   │   │   ├── field 1: key                 35B   "deployment.kubernetes.io/revision"
│   │   │   │   └── field 2: value                3B   "1"
│   │   │   └── [entry 2]                       744B
│   │   │       ├── field 1: key                 32B   "kubectl.kubernetes.io/last-applied-config..."
│   │   │       └── field 2: value              710B   (full JSON of last-applied-configuration)
│   │   └── field 17: managedFields            1,866B   changes 36.8%       (x3 entries)
│   │       ├── [entry 0]                        78B   NEVER changes       (100% cache hit)
│   │       │   ├── field 1: manager              9B   "kubectl"
│   │       │   ├── field 2: operation            8B   "Update"
│   │       │   ├── field 3: apiVersion           9B   "apps/v1"
│   │       │   ├── field 6: fieldsType          10B   "FieldsV1"
│   │       │   ├── field 7: fieldsV1            32B   {"f:spec":{"f:replicas":{}}}
│   │       │   └── field 8: subresource          7B   "scale"
│   │       ├── [entry 1]                     1,174B   NEVER changes       (100% cache hit)
│   │       │   ├── field 1: manager             27B   "kubectl-client-side-apply"
│   │       │   ├── field 2: operation            8B   "Update"
│   │       │   ├── field 3: apiVersion           9B   "apps/v1"
│   │       │   ├── field 4: time                10B   timestamp
│   │       │   ├── field 6: fieldsType          10B   "FieldsV1"
│   │       │   ├── field 7: fieldsV1          1,104B   (JSON field ownership - biggest single blob)
│   │       │   └── field 8: subresource          2B   ""
│   │       └── [entry 2]                       614B   changes 8.3%        (91.7% cache hit)
│   │           ├── field 1: manager             25B   "kube-controller-manager"
│   │           ├── field 2: operation            8B   "Update"
│   │           ├── field 3: apiVersion           9B   "apps/v1"
│   │           ├── field 4: time                10B   timestamp (THIS is what changes)
│   │           ├── field 6: fieldsType          10B   "FieldsV1"
│   │           ├── field 7: fieldsV1           540B   (JSON field ownership)
│   │           └── field 8: subresource          8B   "status"
│   ├── field 2: DeploymentSpec                 470B   changes ~0.1%
│   │   ├── field 1: replicas                     2B   varint=9
│   │   ├── field 2: selector                    23B
│   │   │   └── field 1: matchLabels             21B
│   │   │       ├── field 1: key                  5B   "app"
│   │   │       └── field 2: value               14B   "web-frontend"
│   │   ├── field 3: template                   398B
│   │   │   ├── field 1: ObjectMeta             128B
│   │   │   │   ├── field 1:  name                2B   ""
│   │   │   │   ├── field 2:  generateName        2B   ""
│   │   │   │   ├── field 3:  namespace           2B   ""
│   │   │   │   ├── field 4:  selfLink            2B   ""
│   │   │   │   ├── field 5:  uid                 2B   ""
│   │   │   │   ├── field 6:  resourceVersion     2B   ""
│   │   │   │   ├── field 7:  generation          2B   varint=0
│   │   │   │   ├── field 8:  creationTimestamp    2B   ""
│   │   │   │   ├── field 11: labels             54B   (x3: app, pod-template-hash, tier)
│   │   │   │   └── field 12: annotations        56B   (x2: prometheus.io/scrape, /port)
│   │   │   └── field 2: PodSpec                267B
│   │   │       ├── field 1: containers         197B
│   │   │       │   ├── field 1: name              7B   "nginx"
│   │   │       │   ├── field 2: image            12B   "nginx:1.27"
│   │   │       │   ├── field 6: ports            15B
│   │   │       │   ├── field 7: envFrom          38B   (x2 env vars)
│   │   │       │   ├── field 8: env              68B   (ENV, LOG_LEVEL)
│   │   │       │   ├── field 14: imagePullPolicy 14B   "IfNotPresent"
│   │   │       │   ├── field 18: terminationMsgPath  22B
│   │   │       │   └── field 20: terminationMsgPolicy 7B   "File"
│   │   │       ├── field 3: restartPolicy         8B   "Always"
│   │   │       ├── field 4: terminationGracePeriod 2B   varint=30
│   │   │       ├── field 6: dnsPolicy            14B   "ClusterFirst"
│   │   │       ├── field 14: securityContext       2B   ""
│   │   │       ├── field 19: schedulerName        20B   "default-scheduler"
│   │   │       └── ...other small fields         24B
│   │   ├── field 4: strategy                    35B
│   │   │   ├── field 1: type                    15B   "RollingUpdate"
│   │   │   └── field 2: rollingUpdate           18B   (maxSurge=2, maxUnavailable=1)
│   │   ├── field 5: minReadySeconds              2B   varint=0
│   │   ├── field 6: revisionHistoryLimit          2B   varint=10
│   │   ├── field 7: paused                        2B   varint=0
│   │   └── field 9: progressDeadlineSeconds       3B   varint=600
│   └── field 3: DeploymentStatus               253B   changes ~74%
│       ├── field 1: observedGeneration            2B   varint=3
│       ├── field 2: replicas                      2B   varint=10
│       ├── field 3: updatedReplicas               2B   varint=10
│       ├── field 4: availableReplicas             2B   varint=10
│       ├── field 5: unavailableReplicas           2B   varint=0
│       ├── field 6: conditions                  236B   (x2 entries)
│       │   ├── [Progressing]                    130B
│       │   │   ├── field 1: type                 13B   "Progressing"
│       │   │   ├── field 2: status                6B   "True"
│       │   │   ├── field 4: message              24B   "NewReplicaSetAvailable"
│       │   │   ├── field 5: reason               67B   "ReplicaSet ... has successfully progressed"
│       │   │   ├── field 6: lastUpdateTime       10B   timestamp
│       │   │   └── field 7: lastTransitionTime   10B   timestamp
│       │   └── [Available]                      106B
│       │       ├── field 1: type                 11B   "Available"
│       │       ├── field 2: status                7B   "False"
│       │       ├── field 4: message              18B   "MinimumReplicasAvailable"
│       │       ├── field 5: reason               42B   ...
│       │       ├── field 6: lastUpdateTime       10B   timestamp
│       │       └── field 7: lastTransitionTime   10B   timestamp
│       ├── field 7: readyReplicas                 2B   varint=10
│       └── field 9: collisionCount                2B   varint=0
├── field 3: contentEncoding                      2B   NEVER changes       ""
└── field 4: contentType                          2B   NEVER changes       ""
```

## Recommended Cache Paths

Configuration format: list of field paths in `"x.x.x"` format. The encoder parses
the protobuf value, walks to the target field, and checks `(key, field_path, raw_bytes)`
against cache. If matched, replace with a cache reference.

For repeated fields (like `managedFields`), each entry is checked independently.

| Cache path | What                          | Size    | Cache hit rate | Avg savings/write |
|------------|-------------------------------|---------|----------------|-------------------|
| `2.2`      | DeploymentSpec                | 470B    | 99.9%          | ~470B             |
| `2.1.17`   | managedFields (each entry)    | 1,866B  | entry 0: 100%, entry 1: 100%, entry 2: 91.7% | ~1,808B |
| `2.1.12`   | annotations (each entry)      | 784B    | 98.3%          | ~771B             |
| **Total**  |                               | **3,120B** |             | **~3,049B (87%)** |

## Key Observations

1. **Values are protobuf all the way down** — k8s serializes objects as protobuf with a `k8s\x00` 4-byte prefix, then a wrapper envelope containing TypeMeta + the actual object.
2. **ObjectMeta dominates** — 79% of the Deployment value. Within it, `managedFields` (1,866B) and `annotations` (784B) are the biggest chunks and rarely change.
3. **managedFields entry 1** (`kubectl-client-side-apply`, 1,174B) is the single largest cacheable blob and never changes across 352 observed writes.
4. **DeploymentSpec** (470B) is essentially immutable — only changes when someone explicitly edits the deployment spec (e.g., replica count via `kubectl scale`).
5. **DeploymentStatus** (253B) changes most frequently (~74% of writes) but is small — not worth caching at the top level, though individual conditions could be cached at a deeper level.

## Design: Generic Field-Path Caching

The caching module should be **generic and configurable**, not hardcoded to k8s Deployment structure:

- Different k8s object types (Pods, ReplicaSets, Leases, CRDs) have different hot fields at different nesting depths
- The protobuf wire format is self-describing (field number + wire type), so traversal to any depth works without schema knowledge
- Users provide a list of field paths (e.g., `["2.2", "2.1.17", "2.1.12"]`) and the encoder handles the rest
