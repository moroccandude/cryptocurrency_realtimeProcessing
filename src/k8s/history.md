## Explanation of the Kubernetes Resources

**Namespace**: Created a dedicated namespace crypto-project to isolate your application resources.
Zookeeper: Deployed as a single-replica Deployment with a corresponding Service to expose port 2181.
Kafka:

Deployment with environment variables stored in a ConfigMap
Service to expose both internal (9092) and external (29092) ports
Added readiness probe to check if Kafka is ready
Using lifecycle hooks to create the cryptocurrency topic after startup


Cassandra:

Deployed as a StatefulSet (appropriate for databases) with persistent storage
Service to expose CQL and management ports
Added readiness probe to ensure it's properly initialized


Spark Master:

Deployment with the necessary environment variables
Service to expose master and web UI ports
Using ConfigMap volume to mount your consumer code


Spark Workers:

Deployment with 2 replicas (equivalent to your worker-1 and worker-2)
Using emptyDir volumes for logs and work directories
Configured with liveness probe to ensure workers are functioning


Fetcher:

Deployment with your custom container
Service to expose port 7000
Using ConfigMap to mount your connector.py file



Important Implementation Notes

ConfigMaps for Code: You need to create ConfigMaps containing your actual code:

For the connector.py in the fetcher service
For all the files in your consumer directory


Docker Images:

For your custom fetcher image, you'll need to:

Build and tag it locally (docker build -t fetcher:latest ./src/extractor)
Push it to a container registry if deploying to a remote cluster
Or use imagePullPolicy: Never for local Kubernetes (minikube, kind, etc.)




Persistent Storage:

For Cassandra, I've configured a PersistentVolumeClaim with 1Gi of storage
You may need to adjust the storage size based on your requirements
Ensure your Kubernetes cluster has a StorageClass available



Deployment Steps

Fill in the placeholder ConfigMaps with your actual code
Apply the manifests to your Kubernetes cluster:
```
bashkubectl apply -f 01-namespace.yaml
kubectl apply -f 02-configmap.yaml
kubectl apply -f 03-zookeeper.yaml
kubectl apply -f 04-kafka.yaml
kubectl apply -f 05-cassandra.yaml
kubectl apply -f 06-spark-master.yaml
kubectl apply -f 07-spark-worker.yaml
kubectl apply -f 08-fetcher.yaml
kubectl apply -f 09-configmaps-for-code.yaml
Do you need any clarification on specific parts of this migration? Would you like me to explain any of the Kubernetes concepts in more detail?

```
# 01-namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: crypto-project

---
# 02-configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-config
  namespace: crypto-project
data:
  KAFKA_BROKER_ID: "1"
  KAFKA_ZOOKEEPER_CONNECT: "zookeeper:2181"
  KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092"
  KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:9092,PLAINTEXT_HOST://kafka:29092"
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
  KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
  KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0"

---
# 03-zookeeper.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: zookeeper
  namespace: crypto-project
spec:
  replicas: 1
  selector:
    matchLabels:
      app: zookeeper
  template:
    metadata:
      labels:
        app: zookeeper
    spec:
      containers:
      - name: zookeeper
        image: confluentinc/cp-zookeeper:latest
        ports:
        - containerPort: 2181
        env:
        - name: ZOOKEEPER_CLIENT_PORT
          value: "2181"
        - name: ZOOKEEPER_TICK_TIME
          value: "2000"
---
apiVersion: v1
kind: Service
metadata:
  name: zookeeper
  namespace: crypto-project
spec:
  selector:
    app: zookeeper
  ports:
  - port: 2181
    targetPort: 2181

---
# 04-kafka.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka
  namespace: crypto-project
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      containers:
      - name: kafka
        image: confluentinc/cp-kafka:latest
        ports:
        - containerPort: 9092
        - containerPort: 29092
        envFrom:
        - configMapRef:
            name: kafka-config
        readinessProbe:
          exec:
            command:
            - sh
            - -c
            - kafka-topics --bootstrap-server localhost:9092 --list
          initialDelaySeconds: 20
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 10
        lifecycle:
          postStart:
            exec:
              command:
              - sh
              - -c
              - |
                echo 'Waiting for Kafka to start...';
                sleep 15;
                kafka-topics --create \
                  --topic cryptocurrency \
                  --bootstrap-server localhost:9092 \
                  --partitions 1 \
                  --replication-factor 1;
---
apiVersion: v1
kind: Service
metadata:
  name: kafka
  namespace: crypto-project
spec:
  selector:
    app: kafka
  ports:
  - name: internal
    port: 9092
    targetPort: 9092
  - name: external
    port: 29092
    targetPort: 29092

---
# 05-cassandra.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: cassandra
  namespace: crypto-project
spec:
  serviceName: cassandra
  replicas: 1
  selector:
    matchLabels:
      app: cassandra
  template:
    metadata:
      labels:
        app: cassandra
    spec:
      containers:
      - name: cassandra
        image: cassandra:latest
        ports:
        - containerPort: 7006
        - containerPort: 9042
        env:
        - name: CASSANDRA_CLUSTER_NAME
          value: "CryptoCluster"
        - name: CASSANDRA_DC
          value: "DC1"
        - name: CASSANDRA_RACK
          value: "Rack1"
        - name: CASSANDRA_NUM_TOKENS
          value: "256"
        - name: CASSANDRA_LISTEN_ADDRESS
          value: "auto"
        - name: CASSANDRA_BROADCAST_ADDRESS
          value: "cassandra"
        - name: CASSANDRA_BROADCAST_RPC_ADDRESS
          value: "cassandra"
        - name: CASSANDRA_ENDPOINT_SNITCH
          value: "GossipingPropertyFileSnitch"
        volumeMounts:
        - name: cassandra-data
          mountPath: /var/lib/cassandra
        readinessProbe:
          exec:
            command:
            - nodetool
            - status
          initialDelaySeconds: 60
          periodSeconds: 30
          timeoutSeconds: 10
          failureThreshold: 5
  volumeClaimTemplates:
  - metadata:
      name: cassandra-data
    spec:
      accessModes: [ "ReadWriteOnce" ]
      resources:
        requests:
          storage: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: cassandra
  namespace: crypto-project
spec:
  selector:
    app: cassandra
  ports:
  - name: cassandra-port
    port: 7006
    targetPort: 7006
  - name: cql
    port: 9042
    targetPort: 9042

---
# 06-spark-master.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-master
  namespace: crypto-project
spec:
  replicas: 1
  selector:
    matchLabels:
      app: spark-master
  template:
    metadata:
      labels:
        app: spark-master
    spec:
      securityContext:
        runAsUser: 0
        runAsGroup: 0
      containers:
      - name: spark-master
        image: bitnami/spark:3.5
        ports:
        - containerPort: 7077
        - containerPort: 8080
        env:
        - name: SPARK_MODE
          value: "master"
        - name: SPARK_RPC_AUTHENTICATION_ENABLED
          value: "no"
        - name: SPARK_RPC_ENCRYPTION_ENABLED
          value: "no"
        - name: SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED
          value: "no"
        - name: SPARK_SSL_ENABLED
          value: "no"
        - name: SPARK_MASTER_OPTS
          value: "-Dspark.deploy.defaultCores=1"
        - name: SPARK_DAEMON_MEMORY
          value: "2G"
        volumeMounts:
        - name: consumer-code
          mountPath: /opt/bitnami/spark/venv/src/
        command: ["/bin/bash", "-c"]
        args:
        - |
          mkdir -p /opt/bitnami/spark/venv/src/checkpoint
          pip install --no-cache-dir -r /opt/bitnami/spark/venv/src/requirements.txt
          /opt/bitnami/spark/sbin/start-master.sh
          tail -f /opt/bitnami/spark/logs/spark-*-org.apache.spark.deploy.master.Master-*.out
      volumes:
      - name: consumer-code
        configMap:
          name: consumer-code
---
apiVersion: v1
kind: Service
metadata:
  name: spark-master
  namespace: crypto-project
spec:
  selector:
    app: spark-master
  ports:
  - name: master
    port: 7077
    targetPort: 7077
  - name: web-ui
    port: 8080
    targetPort: 8080

---
# 07-spark-worker.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-worker
  namespace: crypto-project
spec:
  replicas: 2
  selector:
    matchLabels:
      app: spark-worker
  template:
    metadata:
      labels:
        app: spark-worker
    spec:
      containers:
      - name: spark-worker
        image: bitnami/spark:3.5
        env:
        - name: SPARK_MODE
          value: "worker"
        - name: SPARK_MASTER_URL
          value: "spark://spark-master:7077"
        - name: SPARK_WORKER_MEMORY
          value: "1g"
        - name: SPARK_WORKER_CORES
          value: "1"
        - name: SPARK_RPC_AUTHENTICATION_ENABLED
          value: "no"
        - name: SPARK_RPC_ENCRYPTION_ENABLED
          value: "no"
        - name: SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED
          value: "no"
        - name: SPARK_SSL_ENABLED
          value: "no"
        - name: SPARK_WORKER_OPTS
          value: "-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=3600"
        volumeMounts:
        - name: spark-worker-logs
          mountPath: /opt/bitnami/spark/logs
        - name: spark-worker-work
          mountPath: /opt/bitnami/spark/work
        livenessProbe:
          exec:
            command:
            - sh
            - -c
            - ps aux | grep "[o]rg.apache.spark.deploy.worker.Worker" || exit 1
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
      volumes:
      - name: spark-worker-logs
        emptyDir: {}
      - name: spark-worker-work
        emptyDir: {}

---
# 08-fetcher.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fetcher
  namespace: crypto-project
spec:
  replicas: 1
  selector:
    matchLabels:
      app: fetcher
  template:
    metadata:
      labels:
        app: fetcher
    spec:
      containers:
      - name: fetcher
        image: fetcher:latest
        imagePullPolicy: Never  # For locally built images
        ports:
        - containerPort: 7000
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka:9092"
        volumeMounts:
        - name: fetcher-code
          mountPath: /app/src/connector.py
          subPath: connector.py
      volumes:
      - name: fetcher-code
        configMap:
          name: fetcher-code
---
apiVersion: v1
kind: Service
metadata:
  name: fetcher
  namespace: crypto-project
spec:
  selector:
    app: fetcher
  ports:
  - port: 7000
    targetPort: 7000

---
# 09-configmaps-for-code.yaml
# NOTE: You'll need to create this ConfigMap with your actual code
apiVersion: v1
kind: ConfigMap
metadata:
  name: fetcher-code
  namespace: crypto-project
data:
  connector.py: |
    # Place your connector.py content here
    # This is a placeholder and needs to be replaced with your actual code

---
apiVersion: v1
kind: ConfigMap
metadata:
  name: consumer-code
  namespace: crypto-project
data:
  # Add all your consumer code files here
  requirements.txt: |
    # Place your requirements.txt content here
    # This is a placeholder and needs to be replaced with your actual requirements