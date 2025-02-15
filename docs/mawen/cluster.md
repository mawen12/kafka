# cluster

## 单节点启动

### 1.启动zookeeper
```cmd
bin\windows\zookeeper-server-start.cmd config\zookeeper.properties
```

### 2.启动kafka

```cmd
bin\windows\kafka-server-start.cmd config\server.properties
```

### 3.创建主题

```cmd
bin\windows\kafka-topics.cmd --create --topic quickstart-events --bootstrap-server localhost:9092
```