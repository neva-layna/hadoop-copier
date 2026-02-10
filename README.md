# hadoop-copier

REST-сервис для асинхронного копирования данных с удалённого HDFS-кластера на локальную файловую систему.

## Возможности

- Асинхронное копирование файлов и директорий с HDFS
- Параллельное выполнение нескольких операций копирования
- Отслеживание статуса задач по request-id
- Kerberos-аутентификация
- Логирование скорости копирования

## Требования

- JDK 17
- Kerberos (для аутентификации на HDFS-кластере)
- Конфигурационные файлы Hadoop (`core-site.xml`, `hdfs-site.xml`) для каждого namespace

## Сборка и запуск

```bash
export JAVA_HOME=/path/to/jdk17

./gradlew build
./gradlew bootRun
```

## Конфигурация

`application.yml`:

| Параметр | Описание | По умолчанию |
|---|---|---|
| `hadoop.conf-basedir` | Путь к конфигам Hadoop (`{basedir}/{namespace}/core-site.xml`) | `$HADOOP_CONF_DIR` или `/etc/hadoop/conf` |
| `copy.thread-pool-size` | Размер пула потоков для копирования | `10` |

### Nexus-репозитории

Для подключения локальных Nexus-репозиториев добавьте в `gradle.properties`:

```properties
nexus.repo.releases.url=https://nexus.company.com/repository/maven-releases/
nexus.repo.snapshots.url=https://nexus.company.com/repository/maven-snapshots/

# Для Nexus с другими credentials:
nexus.repo.osc.url=https://osc-nexus.company.com/repository/maven-public/
nexus.repo.osc.credentials=OSC
```

Credentials задаются через переменные окружения:

```bash
export NEXUS_USERNAME=admin
export NEXUS_PASSWORD=secret

# Для OSC:
export OSC_USERNAME=osc_user
export OSC_PASSWORD=osc_pass
```

## API

### Создать задачу на копирование

```
POST /api/v1/copy
```

**Запрос:**

```bash
curl -X POST http://localhost:8080/api/v1/copy \
  -H "Content-Type: application/json" \
  -d '{
    "namespace": "nameservice1",
    "items": [
        {
            "hdfsPath": "/data/custom/spark/result1",
            "localPath": "/tmp/res1"
        },
        {
            "hdfsPath": "/data/custom/spark/result2",
            "localPath": "/tmp/res2"
        }
    ]
}'
```

**Ответ** `202 Accepted`:

```json
{
    "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

### Проверить статус задачи

```
GET /api/v1/copy/{requestId}
```

**Запрос:**

```bash
curl http://localhost:8080/api/v1/copy/a1b2c3d4-e5f6-7890-abcd-ef1234567890
```

**Ответ** `200 OK` (копирование в процессе):

```json
{
    "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "namespace": "nameservice1",
    "status": "IN_PROGRESS",
    "createdAt": "2026-02-07T12:00:00Z",
    "completedAt": null,
    "items": [
        {
            "hdfsPath": "/data/custom/spark/result1",
            "localPath": "/tmp/res1",
            "status": "COMPLETED",
            "bytesCopied": 1073741824,
            "durationMs": 15234,
            "speed": "67.18 MB/s",
            "errorMessage": null
        },
        {
            "hdfsPath": "/data/custom/spark/result2",
            "localPath": "/tmp/res2",
            "status": "IN_PROGRESS",
            "bytesCopied": 0,
            "durationMs": 0,
            "speed": "N/A",
            "errorMessage": null
        }
    ]
}
```

**Ответ** `200 OK` (копирование завершено):

```json
{
    "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "namespace": "nameservice1",
    "status": "COMPLETED",
    "createdAt": "2026-02-07T12:00:00Z",
    "completedAt": "2026-02-07T12:01:30Z",
    "items": [
        {
            "hdfsPath": "/data/custom/spark/result1",
            "localPath": "/tmp/res1",
            "status": "COMPLETED",
            "bytesCopied": 1073741824,
            "durationMs": 15234,
            "speed": "67.18 MB/s",
            "errorMessage": null
        },
        {
            "hdfsPath": "/data/custom/spark/result2",
            "localPath": "/tmp/res2",
            "status": "COMPLETED",
            "bytesCopied": 524288000,
            "durationMs": 8100,
            "speed": "61.73 MB/s",
            "errorMessage": null
        }
    ]
}
```

**Ответ** `200 OK` (частичная ошибка):

```json
{
    "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "namespace": "nameservice1",
    "status": "PARTIALLY_FAILED",
    "createdAt": "2026-02-07T12:00:00Z",
    "completedAt": "2026-02-07T12:01:30Z",
    "items": [
        {
            "hdfsPath": "/data/custom/spark/result1",
            "localPath": "/tmp/res1",
            "status": "COMPLETED",
            "bytesCopied": 1073741824,
            "durationMs": 15234,
            "speed": "67.18 MB/s",
            "errorMessage": null
        },
        {
            "hdfsPath": "/data/custom/spark/result2",
            "localPath": "/tmp/res2",
            "status": "FAILED",
            "bytesCopied": 0,
            "durationMs": 320,
            "speed": "N/A",
            "errorMessage": "Source path does not exist: /data/custom/spark/result2"
        }
    ]
}
```

**Ответ** `404 Not Found` — задача с таким requestId не найдена.

### Ошибки валидации

**Пустой namespace:**

```bash
curl -X POST http://localhost:8080/api/v1/copy \
  -H "Content-Type: application/json" \
  -d '{"items": [{"hdfsPath": "/data/res", "localPath": "/tmp/res"}]}'
```

```json
{"error": "namespace is required"}
```

**Пустой список items:**

```bash
curl -X POST http://localhost:8080/api/v1/copy \
  -H "Content-Type: application/json" \
  -d '{"namespace": "ns1", "items": []}'
```

```json
{"error": "items must not be empty"}
```

## Статусы задачи

| Статус | Описание |
|---|---|
| `PENDING` | Задача создана, ожидает выполнения |
| `IN_PROGRESS` | Копирование выполняется |
| `COMPLETED` | Все элементы скопированы успешно |
| `PARTIALLY_FAILED` | Часть элементов скопирована, часть с ошибкой |
| `FAILED` | Все элементы завершились с ошибкой |
