# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

REST-сервис на Spring Boot для асинхронного копирования данных с удалённого HDFS-кластера на локальную файловую систему. Принимает JSON-запросы, копирует параллельно, возвращает request-id для отслеживания статуса.

- **Group**: `com.github.nlayna`
- **Package**: `com.github.nlayna.hadoopcopier`
- **Java**: 17 (Temurin)
- **Framework**: Spring Boot 3.5.x
- **Hadoop Client**: 3.3.6

## Environment Setup

```bash
export JAVA_HOME=/Users/nlayna/Library/Java/JavaVirtualMachines/temurin-17.0.15/Contents/Home
```

## Build Commands

```bash
./gradlew build          # сборка + тесты + jacoco + coverage verification (min 50%)
./gradlew test           # только тесты
./gradlew test --tests 'com.github.nlayna.hadoopcopier.SomeTest'          # один тест-класс
./gradlew test --tests 'com.github.nlayna.hadoopcopier.SomeTest.method'   # один тест-метод
./gradlew bootRun        # запуск приложения
./gradlew jacocoTestReport   # отчёт покрытия (build/reports/jacoco/)
```

## Key Dependencies

- **Lombok** — `compileOnly` + `annotationProcessor`
- **Hadoop Client API/Runtime** — HDFS-клиент с Kerberos-аутентификацией
- **fastjson2** — работа с JSON
- **Micrometer + Prometheus** — метрики через Spring Actuator
- **JUnit 5 + Mockito + Awaitility** — тесты

## Architecture

### REST API
- `POST /api/v1/copy` — принимает `CopyRequest` (namespace + items[]), возвращает `202` с `requestId`
- `GET /api/v1/copy/{requestId}` — статус задачи со всеми item-ами

### Пакеты
- `controller` — `CopyController` (REST), `GlobalExceptionHandler`
- `service` — бизнес-логика копирования:
  - `CopyTaskService` — управление задачами (in-memory ConcurrentHashMap), параллельный запуск через thread pool
  - `HdfsCopyService` — копирование файлов/директорий через Hadoop FileSystem API с fallback на ручной обход
  - `HdfsFileSystemFactory` — создание FileSystem с Kerberos-аутентификацией, кэширование Configuration per namespace
- `model` — DTO (`CopyRequest`, `CopyItem`) и доменные объекты (`CopyTask`, `CopyItemTask`, enums)
- `config` — `HadoopProperties`, `CopyProperties`, `AsyncConfig` (thread pool)

### Nexus-репозитории (см. `gradle.properties.example`)
- `nexus.repo.<имя>.url=<url>` — URL репозитория
- `nexus.repo.<имя>.credentials=<ENV_PREFIX>` — префикс env-переменных (по умолчанию `NEXUS`)
- Credentials из env: `<PREFIX>_USERNAME`, `<PREFIX>_PASSWORD`
- Разные серверы могут использовать разные credentials (например `NEXUS_*` и `OSC_*`)
- Без nexus.repo.* properties — используется только mavenCentral

### Конфигурация (application.yml)
- `hadoop.conf-basedir` — путь к конфигам hadoop (`{basedir}/{namespace}/core-site.xml`)
- `copy.thread-pool-size` — размер пула потоков для копирования

### Тестирование
- Controller тесты: `@WebMvcTest` + `@MockitoBean`
- Service тесты: Mockito + `MockedStatic<FileUtil>` для Hadoop static methods
- Async тесты: Awaitility для ожидания завершения
- Целевое покрытие: 80%, минимальное (enforced): 50%
