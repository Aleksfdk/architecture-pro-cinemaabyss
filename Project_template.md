## Изучите [README.md](README.md) файл и структуру проекта.

## Задание 1

1. 
[Контейнерная диаграмма C4, To-Be (PlantUML)](docs/c4/cinemaabyss-to-be-containers.puml)

![Контейнерная диаграмма To-Be](docs/c4/cinemaabyss-to-be-containers.png)

Целевая система разделена на домены со своими данными. Снаружи доступна только одна точка вызова.

Домены:

- **Movies** — каталог, метаданные, жанры и рейтинги. Сервис уже выделен из монолита.
- **Users** — регистрация и профили.
- **Payments** — проведение и статус оплаты.
- **Subscriptions** — план, срок и продление подписки.
- **Events** — интеграционный сервис: принимает события Movie, User и Payment, публикует их в Kafka и сам читает топики.

Единая точка вызова — **API Gateway (Proxy)** на порту 8000. Зритель обращается только к нему, шлюз маршрутизирует `/api/movies`, `/api/users`, `/api/payments`, `/api/subscriptions` и `/api/events` в соответствующий домен. Пока идёт выделение сервисов, шлюз работает по паттерну Strangler Fig: фиче-флаг `GRADUAL_MIGRATION` и процент `MOVIES_MIGRATION_PERCENT` постепенно переносят трафик фильмов с монолита на Movies. В целевой схеме монолита нет.

Интеграция:

- синхронные запросы идут только через API Gateway;
- изменения доменов публикуются в Kafka (`movie-events`, `user-events`, `payment-events`);
- Events читает все три топика и обрабатывает сообщения;
- Subscriptions читает `payment-events` и продлевает подписку без прямого вызова Payments.

У каждого бизнес-домена своя база PostgreSQL. Общей базы и прямых вызовов между доменами в обход шлюза и Kafka нет.


## Задание 2

### 1. Proxy
Команда КиноБездны уже выделила сервис метаданных о фильмах movies и вам необходимо реализовать бесшовный переход с применением паттерна Strangler Fig в части реализации прокси-сервиса (API Gateway), с помощью которого можно будет постепенно переключать траффик, используя фиче-флаг.


Реализуйте сервис на любом языке программирования в ./src/microservices/proxy.
Конфигурация для запуска сервиса через docker-compose уже добавлена
```yaml
  proxy-service:
    build:
      context: ./src/microservices/proxy
      dockerfile: Dockerfile
    container_name: cinemaabyss-proxy-service
    depends_on:
      - monolith
      - movies-service
      - events-service
    ports:
      - "8000:8000"
    environment:
      PORT: 8000
      MONOLITH_URL: http://monolith:8080
      #монолит
      MOVIES_SERVICE_URL: http://movies-service:8081 #сервис movies
      EVENTS_SERVICE_URL: http://events-service:8082 
      GRADUAL_MIGRATION: "true" # вкл/выкл простого фиче-флага
      MOVIES_MIGRATION_PERCENT: "50" # процент миграции
    networks:
      - cinemaabyss-network
```

- После реализации запустите postman тесты - они все должны быть зеленые.
- Отправьте запросы к API Gateway:
   ```bash
   curl http://localhost:8000/api/movies
   ```
- Протестируйте постепенный переход, изменив переменную окружения MOVIES_MIGRATION_PERCENT в файле docker-compose.yml.

### 2. Kafka
 Вам как архитектуру нужно также проверить гипотезу насколько просто реализовать применение Kafka в данной архитектуре.

Для этого нужно сделать MVP сервис events, который будет при вызове API создавать и сам же читать сообщения в топике Kafka.

    - Разработайте сервис на любом языке программирования с consumer'ами и producer'ами.
    - Реализуйте простой API, при вызове которого будут создаваться события User/Payment/Movie и обрабатываться внутри сервиса с записью в лог
    - Добавьте в docker-compose новый сервис, kafka там уже есть

Необходимые тесты для проверки этого API вызываются при запуске npm run test:local из папки tests/postman 
Приложите скриншот тестов и скриншот состояния топиков Kafka http://localhost:8090 

### Результат

Прокси и events написаны на Node.js.

- `src/microservices/proxy` — API Gateway. Пока `GRADUAL_MIGRATION=true`, доля `MOVIES_MIGRATION_PERCENT` запросов `/api/movies` уходит в movies, остальные в монолит. Если флаг выключен, фильмы уходят в movies целиком. Users, payments и subscriptions идут в монолит. Заголовок ответа `X-Upstream` показывает, какой сервис ответил.
- `src/microservices/events` — producer и три consumer'а на топики `movie-events`, `user-events`, `payment-events`. `POST /api/events/movie|user|payment` публикует событие и возвращает `status: success`. Тот же сервис читает сообщение и пишет обработку в лог.
- Сервис events в `docker-compose.yml` уже был описан, отдельно добавлять его не пришлось. Монолит и movies теперь стартуют после healthy Postgres.

Проверка процента миграции, 20 запросов `GET /api/movies`:

- `MOVIES_MIGRATION_PERCENT=0` — 20 ответов от монолита
- `MOVIES_MIGRATION_PERCENT=100` — 20 ответов от movies-service
- `MOVIES_MIGRATION_PERCENT=50` — 11 от movies-service и 9 от монолита

`npm run test:local`: 22 запроса, 42 проверки, 0 падений.

![Postman-тесты](docs/assignment-2/postman-tests.png)

![Топики Kafka](docs/assignment-2/kafka-topics.png)


## Задание 3

Команда начала переезд в Kubernetes для лучшего масштабирования и повышения надежности. 
Вам, как архитектору осталось самое сложное:
 - реализовать CI/CD для сборки прокси сервиса
 - реализовать необходимые конфигурационные файлы для переключения трафика.


### CI/CD

 В папке .github/worflows доработайте деплой новых сервисов proxy и events в docker-build-push.yml , чтобы api-tests при сборке отрабатывали корректно при отправке коммита в вашу новую ветку.

Нужно доработать 
```yaml
on:
  push:
    branches: [ main ]
    paths:
      - 'src/**'
      - '.github/workflows/docker-build-push.yml'
  release:
    types: [published]
```
и добавить необходимые шаги в блок
```yaml
jobs:
  build-and-push:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write

    steps:
      - name: Checkout repository
        uses: actions/checkout@v3

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v2

      - name: Log in to the Container registry
        uses: docker/login-action@v2
        with:
          registry: ${{ env.REGISTRY }}
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

```
Как только сборка отработает и в github registry появятся ваши образы, можно переходить к блоку настройки Kubernetes
Успешным результатом данного шага является "зеленая" сборка и "зеленые" тесты


### Proxy в Kubernetes

#### Шаг 1
Для деплоя в kubernetes необходимо залогиниться в docker registry Github'а.
1. Создайте Personal Access Token (PAT) https://github.com/settings/tokens . Создавайте class с правом read:packages
2. В src/kubernetes/*.yaml (event-service, monolith, movies-service и proxy-service)  отредактируйте путь до ваших образов 
```bash
 spec:
      containers:
      - name: events-service
        image: ghcr.io/ваш логин/имя репозитория/events-service:latest
```
3. Добавьте в секрет src/kubernetes/dockerconfigsecret.yaml в поле
```bash
 .dockerconfigjson: значение в base64 файла ~/.docker/config.json
```

4. Если в ~/.docker/config.json нет значения для аутентификации
```json
{
        "auths": {
                "ghcr.io": {
                       тут пусто
                }
        }
}
```
то выполните 

и добавьте

```json 
 "auth": "имя пользователя:токен в base64"
```

Чтобы получить значение в base64 можно выполнить команду
```bash
 echo -n ваш_логин:ваш_токен | base64
```

После заполнения config.json, также прогоните содержимое через base64

```bash
cat .docker/config.json | base64
```

и полученное значение добавляем в

```bash
 .dockerconfigjson: значение в base64 файла ~/.docker/config.json
```

#### Шаг 2

  Доработайте src/kubernetes/event-service.yaml и src/kubernetes/proxy-service.yaml

  - Необходимо создать Deployment и Service 
  - Доработайте ingress.yaml, чтобы можно было с помощью тестов проверить создание событий
  - Выполните дальшейшие шаги для поднятия кластера:

  1. Создайте namespace:
  ```bash
  kubectl apply -f src/kubernetes/namespace.yaml
  ```
  2. Создайте секреты и переменные
  ```bash
  kubectl apply -f src/kubernetes/configmap.yaml
  kubectl apply -f src/kubernetes/secret.yaml
  kubectl apply -f src/kubernetes/dockerconfigsecret.yaml
  kubectl apply -f src/kubernetes/postgres-init-configmap.yaml
  ```

  3. Разверните базу данных:
  ```bash
  kubectl apply -f src/kubernetes/postgres.yaml
  ```

  На этом этапе если вызвать команду
  ```bash
  kubectl -n cinemaabyss get pod
  ```
  Вы увидите

  NAME         READY   STATUS    
  postgres-0   1/1     Running   

  4. Разверните Kafka:
  ```bash
  kubectl apply -f src/kubernetes/kafka/kafka.yaml
  ```

  Проверьте, теперь должно быть запущено 3 пода, если что-то не так, то посмотрите логи
  ```bash
  kubectl -n cinemaabyss logs имя_пода (например - kafka-0)
  ```

  5. Разверните монолит:
  ```bash
  kubectl apply -f src/kubernetes/monolith.yaml
  ```
  6. Разверните микросервисы:
  ```bash
  kubectl apply -f src/kubernetes/movies-service.yaml
  kubectl apply -f src/kubernetes/events-service.yaml
  ```
  7. Разверните прокси-сервис:
  ```bash
  kubectl apply -f src/kubernetes/proxy-service.yaml
  ```

  После запуска и поднятия подов вывод команды 
  ```bash
  kubectl -n cinemaabyss get pod
  ```

  Будет наподобие такого

  NAME                              READY   STATUS    

  events-service-7587c6dfd5-6whzx   1/1     Running  

  kafka-0                           1/1     Running   

  monolith-8476598495-wmtmw         1/1     Running  

  movies-service-6d5697c584-4qfqs   1/1     Running  

  postgres-0                        1/1     Running  

  proxy-service-577d6c549b-6qfcv    1/1     Running  

  zookeeper-0                       1/1     Running 

  8. Добавим ingress

  - добавьте аддон
  ```bash
  minikube addons enable ingress
  ```
  ```bash
  kubectl apply -f src/kubernetes/ingress.yaml
  ```
  9. Добавьте в /etc/hosts
  127.0.0.1 cinemaabyss.example.com

  10. Вызовите
  ```bash
  minikube tunnel
  ```
  11. Вызовите https://cinemaabyss.example.com/api/movies
  Вы должны увидеть вывод списка фильмов
  Можно поэкспериментировать со значением   MOVIES_MIGRATION_PERCENT в src/kubernetes/configmap.yaml и убедится, что вызовы movies уходят полностью в новый сервис

  12. Запустите тесты из папки tests/postman
  ```bash
   npm run test:kubernetes
  ```
  Часть тестов с health-чек упадет, но создание событий отработает.
  Откройте логи event-service и сделайте скриншот обработки событий

#### Шаг 3
Добавьте сюда скриншота вывода при вызове https://cinemaabyss.example.com/api/movies и  скриншот вывода event-service после вызова тестов.

### Результат

CI собирает четыре образа и пушит их в GHCR. В `.github/workflows/docker-build-push.yml` ветка `cinema` добавлена в `push`, имена репозиториев приводятся к нижнему регистру, а после monolith и movies собираются `events-service` и `proxy-service`. `api-tests.yml` тоже запускается на `cinema`: `docker compose up -d --build` ждёт health монолита, movies, events и proxy, затем Newman гоняет коллекцию в сети `cinemaabyss-network`.

В Kubernetes у proxy и events есть Deployment и Service. Ingress `cinemaabyss.example.com` отправляет `/api/events` в events-service:8082, остальной трафик — в proxy-service:8000. Образы: `ghcr.io/aleksfdk/architecture-pro-cinemaabyss/{monolith,movies-service,events-service,proxy-service}:latest`.

Проверка `MOVIES_MIGRATION_PERCENT` в `src/kubernetes/configmap.yaml` при `GRADUAL_MIGRATION=true`, заголовок `X-Upstream`:

- `0` — все ответы `GET /api/movies` приходят от монолита
- `100` — все ответы приходят от movies-service

В configmap оставлено значение `100`. Вызов `http://cinemaabyss.example.com/api/movies` возвращает список фильмов и заголовок `x-upstream: movies-service`.

`npm run test:kubernetes`: 22 запроса, 42 проверки, 0 падений. Health-check монолита, movies, events и proxy тоже зелёные: ingress и proxy доводят `/health`, `/api/movies/health` и `/api/events/health` до нужных сервисов. В логе events-service видны `produced` и `consumed` для movie, user и payment.

![Список фильмов](docs/assignment-3/movies.png)

![Логи events-service](docs/assignment-3/events-logs.png)


## Задание 4
Для простоты дальнейшего обновления и развертывания вам как архитектуру необходимо так же реализовать helm-чарты для прокси-сервиса и проверить работу 

Для этого:
1. Перейдите в директорию helm и отредактируйте файл values.yaml

```yaml
# Proxy service configuration
proxyService:
  enabled: true
  image:
    repository: ghcr.io/db-exp/cinemaabysstest/proxy-service
    tag: latest
    pullPolicy: Always
  replicas: 1
  resources:
    limits:
      cpu: 300m
      memory: 256Mi
    requests:
      cpu: 100m
      memory: 128Mi
  service:
    port: 80
    targetPort: 8000
    type: ClusterIP
```

- Вместо ghcr.io/db-exp/cinemaabysstest/proxy-service напишите свой путь до образа для всех сервисов
- для imagePullSecret проставьте свое значение (скопируйте из конфигурации kubernetes)
  ```yaml
  imagePullSecrets:
      dockerconfigjson: ewoJImF1dGhzIjogewoJCSJnaGNyLmlvIjogewoJCQkiYXV0aCI6ICJaR0l0Wlhod09tZG9jRjl2UTJocVZIa3dhMWhKVDIxWmFVZHJOV2hRUW10aFVXbFZSbTVaTjJRMFNYUjRZMWM9IgoJCX0KCX0sCgkiY3JlZHNTdG9yZSI6ICJkZXNrdG9wIiwKCSJjdXJyZW50Q29udGV4dCI6ICJkZXNrdG9wLWxpbnV4IiwKCSJwbHVnaW5zIjogewoJCSIteC1jbGktaGludHMiOiB7CgkJCSJlbmFibGVkIjogInRydWUiCgkJfQoJfSwKCSJmZWF0dXJlcyI6IHsKCQkiaG9va3MiOiAidHJ1ZSIKCX0KfQ==
  ```

2. В папке ./templates/services заполните шаблоны для proxy-service.yaml и events-service.yaml (опирайтесь на свою kubernetes конфигурацию - смысл helm'а сделать шаблоны для быстрого обновления и установки)

```yaml
template:
    metadata:
      labels:
        app: proxy-service
    spec:
      containers:
       Тут ваша конфигурация
```

3. Проверьте установку
Сначала удалим установку руками

```bash
kubectl delete all --all -n cinemaabyss
kubectl delete  namespace cinemaabyss
```
Запустите 
```bash
helm install cinemaabyss .\src\kubernetes\helm --namespace cinemaabyss --create-namespace
```
Если в процессе будет ошибка
```code
[2025-04-08 21:43:38,780] ERROR Fatal error during KafkaServer startup. Prepare to shutdown (kafka.server.KafkaServer)
kafka.common.InconsistentClusterIdException: The Cluster ID OkOjGPrdRimp8nkFohYkCw doesn't match stored clusterId Some(sbkcoiSiQV2h_mQpwy05zQ) in meta.properties. The broker is trying to join the wrong cluster. Configured zookeeper.connect may be wrong.
```

Проверьте развертывание:
```bash
kubectl get pods -n cinemaabyss
minikube tunnel
```

Потом вызовите 
https://cinemaabyss.example.com/api/movies
и приложите скриншот развертывания helm и вывода https://cinemaabyss.example.com/api/movies

### Результат

Чар `src/kubernetes/helm` ставит весь стенд одной командой. В `values.yaml` образы monolith, movies, events и proxy указывают на `ghcr.io/aleksfdk/architecture-pro-cinemaabyss/...:latest`, `pullPolicy` — `IfNotPresent`, секрет `imagePullSecrets.dockerconfigjson` совпадает с `src/kubernetes/dockerconfigsecret.yaml`.

Шаблоны `templates/services/proxy-service.yaml` и `templates/services/events-service.yaml` повторяют рабочие Deployment и Service. Proxy слушает 8000, Service публикует порт 80, переменные Strangler Fig берёт из ConfigMap. Events публикует и читает Kafka по `kafka:9092`. Ingress отправляет `/api/events` в events-service:8082, остальной трафик — в proxy-service:80. В ConfigMap адрес movies — `http://movies-service:8081`, добавлен `EVENTS_SERVICE_URL`.

Ручная установка удалена (`kubectl delete all --all -n cinemaabyss` и namespace). Вместе с namespace ушли старые PVC, поэтому Kafka поднялась на чистых данных.

```bash
helm install cinemaabyss ./src/kubernetes/helm --namespace cinemaabyss --create-namespace
```

Релиз `cinemaabyss` в статусе `deployed`, ревизия 1. Все семь подов `1/1 Running`. `http://cinemaabyss.example.com/api/movies` возвращает список фильмов и заголовок `x-upstream: movies-service` при `MOVIES_MIGRATION_PERCENT=100`.

![Helm-релиз](docs/assignment-4/helm-deploy.png)

![Список фильмов после Helm](docs/assignment-4/movies.png)


# Задание 5
Компания планирует активно развиваться и для повышения надежности, безопасности, реализации сетевых паттернов типа Circuit Breaker и канареечного деплоя вам как архитектору необходимо развернуть istio и настроить circuit breaker для monolith и movies сервисов.

```bash

helm repo add istio https://istio-release.storage.googleapis.com/charts
helm repo update

helm install istio-base istio/base -n istio-system --set defaultRevision=default --create-namespace
helm install istio-ingressgateway istio/gateway -n istio-system
helm install istiod istio/istiod -n istio-system --wait

helm install cinemaabyss .\src\kubernetes\helm --namespace cinemaabyss --create-namespace

kubectl label namespace cinemaabyss istio-injection=enabled --overwrite

kubectl get namespace -L istio-injection

kubectl apply -f .\src\kubernetes\circuit-breaker-config.yaml -n cinemaabyss

```

Тестирование

# fortio
```bash
kubectl apply -f https://raw.githubusercontent.com/istio/istio/release-1.25/samples/httpbin/sample-client/fortio-deploy.yaml -n cinemaabyss
```

# Get the fortio pod name
```bash
FORTIO_POD=$(kubectl get pod -n cinemaabyss | grep fortio | awk '{print $1}')

kubectl exec -n cinemaabyss $FORTIO_POD -c fortio -- fortio load -c 50 -qps 0 -n 500 -loglevel Warning http://movies-service:8081/api/movies
```
Например,

```bash
kubectl exec -n cinemaabyss fortio-deploy-b6757cbbb-7c9qg  -c fortio -- fortio load -c 50 -qps 0 -n 500 -loglevel Warning http://movies-service:8081/api/movies
```

Вывод будет типа такого

```bash
IP addresses distribution:
10.106.113.46:8081: 421
Code 200 : 79 (15.8 %)
Code 500 : 22 (4.4 %)
Code 503 : 399 (79.8 %)
```
Можно еще проверить статистику

```bash
kubectl exec -n cinemaabyss fortio-deploy-b6757cbbb-7c9qg -c istio-proxy -- pilot-agent request GET stats | grep movies-service | grep pending
```

И там смотрим 

```bash
cluster.outbound|8081||movies-service.cinemaabyss.svc.cluster.local;.upstream_rq_pending_total: 311 - столько раз срабатывал circuit breaker
You can see 21 for the upstream_rq_pending_overflow value which means 21 calls so far have been flagged for circuit breaking.
```

Приложите скриншот работы circuit breaker'а

Удаляем все
```bash
istioctl uninstall --purge
kubectl delete namespace istio-system
kubectl delete all --all -n cinemaabyss
kubectl delete namespace cinemaabyss
```
