# Relay

[![ci](https://github.com/SouradipG/relay/actions/workflows/ci.yml/badge.svg)](https://github.com/SouradipG/relay/actions/workflows/ci.yml)

A gateway between many internal teams and several LLM providers. It streams chat answers, runs long jobs in the background, keeps each team inside its token budget, fails over when a provider has a bad day, and stops one noisy team from starving everyone else.

Java 25, Spring Boot 4, Spring AI 2, MongoDB, Redis.

## The problem

Once a company has a few teams building on LLMs, the same things break in the same order.

* **Providers push back.** They cap requests and tokens per minute and answer `429` when you go over. They also have outages, slow days and streams that die halfway.
* **Cost is only known afterwards.** The token count comes back with the response, so a budget enforced before the call is always an estimate.
* **One team can take everything.** A batch job that submits fifty thousand requests uses the whole provider quota, and another team's chat feature starts failing.
* **Streams are fragile.** Load balancers cut idle connections, proxies buffer the stream until the end, deploys kill open connections, and a user who closes the tab leaves the model generating tokens that are still billed.

Relay puts all of that in one place, so each team gets a simple API and none of them has to solve it alone.

## What it covers

### Sync and async, side by side

* **Chat over Server Sent Events.** For a person waiting on an answer. Everything that can be refused (no budget, no provider, instance full) is refused before the stream opens, as a normal JSON error with a status code. Failures after the first byte are reported as events, because the status is already `200`.
* **Jobs with `202 Accepted`.** For work the caller does not need to wait for: summarising a document, a batch of classifications. The job id comes back straight away, the result arrives by polling or by a signed webhook, and a provider outage becomes a delay instead of an error.

### Fairness and budgets

* **Token budget per tenant** in Redis: estimate, reserve, call, then settle with the real usage. Over estimates are refunded, under estimates are charged, and a tenant that overspent waits for the refill.
* **Weighted fair scheduling** of jobs across tenants, plus a cap on how many of one tenant's jobs run at once.

### Providers

* **Failover** in preference order, only before the first token.
* **Circuit breaker per provider**, so an outage does not make every call wait for a timeout first.
* **Adaptive concurrency per provider**: additive increase, halve on `429`.
* **Three timeouts on streams**: connect, first byte, and idle between chunks.
* **Spring AI adapter**, so any model Spring AI supports can sit behind the same budgets and breakers.

### Guardrails

* **PII redaction** of emails, card numbers (Luhn checked) and phone numbers before a prompt leaves.

### Durability

* **Leased jobs** with retries, exponential backoff with full jitter, and a dead letter state with the reason.
* **Webhooks** signed with HMAC and a timestamp, retried with backoff, parked after the last attempt and replayable.

## Running it

```bash
docker compose up -d                                    # MongoDB 8 and Redis 8
./mvnw spring-boot:run -Dspring-boot.run.profiles=demo  # demo starts two simulated providers on :8091 and :8092
```

```bash
# create a tenant; the key and webhook secret are shown once
curl -s -X POST localhost:8080/admin/tenants -H 'X-Admin-Token: change-me' -H 'Content-Type: application/json' \
  -d '{"name":"search","weight":1,"tokensPerMinute":50000,"maxConcurrentJobs":4}'

# stream a chat answer
curl -N -X POST localhost:8080/v1/chat -H 'X-Api-Key: rk_...' -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"hello"}],"maxTokens":200}'

# submit a job, then poll it
curl -s -X POST localhost:8080/v1/jobs -H 'X-Api-Key: rk_...' -H 'Idempotency-Key: doc-42' \
  -H 'Content-Type: application/json' -d '{"prompt":"summarise ...","maxTokens":300}'
curl -s localhost:8080/v1/jobs/<id> -H 'X-Api-Key: rk_...'
```

To route to a real model, add a Spring AI model starter (for example `spring-ai-starter-model-ollama`) and configure it. Relay picks up the `ChatModel` bean as one more provider.

Tests need only Docker. Testcontainers starts MongoDB and Redis, and the simulated providers run inside the test JVM on real sockets:

```bash
./mvnw verify
```

## When to use which

| | Chat (sync, SSE) | Job (async, 202) |
|---|---|---|
| Caller | A person watching the answer appear | A system that will come back for the result |
| Length | Seconds | Seconds to minutes |
| Provider busy | Fast `503` with `Retry-After`. Queueing it would only make someone wait for an error. | Waits in the queue with backoff |
| Provider dies midway | Error event saying how much was delivered | Retried from the start, which is safe because nobody saw the partial answer |
| Cost control | Budget checked before the stream opens | Budget checked when a worker picks it up; waiting for budget does not use up an attempt |

## Design decisions

### Failover only before the first token

Once tokens have reached the client, switching providers would start the answer again from the top. The client would see two answers glued together and the tenant would pay for both. So the router fails over freely while it is still waiting for the provider to accept the call, and after that a broken stream is reported honestly:

```
event: token   data: {"text":"tok0 "}
...
event: error   data: {"code":"UPSTREAM_BROKEN","delivered":5,"message":"..."}
```

### Estimate, reserve, settle

```
estimate = input characters / 4 + overhead per message + maxTokens
reserve(estimate)         Lua script in Redis, atomic across instances
call the model
settle(actual - estimate) refund or charge the difference
```

Charging the full `maxTokens` up front is deliberate. The model may use all of it, and reserving less lets concurrent calls together overspend the budget. When a stream is cut or the client leaves, the provider never sends its usage, so Relay charges what it actually received.

### Fair scheduling

Each tenant has a pass value. The tenant with the lowest pass runs next, and each job it runs adds `1 / weight`. A tenant with 50,000 jobs queued and a tenant with 5 take turns, so the small tenant's jobs start almost at once. A tenant coming back after being idle starts at the current virtual time, so it cannot save up credit and then take every worker.

`FairnessTest` and `FifoContrastTest` run the same flood with both policies on the same running workers: 200 jobs from one tenant, then 10 from another. With fair scheduling fewer than 40 of the flood run before the small tenant is done. With FIFO more than 180 do.

### Leases instead of locks

A worker claims a job with one `findAndModify` that sets `leaseOwner` and `leaseUntil`. While it works it keeps extending the lease. If it dies, the lease runs out and any worker can claim the job. Every write a worker makes is conditional on still holding the lease, so a worker that was slow rather than dead cannot overwrite the result of the one that took over. `JobQueueTest` plays exactly that: a worker dies holding a job, another finishes it, then the first one comes back with a stale result and is refused.

Running a job twice is safe by construction: results are written once under the lease, and the webhook delivery uses the job id as its own id, so a second run cannot produce a second notification.

### Breaker and adaptive concurrency together

They solve different problems. The breaker answers "is this provider broken right now?" and stops sending traffic for a while if so, then sends one probe. The adaptive limit answers "how much can this provider take?" and moves up slowly on success and halves on `429`. Without the limit, a provider that is only busy gets hammered until it is also broken.

### Webhooks

```
Relay-Signature: t=1767225600,v1=5f2b...   HMAC-SHA256 over "t.body"
Relay-Delivery-Id: <job id>                 dedupe on this
```

The timestamp inside the signature lets a receiver refuse a captured request replayed later. Deliveries retry with backoff, park after the last attempt with the last status code, and can be replayed once the receiver is fixed.

## Works locally, breaks in production

| Looks fine locally | What happens in production | What Relay does |
|---|---|---|
| Stream straight from the app | An AWS ALB closes connections idle for 60 s. A long think before the first token cuts the stream. | Heartbeat comment every 15 s. |
| Stream works against the app directly | Nginx or another proxy buffers the response and the user gets every token at once at the end. | Every event is flushed as it is written, and the response carries `X-Accel-Buffering: no` and `Cache-Control: no-cache` so proxies pass it through. |
| Deploy restarts the app | ECS sends SIGTERM and kills the task 30 s later, cutting every open stream. | New streams refused while draining, open ones get up to 25 s to finish, the rest get a `SERVER_RESTARTING` event. Workers hand their jobs back. |
| `@Scheduled` webhook sender | Runs on every instance, so every webhook is sent N times. | Deliveries are claimed with a lease, one instance per delivery. |
| A single read timeout | Either cuts healthy long streams or lets a stalled one hold a connection forever. | Separate connect, first byte and idle timeouts. |
| Stub that always answers instantly | Retry, failover and cancellation code first runs for real in production. | The simulator returns `429`s, goes down, cuts streams, stalls, and counts tokens it would bill. |
| User closes the tab | The model keeps generating and the tokens are billed. | Disconnect closes the upstream connection. `ChatStreamTest` checks the provider saw the cancel and the budget was refunded. |
| Logs show the request | The async half of the request, in a worker on another instance, has no trace id. | The W3C trace id is stored on the job and restored in the worker's logging context. |
| Retry every failure | A provider `400` is retried on every provider, and spent on every time. | Rejections are final. Only provider side failures fail over or retry. |

## What is next

* **Hard per tenant job cap** with a Redis semaphore. The current cap is checked before claiming and can be over by one for a moment.
* **Output schema validation** with one bounded repair retry, and prompt injection checks on retrieved documents.
* **Webhook URL checks** that refuse private and link local addresses, so a tenant cannot point Relay at internal services.
* **Published drill numbers**: small tenant p95 during a flood, breaker behaviour under 30 percent `429`s, streams cut during a deploy with and without draining.
