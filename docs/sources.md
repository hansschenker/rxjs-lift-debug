# Sources for the lift debugger analysis

Analysis date: **13 November 2026** (Europe/Zurich). Implementation claims are pinned to **RxJS 7.8.2**. Historical descriptions and other debugging projects illustrate design choices; they do not establish compatibility with every RxJS version.

## Video and historical context

| Reference | Relevance and evidence boundary |
| --- | --- |
| [How To Build Your Own RxJS Operators — Ben Lesh & Tracy Lee](https://www.youtube.com/watch?v=E6R_1QB8q4o&t=1063s), ng-conf, published 6 May 2019, linked at 17:43 | The user identified this passage as the motivation. Video metadata was verified, but the passage's transcript could not be retrieved. The analysis does not present a recovered quotation from that passage. |
| [Ben Lesh: Discuss the value of `lift` in the 6.0 world, issue #2911](https://github.com/ReactiveX/rxjs/issues/2911), 6 October 2017 | Primary account of `lift`'s original extensibility and tooling motivations. It explicitly qualifies coverage as almost all operators and acknowledges bypasses. |
| [André Staltz: How to debug RxJS code](https://staltz.com/how-to-debug-rxjs-code.html), 29 December 2015 | Historical discussion of tracing, dependency graphs, marble diagrams, and plans for tooling based on the lift architecture. It distinguishes observation inside an execution from adding another subscription. |

## Tagged RxJS 7.8.2 implementation

These files were inspected at the explicit `7.8.2` tag. Links deliberately avoid the moving default branch.

| Source | What it establishes |
| --- | --- |
| [Observable.ts](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/Observable.ts) | `lift` stores `source` and `operator`; `subscribe` invokes `operator.call(subscriber, source)` and attaches teardown. These surfaces are marked as deprecated implementation details. |
| [util/lift.ts](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/util/lift.ts) | `operate` creates the operator function and passes a callback to `source.lift`; the callback's `this` is the destination Subscriber. |
| [Subscriber.ts](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/Subscriber.ts) | Notification guards, terminal delivery, unsubscription, and immediate subscription linkage when the destination is a Subscription. |
| [Subscription.ts](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/Subscription.ts) | Teardown registration, parent/child subscription relationships, and immediate execution of a finalizer added to an already closed subscription. |
| [Subject.ts](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/Subject.ts) | The `Subject.lift` override and `AnonymousSubject` path, explaining why a base-prototype patch is incomplete. |
| [OperatorSubscriber.ts](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/OperatorSubscriber.ts) | The operator Subscriber helper and forwarding, exception, and finalization behavior used by built-in operators. |
| [debounceTime.ts](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/debounceTime.ts) | Pending value and time state, task reuse/rescheduling, completion flush, and error/teardown handling. |
| [switchMap.ts](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/switchMap.ts) | Dynamic inner subscriptions, cancellation of the previous inner, and completion after the outer has completed and the active inner has ended. |
| [share.ts](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/share.ts) | Shared closure state, reference count, Subject, and separate upstream connection. `share` uses `operate` and must not be classified as bypassing lift. |
| [concatMap.ts](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/concatMap.ts) | Delegation to `mergeMap` with concurrency one; a stored operation does not reliably reveal the public operator originally called. |
| [take.ts](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/take.ts) | The nonpositive-count path returns `EMPTY` without lifting or subscribing to the original source. |
| [tap.ts](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/tap.ts) | The no-handler path returns the identity operation; ordinary instrumentation using tap can itself use lift. |
| [AsyncAction.ts](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/scheduler/AsyncAction.ts) | Action scheduling, rescheduling, execution, and unsubscription; instrumenting only the first scheduler call is insufficient. |

The deprecated comments are statements in the pinned source. Their references to later versions are not treated as proof of the state of those later versions.

## Existing debugging architectures

| Project | Design evidence |
| --- | --- |
| [rxjs-spy](https://github.com/cartant/rxjs-spy) | Documents attaching to `Observable.prototype.subscribe`, tagged observation, notification/lifetime logs, graphs, snapshots, breakpoints, and buffered notification stepping. It is not described here as a lift-only debugger. |
| [RxJS Insights](https://github.com/ksz-ksz/rxjs-insights) | Separates construction/operator calls, subscribers, external callers, events and causality, and asynchronous task context. |

## Local evidence

The repository's [verification record](verification.md) describes the executed checks against the [canonical TypeScript recorder](../src/lift-recorder.ts). Those results establish the named scenarios only. The broader architecture, future adapters, and semantic-model integration remain design proposals.
