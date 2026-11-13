# RxJS lift Debug

## How `lift()` can support an RxJS debugger

**Analysis and teaching prototype by ChatGPT — GPT-6 Astra, main contributor and co-author, in collaboration with Hans Schenker.**

This repository preserves the step-by-step analysis of how RxJS **7.8.2**'s `lift()` architecture can help discover operator connections and observe their execution. It includes the small TypeScript recorder developed for that analysis and the eight focused verification cases that establish its behavior and its limits.

**Start with [the full analysis](docs/lift-debugger-analysis.md).** It follows the explanation from dataflow intuition through source-level mechanics, cancellation, scheduling, sharing, and executable evidence.

The motivating passage is in [How To Build Your Own RxJS Operators — Ben Lesh & Tracy Lee, at 17:43](https://www.youtube.com/watch?v=E6R_1QB8q4o&t=1063s). The passage was not independently transcribed. The implementation explanation is grounded in the tagged RxJS 7.8.2 source and Ben Lesh's own [discussion of `lift` as a tooling hook](https://github.com/ReactiveX/rxjs/issues/2911).

## The central idea

`lift()` exposes **construction**: a source Observable is connected to an operation and a result Observable. A debugger can record that relationship and decorate the operation that will run later.

When someone subscribes, the decorated operation exposes **execution**: input and output notifications, connection lifetimes, and cancellation. The probe observes the application's requested connection; it does not create a second application subscription to inspect the source.

Boundary observations, private implementation state, and semantic explanations are distinct. Operator names, dynamic inner connections, shared connection ownership, and scheduler actions require additional metadata or instrumentation.

## Contents

| File | Purpose |
| --- | --- |
| [Full analysis](docs/lift-debugger-analysis.md) | The complete reasoning, examples, architecture, and limitations from the discussion |
| [Source references](docs/sources.md) | Tagged RxJS implementation files and primary historical/tooling references |
| [Verification record](docs/verification.md) | Reproduction commands, eight cases, observed results, and the boundaries of the evidence |
| [TypeScript recorder](src/lift-recorder.ts) | Functional helpers that record construction edges and input/output connections |
| [Verification cases](tests/verify.cjs) | The preserved eight-case suite, exercising the compiled TypeScript implementation |
| [Runnable example](examples/basic.ts) | A synchronous `map` → `filter` → `scan` pipeline and its recorded boundaries |

## Run the preserved experiment

The recorded validation environment uses **Node.js 24.22.1** and **npm 11.9.0**. See [Verification](docs/verification.md) for the exact commands and results.

```sh
npm ci
npm run typecheck
npm test
npm run example
```

The package pins **RxJS 7.8.2** and **TypeScript 5.8.3**. `npm test` builds the canonical TypeScript source before executing the preserved cases.

The basic example takes source values `1, 2, 3`, doubles them to `2, 4, 6`, accepts values greater than two, and accumulates the accepted values. The consumer receives **`4, 10`**.

## What the recorder captures

- A stable node ID for each encountered Observable and the source/result construction edges.
- A fresh `run` ID for each invocation of recorded operator connection logic.
- A fresh `port` ID for each observed input/output connection.
- `start`, `next`, `error`, `complete`, and `close` events, with an event sequence number and an injectable clock.
- Closure classification based on whether the boundary observed completion, an error, or neither before closing.

Each responsibility has a small named helper: `getNodeId`, `recordEvent`, `createProbe`, `observeInput`, `applyLift`, and `recordedLift`.

## Scope and limits

This is a version-specific teaching experiment. It patches **only `Observable.prototype.lift`** and uses internal RxJS surfaces that are deprecated in the pinned release. Install it before constructing the pipeline to be recorded.

- `Subject` has its own `lift()` override, so direct `Subject.pipe(...)` chains bypass this patch. One verification case deliberately demonstrates that coverage gap.
- Creation functions, custom operators that bypass `lift`, identity cases, other RxJS copies, and already constructed pipelines need additional instrumentation.
- A `run` records the activation in which a probe was installed. A shared input connection can outlive that activation's output subscription. A full debugger needs separate shared connection relationships.
- Operator names/configuration, internal closure variables, inner-subscription causality, and scheduled actions are not automatically discovered.
- Values are retained as references in an unbounded teaching array. A long-running debugger needs bounded retention and an explicit snapshot policy.
- `close` marks boundary closure, not a guarantee that every external resource has finished shutting down. Closing a subscription does not itself establish that underlying work was aborted.
- `restore()` restores future use of the prototype method. Observables already constructed with recording wrappers keep them.
- Decorating the source passed to an operation can affect arbitrary custom operators that depend on its identity or extra properties. The focused checks do not establish universal transparency.

See the full analysis for the distinction between directly observed events, reconstructed semantic state, and independently instrumented implementation state.

## Relationship to the other RxJS projects

This repository is a separate research and evidence record for the `lift()` debugger idea. Its analysis can inform [rxjs-inspector](https://github.com/hansschenker/rxjs-inspector) and the **Intuition → Specification → Verification** approach in the RxJS Formal Way work.

The next design increments described in the analysis—stable operator labels, subscription graphs, scheduler integration, and semantic adapters—are proposals. They are not implemented by this recorder.

## Contributors

- **Hans Schenker** — project owner, research direction, teaching goals, and review.
- **ChatGPT — GPT-6 Astra** — main contributor and co-author of the analysis, recorder, and verification material.

Saved on **13 November 2026**, Europe/Zurich.
