# Verification record

Recorded on **13 November 2026** (Europe/Zurich).

The [canonical TypeScript recorder](../src/lift-recorder.ts) was preserved from the experiment accompanying the analysis. The [eight-case verification suite](../tests/verify.cjs) was preserved with one packaging change: it now loads `../dist/src/lift-recorder.js`, so it tests the compiled canonical source rather than a separate handwritten JavaScript copy.

## Environment and reproduction

| Component | Version |
| --- | --- |
| RxJS | **7.8.2**, pinned |
| TypeScript | **5.8.3**, pinned |
| Node.js | **24.22.1** |
| npm | **11.9.0** |

Run these commands from the repository root:

```sh
npm ci --ignore-scripts --no-audit --no-fund
npm test
npm run typecheck
npm run example
```

| Command | Observed result |
| --- | --- |
| Clean dependency installation | Succeeded using the committed lockfile |
| `npm test` | Strict TypeScript build succeeded; **8/8 cases passed** |
| `npm run typecheck` | Passed |
| `npm run example` | Consumer received `[4, 10]`; three constructed operator edges and the expected input/output value records |

`npm test` compiles before running the suite. Tests execute sequentially and restore the prototype patch in `finally` blocks. This is necessary because the recorder temporarily modifies a prototype shared by the tests.

## The eight preserved cases

| # | Case | Checked evidence |
| ---: | --- | --- |
| 1 | `map` → `filter` → `scan` | Source values `[1, 2, 3]`; map output `[2, 4, 6]`; filter output `[4, 6]`; scan output `[4, 10]`. Three operator edges are recorded, and all six input/output boundary connections start and close. |
| 2 | Synchronous source plus `take(1)` | A source loop that checks `subscriber.closed` produces exactly one value. The consumer receives only that value, and source teardown runs exactly once. This detects the early-cancellation failure that a final-values-only assertion would miss. |
| 3 | Projection throws | The exact original Error object reaches the consumer. The projection executes once, and upstream is canceled before the next source value is processed. |
| 4 | Early unsubscribe | Only values before cancellation reach the consumer. No completion notification is invented. Both observed boundaries close without a terminal notification, and source teardown runs once. |
| 5 | Repeated subscriptions | Subscribing twice to the same cold `scan` pipeline starts two source executions with independent accumulated state and activation IDs. Both produce `[2, 5]`. |
| 6 | `debounceTime(5, scheduler)` | Virtual inputs `a@0`, `b@2`, `c@10`, with completion at `11`, produce `b@7` and `c@11`. The pending value is flushed on completion, the trace timestamps agree with the consumer's observations, and no scheduled actions remain. |
| 7 | Two consumers through `share()` | There is one source connection. Closing the first consumer keeps it alive for the second; closing the last consumer disposes it once. A later shared input event still has the activation ID under which its probe was installed, proving that activation attribution must not be treated as exclusive lifetime ownership. |
| 8 | `Subject.lift()` override | A direct `Subject.pipe(map(...))` still produces the correct values, but the recorder captures no events. This is an explicit coverage test for a known limitation, not evidence that the Subject path is instrumented. |

## What these results establish

The experiment preserves the observed values, errors, connection endings, and virtual timing in the listed scenarios. The synchronous cancellation check and the sharing check test upstream work and lifetime as well as downstream output.

The checked example is cold and synchronous. The sharing case explicitly introduces a shared upstream connection. The time case uses the same `VirtualTimeScheduler` for the source, the debounce operator, and the recorder's injected clock.

The tests exercise the actual pinned RxJS implementation. They are focused regression examples, not property-based exploration, model checking, or a proof for all operator implementations.

## Limits of the evidence

- Only the listed scenarios and recorded runtime/toolchain versions were verified in this repository snapshot.
- The prototype patches only `Observable.prototype.lift`. The successful Subject coverage test deliberately confirms the missing path.
- `run` describes the activation in which a probe was installed. A shared input connection can outlive that activation's output. A full debugger needs a separate shared-connection model.
- The source proxy does not preserve every source identity or extra property that an arbitrary custom operator might inspect.
- No test here establishes automatic visibility into closure state, scheduled actions, dynamic inner-subscription causality, or physical cancellation of external work.
- The recorder retains payload references. A later mutation of an object can affect what an inspector displays; no snapshot policy is implemented.
- A `close` event is a boundary-lifetime observation, not an assertion that every downstream or upstream finalizer has finished.
- `restore()` restores future construction through the prototype. Previously constructed Observables retain their recording wrappers.
- Recording adds overhead. The virtual-time result controls scheduling for the experiment; it does not establish zero disturbance of real-time application timing.

## Next evidence to add when development resumes

Stable operator metadata, a general subscription graph, scheduler instrumentation, and semantic adapters are described in the [analysis](lift-debugger-analysis.md). Each should introduce verification for its own new behavior. Those extensions have not been added to this archive.
