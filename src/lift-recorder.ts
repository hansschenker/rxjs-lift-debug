import { Observable, Subscriber, type TeardownLogic } from 'rxjs';

type CloseReason = 'unsubscribe' | 'error' | 'complete';
type RunContext = { node: string; run: string };
type ProbeContext = RunContext & { side: 'input' | 'output'; port: string };
type Detail = { value?: unknown; error?: unknown; reason?: CloseReason };
type EventKind = 'start' | 'next' | 'error' | 'complete' | 'close';
type TraceEvent = ProbeContext & Detail & { sequence: number; time: number; kind: EventKind };
type Node = { node: string; parent: string };
type Operation<T, R> = {
  call(destination: Subscriber<R>, source: Observable<T>): TeardownLogic;
};

// Teaching experiment for RxJS 7.8.2; only Observable.prototype is patched.
export function installLiftRecorder(now = () => performance.now()) {
  const originalLift = Observable.prototype.lift;
  const events: TraceEvent[] = [];
  const nodes: Node[] = [];
  const nodeIds = new WeakMap<object, string>();
  let nodeSequence = 0, runSequence = 0, portSequence = 0, eventSequence = 0;

  function getNodeId(observable: object): string {
    if (!nodeIds.has(observable)) nodeIds.set(observable, `n${++nodeSequence}`);
    return nodeIds.get(observable)!;
  }

  function recordEvent(context: ProbeContext, kind: EventKind, detail: Detail = {}) {
    try {
      events.push({ sequence: ++eventSequence, time: now(), ...context, kind, ...detail });
    } catch { /* Recorder failures must not enter the application's dataflow. */ }
  }

  function createProbe<T>(destination: Subscriber<T>, context: ProbeContext) {
    let reason: CloseReason = 'unsubscribe';
    recordEvent(context, 'start');
    const probe = new Subscriber<T>({
      next(value) {
        recordEvent(context, 'next', { value });
        destination.next(value);
      },
      error(error: unknown) {
        reason = 'error';
        recordEvent(context, 'error', { error });
        destination.error(error);
      },
      complete() {
        reason = 'complete';
        recordEvent(context, 'complete');
        destination.complete();
      }
    });
    destination.add(probe); // Wire cancellation before upstream execution.
    probe.add(() => recordEvent(context, 'close', { reason }));
    return probe;
  }

  function observeInput<T>(source: Observable<T>, context: RunContext) {
    return new Observable<T>(destination => {
      const probe = createProbe(destination, {
        ...context, side: 'input', port: `p${++portSequence}`
      });
      source.subscribe(probe);
      return probe;
    });
  }

  function applyLift<T, R>(source: Observable<T>, operation?: Operation<T, R>) {
    // Function.call erases lift's generic result type; restore it at this boundary.
    return originalLift.call(source, operation) as Observable<R>;
  }

  function recordedLift<T, R>(this: Observable<T>, operation?: Operation<T, R>): Observable<R> {
    if (!operation) return applyLift(this, operation);
    const parent = getNodeId(this);
    let node: string;
    const result = applyLift<T, R>(this, {
      call(destination, source) {
        const context = { node, run: `r${++runSequence}` };
        const output = createProbe(destination, {
          ...context, side: 'output', port: `p${++portSequence}`
        });
        output.add(operation.call(output, observeInput(source, context)));
        return output;
      }
    });
    node = getNodeId(result);
    nodes.push({ node, parent });
    return result;
  }

  Observable.prototype.lift = recordedLift;
  return {
    events, nodes, getNodeId,
    restore() {
      // Existing Observables keep their wrappers; this affects future construction.
      if (Observable.prototype.lift === recordedLift) Observable.prototype.lift = originalLift;
    }
  };
}
