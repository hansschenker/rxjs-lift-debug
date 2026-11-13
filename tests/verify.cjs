const assert = require('node:assert/strict');
const { Observable, Subject, of, map, filter, scan, take, share, debounceTime, VirtualTimeScheduler } = require('rxjs');
const { installLiftRecorder } = require('../dist/src/lift-recorder.js');
const cases = [];

function test(name, fn) {
  fn();
  cases.push(name);
}

test('map/filter/scan: values and exact operator boundary records', () => {
  const recorder = installLiftRecorder(() => 0);
  try {
    const result = of(1, 2, 3).pipe(map(n => n * 2), filter(n => n > 2), scan((a, n) => a + n, 0));
    const values = [];
    result.subscribe(v => values.push(v));
    assert.deepEqual(values, [4, 10]);
    assert.equal(recorder.nodes.length, 3);
    const [mapped, filtered, scanned] = recorder.nodes.map(n => n.node);
    const nextValues = (node, side) => recorder.events.filter(e => e.node === node && e.side === side && e.kind === 'next').map(e => e.value);
    assert.deepEqual(nextValues(mapped, 'input'), [1, 2, 3]);
    assert.deepEqual(nextValues(mapped, 'output'), [2, 4, 6]);
    assert.deepEqual(nextValues(filtered, 'output'), [4, 6]);
    assert.deepEqual(nextValues(scanned, 'output'), [4, 10]);
    assert.equal(recorder.events.filter(e => e.kind === 'start').length, 6);
    assert.equal(recorder.events.filter(e => e.kind === 'close').length, 6);
  } finally { recorder.restore(); }
});

test('synchronous producer stops after take(1), teardown once', () => {
  const recorder = installLiftRecorder(() => 0);
  try {
    let produced = 0, disposed = 0;
    const source = new Observable(s => {
      for (let n = 0; n < 100 && !s.closed; n++) { produced++; s.next(n); }
      if (!s.closed) s.complete();
      return () => disposed++;
    });
    const values = [];
    source.pipe(map(n => n), take(1)).subscribe(v => values.push(v));
    assert.deepEqual(values, [0]);
    assert.equal(produced, 1);
    assert.equal(disposed, 1);
  } finally { recorder.restore(); }
});

test('projection error identity and upstream cancellation preserved', () => {
  const recorder = installLiftRecorder(() => 0);
  try {
    const failure = new Error('same instance');
    let seen, calls = 0;
    of(1, 2).pipe(map(() => { calls++; throw failure; })).subscribe({ error(e) { seen = e; } });
    assert.equal(seen, failure);
    assert.equal(calls, 1);
    const errors = recorder.events.filter(e => e.kind === 'error');
    assert.equal(errors.length, 1);
    assert.equal(errors[0].error, failure);
  } finally { recorder.restore(); }
});

test('early unsubscribe closes input/output without completion', () => {
  const recorder = installLiftRecorder(() => 0);
  try {
    let upstream, disposed = 0, completed = 0;
    const source = new Observable(s => { upstream = s; return () => disposed++; });
    const values = [];
    const subscription = source.pipe(map(n => n * 2)).subscribe({ next: v => values.push(v), complete: () => completed++ });
    upstream.next(1);
    subscription.unsubscribe();
    upstream.next(2);
    assert.deepEqual(values, [2]);
    assert.equal(disposed, 1);
    assert.equal(completed, 0);
    assert.equal(recorder.events.filter(e => e.kind === 'complete').length, 0);
    assert.equal(recorder.events.filter(e => e.kind === 'close' && e.reason === 'unsubscribe').length, 2);
  } finally { recorder.restore(); }
});

test('two subscriptions create independent runs, state and execution', () => {
  const recorder = installLiftRecorder(() => 0);
  try {
    let starts = 0;
    const source = new Observable(s => { starts++; s.next(2); s.next(3); s.complete(); });
    const result = source.pipe(scan((a, n) => a + n, 0));
    const a = [], b = [];
    result.subscribe(v => a.push(v)); result.subscribe(v => b.push(v));
    assert.deepEqual(a, [2, 5]); assert.deepEqual(b, [2, 5]); assert.equal(starts, 2);
    assert.equal(new Set(recorder.events.map(e => e.run)).size, 2);
  } finally { recorder.restore(); }
});

test('debounce virtual timing: replacement, timer emission, completion flush', () => {
  const scheduler = new VirtualTimeScheduler();
  const recorder = installLiftRecorder(() => scheduler.now());
  try {
    const source = new Observable(s => {
      s.add(scheduler.schedule(() => s.next('a'), 0));
      s.add(scheduler.schedule(() => s.next('b'), 2));
      s.add(scheduler.schedule(() => s.next('c'), 10));
      s.add(scheduler.schedule(() => s.complete(), 11));
    });
    const values = [];
    source.pipe(debounceTime(5, scheduler)).subscribe(v => values.push([scheduler.now(), v]));
    scheduler.flush();
    assert.deepEqual(values, [[7, 'b'], [11, 'c']]);
    assert.deepEqual(recorder.events.filter(e => e.side === 'output' && e.kind === 'next').map(e => [e.time, e.value]), values);
    assert.equal(scheduler.actions.length, 0);
  } finally { recorder.restore(); }
});

test('share behavior preserved; input run attribution outlives first output run', () => {
  const recorder = installLiftRecorder(() => 0);
  try {
    let upstream, starts = 0, disposed = 0;
    const source = new Observable(s => { starts++; upstream = s; return () => disposed++; });
    const result = source.pipe(share());
    const first = [], second = [];
    const a = result.subscribe(v => first.push(v));
    const b = result.subscribe(v => second.push(v));
    assert.equal(starts, 1);
    upstream.next(1); a.unsubscribe(); upstream.next(2);
    assert.deepEqual(first, [1]); assert.deepEqual(second, [1, 2]); assert.equal(disposed, 0);
    const firstRun = recorder.events.find(e => e.side === 'output' && e.kind === 'start').run;
    const firstClosed = recorder.events.find(e => e.run === firstRun && e.side === 'output' && e.kind === 'close');
    const laterInput = recorder.events.find(e => e.run === firstRun && e.side === 'input' && e.kind === 'next' && e.value === 2);
    assert.ok(laterInput.sequence > firstClosed.sequence);
    b.unsubscribe(); assert.equal(disposed, 1);
  } finally { recorder.restore(); }
});

test('Subject override bypasses prototype patch, as documented', () => {
  const recorder = installLiftRecorder(() => 0);
  try {
    const source = new Subject(); const values = [];
    source.pipe(map(n => n * 2)).subscribe(v => values.push(v));
    source.next(2); source.complete();
    assert.deepEqual(values, [4]); assert.deepEqual(recorder.events, []);
  } finally { recorder.restore(); }
});

console.log(JSON.stringify({ rxjs: require('rxjs/package.json').version, passed: cases.length, cases }, null, 2));
