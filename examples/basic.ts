import { of, map, filter, scan } from 'rxjs';
import { installLiftRecorder } from '../src/lift-recorder';

// Install before constructing the dataflow. This source is cold and synchronous.
const recorder = installLiftRecorder();

try {
  const result$ = of(1, 2, 3).pipe(
    map(value => value * 2),
    filter(value => value > 2),
    scan((total, value) => total + value, 0)
  );

  const values: number[] = [];
  result$.subscribe(value => values.push(value));

  console.log('Consumer values:', values); // [4, 10]
  console.log('Constructed connections:');
  console.table(recorder.nodes);
  console.log('Values crossing operator boundaries:');
  console.table(recorder.events.filter(event => event.kind === 'next'));
} finally {
  // Stops wrapping future constructions; existing Observables keep their wrappers.
  recorder.restore();
}
