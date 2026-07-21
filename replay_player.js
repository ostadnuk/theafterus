/* Static replay player — replaces the local Python WebSocket server.
   Streams the archived pilot readings with the original transport cadence.
   No API, no backend: the machine remembers, it does not think anew. */
(function () {
  class ReplayWS {
    constructor () {
      ReplayWS.instances.push(this);
      setTimeout(() => {
        this.readyState = 1;
        if (this.onopen) this.onopen({});
      }, 60);
    }
    send () {}
    close () {}
  }
  ReplayWS.instances = [];
  window.WebSocket = ReplayWS;

  const emit = obj => {
    const payload = JSON.stringify(obj);
    ReplayWS.instances.forEach(ws => {
      try { if (ws.onmessage) ws.onmessage({ data: payload }); } catch (e) {}
    });
  };
  const sleep = ms => new Promise(r => setTimeout(r, ms));

  const START_DELAY = 900;    // pause after trace_start, as in the live loop
  const CHUNK = 24;           // transport chunk size
  const CHUNK_DELAY = 45;     // ms between chunks
  const PAUSE = 10000;        // ms between traces — slow dramaturgy

  fetch('replay_data.json')
    .then(r => r.json())
    .then(async ({ events }) => {
      // wait until the page has actually opened its socket and is listening —
      // otherwise the first trace is emitted into the void and lost
      while (!ReplayWS.instances.some(ws => typeof ws.onmessage === 'function')) {
        await sleep(120);
      }
      await sleep(300);
      // absolute clock: browsers throttle timers in background tabs, so each
      // step is scheduled against real elapsed time — on wake the player
      // catches up in a burst instead of crawling forever behind
      let clock = performance.now();
      const tick = ms => {
        clock += ms;
        const wait = clock - performance.now();
        return wait > 4 ? sleep(wait) : Promise.resolve();
      };
      const maxPass = Math.max(...events.map(e => e.pass || 1));
      let seq = 0, cycle = 0;
      for (;;) {
        for (const ev of events) {
          seq++;
          const pass = (ev.pass || 1) + cycle * maxPass;
          emit({ type: 'trace_start', seq, pass, trace_id: ev.trace_id,
                 image: ev.image, machine_image: ev.machine_image });
          await tick(START_DELAY);
          const raw = JSON.stringify(ev.data);
          for (let i = 0; i < raw.length; i += CHUNK) {
            emit({ type: 'trace_delta', seq, pass, trace_id: ev.trace_id,
                   delta: raw.slice(i, i + CHUNK) });
            await tick(CHUNK_DELAY);
          }
          emit({ type: 'trace_done', seq, pass, trace_id: ev.trace_id,
                 image: ev.image, machine_image: ev.machine_image, data: ev.data });
          await tick(PAUSE);
        }
        cycle++;
      }
    });
})();
