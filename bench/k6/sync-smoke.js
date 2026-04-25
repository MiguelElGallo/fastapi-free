import http from 'k6/http';
import { check, sleep } from 'k6';
import exec from 'k6/execution';

const BASE_URL = (__ENV.BASE_URL || 'http://localhost:8000').replace(/\/$/, '');
const AUTH_TOKEN = __ENV.AUTH_TOKEN || '';
const ROUTE_PROFILE = __ENV.ROUTE_PROFILE || 'sync-mixed';
const DATASET_SIZE = Number(__ENV.DATASET_SIZE || '10000');
const LARGE_DATASET_SIZE = Number(__ENV.LARGE_DATASET_SIZE || '1000');
const SLEEP_SECONDS = Number(__ENV.SLEEP_SECONDS || '1');
const REQUEST_TIMEOUT = __ENV.REQUEST_TIMEOUT || '60s';

const PROFILE_ENDPOINTS = {
  'sync-read': [
    '/lookup/key-{key}',
    '/lookup-large/key-{large_key}',
    '/lookup-preencoded/key-{key}',
    '/cow/key-{key}',
  ],
  'sync-cpu': ['/cpu/key-{key}'],
  'sync-cache': ['/locked-cache/key-{key}'],
  'sync-mixed': [
    '/lookup/key-{key}',
    '/lookup-preencoded/key-{key}',
    '/cpu/key-{key}',
    '/cow/key-{key}',
    '/locked-cache/key-{key}',
  ],
};

const ENDPOINTS = (__ENV.SYNC_ENDPOINTS || '')
  .split(',')
  .map((endpoint) => endpoint.trim())
  .filter(Boolean);
const ACTIVE_ENDPOINTS = ENDPOINTS.length > 0 ? ENDPOINTS : PROFILE_ENDPOINTS[ROUTE_PROFILE] || PROFILE_ENDPOINTS['sync-mixed'];

export const options = {
  vus: Number(__ENV.VUS || '1'),
  iterations: Number(__ENV.ITERATIONS || String(ACTIVE_ENDPOINTS.length)),
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
  tags: {
    suite: 'sync-smoke',
  },
};

function headers() {
  const result = {
    Accept: 'application/json',
  };

  if (AUTH_TOKEN) {
    result.Authorization = `Bearer ${AUTH_TOKEN}`;
  }

  return result;
}

function endpointForIteration(iteration) {
  const template = ACTIVE_ENDPOINTS[iteration % ACTIVE_ENDPOINTS.length];
  const key = iteration % DATASET_SIZE;
  const largeKey = iteration % LARGE_DATASET_SIZE;
  return template
    .replace(/\{key\}/g, String(key))
    .replace(/\{large_key\}/g, String(largeKey));
}

export default function () {
  const endpoint = endpointForIteration(exec.scenario.iterationInTest);
  const url = `${BASE_URL}${endpoint.startsWith('/') ? endpoint : `/${endpoint}`}`;
  const response = http.get(url, {
    headers: headers(),
    timeout: REQUEST_TIMEOUT,
    tags: {
      endpoint,
      kind: 'sync',
    },
  });

  check(response, {
    'sync endpoint returned 2xx': (r) => r.status >= 200 && r.status < 300,
    'sync endpoint completed under 500ms': (r) => r.timings.duration < 500,
  });

  sleep(SLEEP_SECONDS);
}
