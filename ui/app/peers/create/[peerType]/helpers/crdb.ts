import { CockroachConfig } from '@/grpc_generated/peers';

import { PeerSetting } from './common';

export const cockroachdbSetting: PeerSetting[] = [
  {
    label: 'Host',
    field: 'host',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, host: value as string })),
    tips: 'Hostname or IP address of a CockroachDB node. Ensure this host is reachable and has us whitelisted so we can connect to it.',
  },
  {
    label: 'Port',
    field: 'port',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, port: parseInt(value as string, 10) })),
    type: 'number',
    default: 26257,
    tips: 'TCP port on which CockroachDB is listening (default: 26257).',
  },
  {
    label: 'User',
    field: 'user',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, user: value as string })),
    tips: 'The user we should use to connect. Requires the CHANGEFEED privilege on the mirrored tables.',
  },
  {
    label: 'Password',
    field: 'password',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, password: value as string })),
    type: 'password',
    tips: 'Password associated with the user you provided.',
  },
  {
    label: 'Database',
    field: 'database',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, database: value as string })),
    default: 'defaultdb',
    tips: 'The database to associate with this peer (default: defaultdb).',
  },
  {
    label: 'Require TLS?',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, requireTls: value as boolean })),
    type: 'switch',
    optional: true,
    tips: 'Use sslmode=require instead of sslmode=prefer. CockroachDB Cloud requires TLS.',
  },
  {
    label: 'Skip Certificate Verification?',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, skipCertVerification: value as boolean })),
    type: 'switch',
    optional: true,
    tips: 'Skip TLS certificate verification (insecure, use with caution).',
  },
  {
    label: 'Root Certificate',
    stateHandler: (value, setter) => {
      if (!value) {
        // remove key from state if empty
        setter((curr) => {
          const newCurr = { ...curr } as CockroachConfig;
          delete newCurr.rootCa;
          return newCurr;
        });
      } else setter((curr) => ({ ...curr, rootCa: value as string }));
    },
    type: 'file',
    optional: true,
    tips: 'Root CA certificate for TLS connections. If not provided, host CA roots will be used.',
  },
  {
    label: 'TLS Hostname',
    field: 'tlsHost',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, tlsHost: value as string })),
    tips: 'Overrides expected hostname during TLS cert verification.',
    optional: true,
  },
  {
    label: 'Resolved Interval (Seconds)',
    field: 'resolvedIntervalSeconds',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        resolvedIntervalSeconds: parseInt(value as string, 10),
      })),
    type: 'number',
    default: 10,
    optional: true,
    tips: 'Cadence of changefeed `resolved` timestamps, which drive replication checkpoints. Defaults to 10 seconds.',
  },
  {
    label: 'History Protection Window (Seconds)',
    field: 'historyProtectionWindowSeconds',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        historyProtectionWindowSeconds: parseInt(value as string, 10),
      })),
    type: 'number',
    default: 86400,
    optional: true,
    tips: 'How long PeerDB pins MVCC history at the initial-snapshot start timestamp so a long snapshot can outlive the source `gc.ttlseconds` (PeerDB extends it automatically while the snapshot runs). Defaults to 86400 (24h); set to 0 to disable.',
  },
];

export const blankCockroachDBSetting: CockroachConfig = {
  host: '',
  port: 26257,
  user: '',
  password: '',
  database: 'defaultdb',
  tlsHost: '',
  requireTls: false,
  skipCertVerification: false,
  resolvedIntervalSeconds: 10,
  changefeedExtraOptions: {},
};
