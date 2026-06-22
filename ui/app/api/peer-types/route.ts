import { GetPeerDBClickHouseMode } from '@/peerdb-env/allowed_targets';
import { NextRequest } from 'next/server';
export const dynamic = 'force-dynamic';

type PeerTypeItem = { label: string; url?: string; deprecated?: boolean };
type PeerTypeCategory = [string, ...Array<string | PeerTypeItem>];

export async function GET(request: NextRequest) {
  const allWarehouseTypes: PeerTypeCategory = [
    'Warehouses',
    { label: 'SNOWFLAKE', deprecated: true },
    { label: 'BIGQUERY', deprecated: true },
    'S3',
    'CLICKHOUSE',
    { label: 'ELASTICSEARCH', deprecated: true },
  ];
  const clickhouseWarehouseTypes: PeerTypeCategory = ['Targets', 'CLICKHOUSE'];
  const queueTypes: PeerTypeCategory = [
    'Queues',
    { label: 'REDPANDA', deprecated: true },
    { label: 'CONFLUENT', deprecated: true },
    { label: 'KAFKA', deprecated: true },
    { label: 'EVENTHUBS', deprecated: true },
    { label: 'PUBSUB', deprecated: true },
  ];
  const postgresTypes: PeerTypeCategory = [
    'Sources',
    'MYSQL',
    'POSTGRESQL',
    'RDS POSTGRESQL',
    'GOOGLE CLOUD POSTGRESQL',
    'AZURE FLEXIBLE POSTGRESQL',
    'CRUNCHY POSTGRES',
    'NEON',
    'MONGO',
  ];
  if (process.env.SUPABASE_ID) {
    postgresTypes.push({
      label: 'SUPABASE',
      url: `https://api.supabase.com/v1/oauth/authorize?client_id=${encodeURIComponent(
        process.env.SUPABASE_ID
      )}&response_type=code&redirect_uri=${encodeURIComponent(
        process.env.SUPABASE_REDIRECT ?? ''
      )}&state=${encodeURIComponent(process.env.SUPABASE_OAUTH_STATE ?? '')}`,
    });
  }

  if (GetPeerDBClickHouseMode()) {
    return new Response(
      JSON.stringify([postgresTypes, clickhouseWarehouseTypes])
    );
  }
  return new Response(
    JSON.stringify([postgresTypes, allWarehouseTypes, queueTypes])
  );
}
