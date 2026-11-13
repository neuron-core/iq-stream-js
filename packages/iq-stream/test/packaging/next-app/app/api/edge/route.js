import { BUILD, IQClient } from '@neuron-core/iq-stream';

export const runtime = 'edge';

export async function GET() {
  const client = new IQClient({ url: 'http://127.0.0.1:1', appKey: 'demo-app', appSecret: 'test-app-secret-0123456789abcdef0123' });
  const streamId = client.newStreamId();
  const token = await client.token('sub', streamId);
  return Response.json({ runtime: 'edge', BUILD, verified: await client.verify('sub', token, streamId) });
}
