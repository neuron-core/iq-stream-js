'use client';
import { subscribe, BUILD } from '@neuron-core/iq-stream/client';

export default function Page() {
  return <p id="client">{BUILD} {typeof subscribe}</p>;
}
