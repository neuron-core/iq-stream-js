'use client';
import { useEffect, useState } from 'react';
import { IQClient } from '@neuron-core/iq-stream'; // the mistake: the publisher in a client component

export default function Misuse() {
  const [text, setText] = useState('waiting');
  useEffect(() => {
    try {
      new IQClient({ url: 'http://127.0.0.1:1', appKey: 'app', appSecret: 'x'.repeat(32) });
      setText('constructed');
    } catch (error) {
      setText(error.message);
    }
  }, []);
  return <p id="misuse">{text}</p>;
}
