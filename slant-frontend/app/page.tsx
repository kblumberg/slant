'use client';

import '../styles/custom.css';
import '@solana/wallet-adapter-react-ui/styles.css';


import Chat from './chat';
import { WalletContextProvider } from '@/components/WalletContextProvider';


export default function Home() {
	const conversationId = crypto.randomUUID();

  return (
    <WalletContextProvider>
      <Chat conversationId={conversationId} />
    </WalletContextProvider>
  );
}
