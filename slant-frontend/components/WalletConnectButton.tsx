'use client';

import { FC } from 'react';
import { useWallet } from '@solana/wallet-adapter-react';
import { WalletMultiButton } from '@solana/wallet-adapter-react-ui';
// import styles from '../styles/WalletConnectButton.module.css';

export const WalletConnectButton: FC = () => {
  const { publicKey, connected } = useWallet();

  return (
    <div className="walletContainer">
      <WalletMultiButton />
      {connected && (
        <p className="addressText">
          Connected: {publicKey?.toString().slice(0, 4)}...{publicKey?.toString().slice(-4)}
        </p>
      )}
    </div>
  );
};