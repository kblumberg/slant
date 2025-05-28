// app/layout.tsx
import Header from '@/components/ui/Header';
import SidePanel from '@/components/ui/SidePanel';
import { WalletContextProvider } from '@/components/WalletContextProvider';
import '../styles/custom.css';
import '@solana/wallet-adapter-react-ui/styles.css';
import type { ReactNode } from 'react';
import "./globals.css";
import { NewsProvider } from '@/context/NewsContext';



export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body className="bg-[#0e0f12] text-white overflow-x-hidden">
        <NewsProvider>
          <WalletContextProvider>
            <Header />
            <SidePanel />
            <main className="pt-24 px-4 sm:px-6 sm:pl-48 pl-24 w-full">{children}</main>
            {/* <ChatPanel /> */}
          </WalletContextProvider>
        </NewsProvider>
      </body>
    </html>
  );
}
