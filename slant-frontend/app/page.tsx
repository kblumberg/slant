'use client';

import '../styles/custom.css';
import '@solana/wallet-adapter-react-ui/styles.css';


// import Chat from './chat';
import { WalletContextProvider } from '@/components/WalletContextProvider';
import NewsPage from '@/components/NewsPage';
import { useEffect, useState } from 'react';
import News from '@/types/News';


export default function Home() {
	// const conversationId = crypto.randomUUID();
  const [news, setNews] = useState<News[]>([]);

  useEffect(() => {
    console.log('Home');
    const fetchNews = async () => {
      const response = await fetch('http://localhost:5000/load_news');
      const data = await response.json();
      console.log(data);
      setNews(data.data);
    }
    fetchNews();
  }, []);

  return (
    <WalletContextProvider>
      {/* <Chat conversationId={conversationId} /> */}
      <NewsPage news={news} />
    </WalletContextProvider>
  );
}
