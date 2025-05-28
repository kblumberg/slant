// context/NewsContext.tsx
'use client';

import { createContext, useContext, useEffect, useState, ReactNode } from 'react';
import News from '@/types/News';

interface NewsContextType {
  news: News[] | null;
  setNews: (news: News[]) => void;
  loading: boolean;
}

const NewsContext = createContext<NewsContextType | undefined>(undefined);

export const NewsProvider = ({ children }: { children: ReactNode }) => {
  const [news, setNews] = useState<News[] | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadNews = async () => {
      if (!news) {
        const res = await fetch('http://localhost:5000/load_news');
        const data = await res.json();
        setNews(data.data);
      }
      setLoading(false);
    };
    loadNews();
  }, []);

  return (
    <NewsContext.Provider value={{ news, setNews, loading }}>
      {children}
    </NewsContext.Provider>
  );
};

export const useNews = () => {
  const context = useContext(NewsContext);
  if (!context) throw new Error('useNews must be used within a NewsProvider');
  return context;
};
