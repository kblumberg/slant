'use client';

import { useNews } from '@/context/NewsContext';
import NewsPage from '@/components/NewsPage';

export default function NewsRoute() {
  const { news, loading } = useNews();

  if (loading || !news) {
    return <div className="p-8 text-gray-400">Loading news...</div>;
  }

  return <NewsPage news={news} />;
}
