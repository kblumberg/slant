// components/NewsPage.tsx
"use client";

import News from '@/types/News';
import React, { useState, useMemo } from 'react';

interface NewsPageProps {
  news: News[];
}

const NewsPage = ({ news }: NewsPageProps) => {
  const [timeFilter, setTimeFilter] = useState<24 | 7 | 30 | 90>(24);
  const [projectFilter, setProjectFilter] = useState<string>('All');
  const [tagFilter, setTagFilter] = useState<string>('All');
  const [sortBy, setSortBy] = useState<'Trending' | 'Smart Engagement' | 'Newest'>('Trending');

  // Compute project and tag options
  const projectOptions = useMemo(() => {
    const projectsSet = new Set<string>();
    news.forEach((item) => item.projects.forEach((p) => projectsSet.add(p)));
    return Array.from(projectsSet).sort();
  }, [news]);

  const tagOptions = useMemo(() => {
    const tagsSet = new Set<string>();
    news.forEach((item) => tagsSet.add(item.tag));
    return Array.from(tagsSet).sort();
  }, [news]);

  // Filter and sort news
  const filteredNews = useMemo(() => {
    const cutoffTimestamp = Date.now() / 1000 - 
      (timeFilter === 24 ? 24 * 3600 : timeFilter * 24 * 3600);

    let result = news.filter(item => item.timestamp >= cutoffTimestamp);

    if (projectFilter !== 'All') {
      result = result.filter(item => item.projects.includes(projectFilter));
    }

    if (tagFilter !== 'All') {
      result = result.filter(item => item.tag === tagFilter);
    }

    if (sortBy === 'Trending') {
      result = result.sort((a, b) => b.score_decayed - a.score_decayed);
    } else if (sortBy === 'Smart Engagement') {
      result = result.sort((a, b) => b.score - a.score);
    } else if (sortBy === 'Newest') {
      result = result.sort((a, b) => b.timestamp - a.timestamp);
    }

    return result;
  }, [news, timeFilter, projectFilter, tagFilter, sortBy]);

  return (
    <div className="sm:p-12 pt-0 pb-14 flex flex-col items-center w-full">
      <h1 className="text-3xl font-bold text-white mb-6">Trending News</h1>

      {/* Filters */}
      <div className="flex flex-wrap justify-center gap-4 mb-8 w-full max-w-5xl px-4">
        {/* Time Filter */}
        <div className="flex items-center gap-2">
          <label className="text-gray-300 text-sm">Time:</label>
          {[24, 7, 30, 90].map((t) => (
            <button
              key={t}
              onClick={() => setTimeFilter(t as 24 | 7 | 30 | 90)}
              className={`px-3 py-1 rounded-full text-sm border ${
                timeFilter === t ? 'bg-[var(--primary-color)] text-white border-transparent' : 'border-gray-500 text-gray-300'
              }`}
            >
              {t === 24 ? '24h' : `${t}d`}
            </button>
          ))}
        </div>

        {/* Project Filter */}
        <div className="flex items-center gap-2">
          <label className="text-gray-300 text-sm">Project:</label>
          <select
            value={projectFilter}
            onChange={(e) => setProjectFilter(e.target.value)}
            className="bg-[#1a1d24] border border-gray-500 text-gray-300 text-sm rounded px-2 py-1"
          >
            <option value="All">All</option>
            {projectOptions.map((p) => (
              <option key={p} value={p}>{p}</option>
            ))}
          </select>
        </div>

        {/* Tag Filter */}
        <div className="flex items-center gap-2">
          <label className="text-gray-300 text-sm">Tag:</label>
          <select
            value={tagFilter}
            onChange={(e) => setTagFilter(e.target.value)}
            className="bg-[#1a1d24] border border-gray-500 text-gray-300 text-sm rounded px-2 py-1"
          >
            <option value="All">All</option>
            {tagOptions.map((tag) => (
              <option key={tag} value={tag}>{tag}</option>
            ))}
          </select>
        </div>

        {/* Sort By */}
        <div className="flex items-center gap-2">
          <label className="text-gray-300 text-sm">Sort by:</label>
          <select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value as 'Trending' | 'Smart Engagement' | 'Newest')}
            className="bg-[#1a1d24] border border-gray-500 text-gray-300 text-sm rounded px-2 py-1"
          >
            <option value="Trending">Trending</option>
            <option value="Smart Engagement">Smart Engagement</option>
            <option value="Newest">Newest</option>
          </select>
        </div>
      </div>

      {/* News List */}
      <div className="grid gap-6 w-full max-w-4xl px-4">
        {filteredNews.map((item) => {
  const hoursAgo = Math.round((Date.now() / 1000 - item.timestamp) / 3600);
  const daysAgo = Math.round((Date.now() / 1000 - item.timestamp) / 3600 / 24);
  const timeAgo = hoursAgo < 24 ? `${hoursAgo}h ago` : `${daysAgo}d ago`;
  const normalizedImgUrl = item.profile_image_url?.replace('_normal', '');

  return (
    <div
      key={item.headline + item.timestamp} // more unique key
      onClick={() => window.open(item.twitter_url, '_blank')}
      className="bg-[#1a1d24] rounded-2xl sm:p-6 p-3 shadow-md hover:shadow-lg transition-shadow border border-[#2a2e39] cursor-pointer hover:bg-[#22252e]"
    >
      <div className="flex items-start space-x-4">
        {item.profile_image_url && (
          <img
            src={normalizedImgUrl}
            alt={item.username}
            className="w-10 h-10 rounded-full"
          />
        )}
        <div className="flex-1">
          {/* Headline */}
          <h2 className="text-xl font-bold text-white mb-2">{item.headline}</h2>

          {/* Meta line */}
          <div className="flex flex-wrap text-xs text-gray-400 mb-3 gap-3">
            {item.projects.length > 0 && (
              <div>🛠️ <strong>Projects:</strong> {item.projects.join(', ')}</div>
            )}
            {item.tag && (
              <div>🏷️ <strong>Tag:</strong> {item.tag}</div>
            )}
            {item.score && (
              <div>🚀 <strong>Smart Engagement:</strong> {Math.round(item.score)} / 100</div>
            )}
            <div>🕒 @{item.username} • {timeAgo}</div>
          </div>

          {/* Summary */}
          <p className="text-sm text-gray-300 mb-2">{item.summary}</p>

          {/* Key takeaway */}
          <p className="text-sm text-gray-400 space-y-1 mb-2">{item.key_takeaway}</p>
        </div>
      </div>
    </div>
  );
})}


        {filteredNews.length === 0 && (
          <div className="text-gray-400 text-center mt-10">No news for the selected filters.</div>
        )}
      </div>
    </div>
  );
};

export default NewsPage;
