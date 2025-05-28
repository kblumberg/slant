"use client";

import News from '@/types/News';
import React, { useState } from 'react';


interface NewsPageProps {
  news: News[];
}

const NewsPage = ({ news }: NewsPageProps) => {
  const [filter, setFilter] = useState<1 | 7 | 30>(1);

  const filteredNews = news.filter((item) => item.n_days == filter);

  return (
    <div className="sm:p-12 pt-0 pb-14 flex flex-col items-center w-full">
      <h1 className="text-3xl font-bold text-white mb-6">Trending News</h1>

      <div className="flex space-x-3 mb-8">
        {[1, 7, 30].map((day) => (
          <button
            key={day}
            className={`px-4 py-2 rounded-full border text-sm font-semibold transition ${
              filter === day
                ? 'bg-[var(--primary-color)] text-white border-transparent'
                : 'bg-transparent text-[var(--secondary-color)] border-[var(--secondary-color)]'
            }`}
            onClick={() => setFilter(day as 1 | 7 | 30)}
          >
            {day == 1 ? '24h' : `${day}d`}
          </button>
        ))}
      </div>

      <div className="grid gap-6 w-full max-w-4xl px-4">
        {filteredNews.map((item) => {
			console.log('item');
			console.log(item);
			
			const hoursAgo = Math.round((Date.now() / 1000 - item.timestamp) / 3600);
			const daysAgo = Math.round((Date.now() / 1000 - item.timestamp) / 3600 / 24);
			const timeAgo = hoursAgo < 24 ? `${hoursAgo}h ago` : `${daysAgo}d ago`;
			const normalizedImgUrl = item.profile_image_url?.replace('_normal', '');
			return (
				<div
					key={item.headline}
					className="bg-[#1a1d24] rounded-2xl sm:p-6 p-3 shadow-md hover:shadow-lg transition-shadow border border-[#2a2e39]"
				>
					<div className="flex items-start space-x-4">
					{item.profile_image_url && (
						<>
						<div className="hidden sm:contents">
								<img
								src={normalizedImgUrl}
								alt={item.username}
								className="w-10 h-10 rounded-full"
								/>
						</div>
						{/* <div className="sm:hidden w-full flex flex-row">
							<div className="flex flex-col">
								<img
								src={item.profile_image_url}
								alt={item.username}
								className="w-10 h-10 rounded-full"
								/>
								<br/>
						</div>
						</div> */}
						</>
					)}
					<div>
						<h2 className="text-xl font-bold text-white mb-1">{item.headline}</h2>
						<p className="text-sm text-gray-300 mb-2">{item.summary}</p>
						<ul className="list-disc list-inside text-sm text-gray-400 space-y-1 hidden sm:contents">
						{item.key_takeaways.map((takeaway, idx) => (
							<li key={idx}><a href={item.sources[idx]} target="_blank">{takeaway}</a></li>
						))}
						</ul>
						<p className="mt-3 text-xs text-gray-500">
						@{item.username} • {timeAgo}
						</p>
					</div>
					</div>
				</div>
			)
        })}

        {filteredNews.length === 0 && (
          <div className="text-gray-400 text-center mt-10">No news for the selected time range.</div>
        )}
      </div>
        </div>
  );
};

export default NewsPage;