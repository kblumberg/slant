// types/News.ts

interface News {
	headline: string;
	key_takeaway: string;
	n_days: number;
	original_tweet: number;
	original_tweets: string[];
	projects: string[];
	tag: string;
	profile_image_url: string;
	score: number;
	time_ago_d: number;
	score_decayed: number;
	sources: string[];
	summary: string;
	timestamp: number;
	updated_at: Date;
	username: string;
	twitter_url: string;
}

export default News;