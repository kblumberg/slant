
interface News {
	headline: string;
	key_takeaways: string[];
	n_days: number;
	original_tweet: number;
	original_tweets: string[];
	profile_image_url: string;
	score: number;
	sources: string[];
	summary: string;
	timestamp: number;
	source: string;
	updated_at: Date;
	username: string;
}

export default News;