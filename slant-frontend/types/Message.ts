import ChatData from "./ChatData";

interface Message {
	id: string;
	content: string;
	sender: 'user' | 'bot';
	timestamp: Date;
	data: ChatData[] | null;
	query: string;
}

export default Message;