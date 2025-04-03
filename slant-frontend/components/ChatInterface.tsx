"use client";

import React, { useState, useRef, useEffect } from 'react';
import { Send } from 'lucide-react';
import DOMPurify from "dompurify";
import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';
import Message from '@/types/Message';
// import type { SeriesOptionsType } from 'highcharts';

import { motion, AnimatePresence } from "framer-motion";
import HighchartsData from '@/types/HighchartsData';
import HighchartsDataSeries from '@/types/HighchartsDataSeries';

// const texts = [
//   "Writing query",
//   "Executing query",
//   "Analyzing query",
//   "Summarizing data",
// ];


const QueryStatus = ({text}: {text: string}) => {
	// const [textIndex, setTextIndex] = useState(0);
	const [dotCount, setDotCount] = useState(0);
  
	// Handle dot animation every second
	useEffect(() => {
	  const dotInterval = setInterval(() => {
		setDotCount((prev) => (prev + 1) % 5);
	  }, 700);
	  return () => clearInterval(dotInterval);
	}, []);
  
	// Rotate text every 5 seconds (after a full cycle of dots)
	// useEffect(() => {
	//   if (dotCount === 0) {
	// 	setTextIndex((prev) => (prev + 1) % texts.length);
	//   }
	// }, [dotCount]);
  
	const baseText = text;
	const dots = ".".repeat(dotCount);
  
	return (
	//   <div className="w-full flex justify-center items-center h-20 font-medium text-xl text-white">
	  <div className="w-full flex justify-center items-center h-20 text-white">
		<AnimatePresence mode="wait">
		  <motion.div
			key={baseText}
			initial={{ opacity: 0, y: 10 }}
			animate={{ opacity: 1, y: 0 }}
			exit={{ opacity: 0, y: -10 }}
			transition={{ duration: 0.4 }}
			className="flex items-center"
		  >
			{baseText}
			<motion.span
			  key={dotCount} // animate each change in dots
			  initial={{ opacity: 1 }}
			  animate={{ opacity: 1 }}
			  exit={{ opacity: 0 }}
			  transition={{ duration: 0.3 }}
			  className="inline-block w-6"
			>
			  {dots}
			</motion.span>
		  </motion.div>
		</AnimatePresence>
	  </div>
	);
}


const ChatInterface = () => {
	const [messages, setMessages] = useState<Message[]>([]);
	const [inputText, setInputText] = useState('');
	const [isLoading, setIsLoading] = useState(false);
	// const [sessionId, setSessionId] = useState(() => crypto.randomUUID());
	const sessionId = crypto.randomUUID();
	const [status, setStatus] = useState('Analyzing query');
	const messagesEndRef = useRef<HTMLDivElement>(null);
	// const [status, setStatus] = useState('');
	const scrollToBottom = () => {
		messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
	};

	useEffect(() => {
		scrollToBottom();
		// setSessionId(crypto.randomUUID());
		// const eventSource = new EventSource('http://localhost:5000/stream');

		// eventSource.onmessage = (event) => {
		// //   setStatus(event.data);
		//   console.log("SSE update:");
		//   console.log(event);
		// };
	
		// eventSource.onerror = () => {
		//   eventSource.close();
		// };
	
		// return () => {
		//   eventSource.close();
		// };
	}, [messages]);

	const fetchSlant = async (query: string) => {
		try {
			// const url = "http://127.0.0.1:5000/ask_sharky"
			setStatus('Analyzing query');
			const params = new URLSearchParams({
				query,
				session_id: sessionId,
			});
			console.log(`params`)
			console.log(params.toString())
			// const eventSource = new EventSource(`http://127.0.0.1:5000/ask?${params.toString()}`);
			const eventSource = new EventSource(`http://127.0.0.1:5000/ask_analyst?${params.toString()}`);
			// const eventSource = new EventSource(`https://slant-backend-production.up.railway.app/ask?${params.toString()}`);

			eventSource.onmessage = (event) => {
				// console.log("SSE update:");
				// console.log(event);
				try {
				  const data = JSON.parse(event.data);
				//   console.log('eventSource.onmessage data');
				//   console.log(data);

					// Process the successful response
					// const msg = result.response
					// console.log(`data`)
					// console.log(result.data)
					// const data = result.data ? JSON.parse(JSON.stringify(result.data)) : null
					// console.log(`parsed data`)
					// console.log(data)

					if (data.status === "done") {
						setIsLoading(false);
						eventSource.close();
						setStatus('');
					}
					else if (data.status) {
						setStatus(data.status);
						// console.log(`data.status = ${data.status}`);
					}
					else if (data.response) {
						const parsedData = data.data;
						console.log('eventSource.onmessage data');
						console.log(data);
						let highchartsOptions = parsedData?.highcharts || {};
						const highchartsData = parsedData?.highcharts_data || [];
						console.log(`highchartsOptions`)
						console.log(highchartsOptions)
						console.log(`highchartsData`)
						console.log(highchartsData)
						try {
							if (typeof highchartsOptions === "string") {
								highchartsOptions = JSON.parse(highchartsOptions)
							}
							console.log(`highchartsOptions`)
							console.log(highchartsOptions)
							const highchartsDataParsed: HighchartsData = typeof highchartsData === "string" ? JSON.parse(highchartsData) : highchartsData;
							console.log(`highchartsDataParsed`)
							console.log(highchartsDataParsed)
							highchartsOptions['credits'] = {'enabled': false}
							const { x, series, mode } = highchartsDataParsed;

							// const scales = {};

							if (mode === "timestamp") {
								// Each data point already has its own `x` in the backend
								highchartsOptions.xAxis = { type: "datetime" };
								// highchartsOptions.series = series;

								// eslint-disable-next-line @typescript-eslint/no-explicit-any
								highchartsOptions.series = highchartsOptions.series.map((seriesConfig: any) => {
									const matchedSeries = series.find(
										(s: HighchartsDataSeries) => s.name === seriesConfig.column || s.name === seriesConfig.name
									);
									if (matchedSeries) {
										return {
											...seriesConfig,
											data: matchedSeries.data
										};
									}
									return seriesConfig;
								});
							  } else {
								highchartsOptions.xAxis = { categories: x };
								highchartsOptions.series = series.map((s: HighchartsDataSeries) => ({
								  ...s,
								  data: s.data
								}));
								console.log(`highchartsOptions`)
								console.log(highchartsOptions)
							}
							// const series = highchartsOptions.series.map(({ name, column }: { name: string, column: string }) => ({
							// 	name,
							// 	data: highchartsData.map((row: any) => row[column])
							// }));
							// console.log(`series`)
							// console.log(series)
							// highchartsOptions.series = series;
							// console.log(`highchartsOptions`)
							// console.log(highchartsOptions)
						} catch (error) {
							console.error('Error parsing highcharts options:', error);
						}
						const botMessage: Message = {
							id: (Date.now() + 1).toString(),
							content: data.response,
							data: {
								highcharts: highchartsOptions,
								highcharts_data: highchartsData,
								flipside_data: highchartsData
							},
							sender: 'bot',
							timestamp: new Date(),
						// data: null,
							query: query
						};
						setMessages(prev => [...prev, botMessage]);
					}
		  
				//   setMessages((prev) => [...prev, data]);
		  
				} catch (err) {
				  console.error("Failed to parse SSE message:", err);
				}
			};

			eventSource.onerror = (err) => {
			  console.log("SSE connection error:", err);
			  setIsLoading(false);
			  setStatus('');
			  eventSource.close();
			};

			return () => {
				eventSource.close();
			};

			const url = "http://127.0.0.1:5000/ask"
			// const url = "https://slant-backend-production.up.railway.app/ask_sharky"
			const body = {
				"query": query,
				"session_id": sessionId
			}
			const response = await fetch(url, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
				},
				body: JSON.stringify(body)
			})
			console.log(`response`)
			console.log(response)
			const result = await response.json()

			if (result.error) {
				console.error('Error fetching data:', result.error);
				const msg = 'Sorry, I couldn\'t understand your question. Please try again.'
				const botMessage: Message = {
					id: (Date.now() + 1).toString(),
					content: msg,
					sender: 'bot',
					data: null,
					timestamp: new Date(),
					// data: null,
					query: query
				};
				setMessages(prev => [...prev, botMessage]);
			} else {
				// Process the successful response
				const msg = result.response
				console.log(`data`)
				console.log(result.data)
				const data = result.data ? JSON.parse(JSON.stringify(result.data)) : null
				console.log(`parsed data`)
				console.log(data)
				const botMessage: Message = {
					id: (Date.now() + 1).toString(),
					content: msg,
					data: data,
					sender: 'bot',
					timestamp: new Date(),
					// data: null,
					query: query
				};
				setMessages(prev => [...prev, botMessage]);
			}
		}
		catch (error) {
			console.error('Error fetching data:', error);
			const msg = 'Sorry, I couldn\'t understand your question. Please try again.'
			const botMessage: Message = {
				id: (Date.now() + 1).toString(),
				content: msg,
				sender: 'bot',
				data: null,
				timestamp: new Date(),
				query: query
			};
			setMessages(prev => [...prev, botMessage]);
		}
		setIsLoading(false);
	}

	const handleSend = async () => {
		if (!inputText.trim()) return;

		const curText = inputText;

		const userMessage: Message = {
			id: Date.now().toString(),
			content: curText,
			sender: 'user',
			data: null,
			timestamp: new Date(),
			query: ''
		};

		setMessages(prev => [...prev, userMessage]);
		setInputText('');
		setIsLoading(true);

		fetchSlant(curText);
	};

	const handleKeyPress = (e: React.KeyboardEvent) => {
		if (e.key === 'Enter' && !e.shiftKey) {
			e.preventDefault();
			handleSend();
		}
	};

	// console.log(`messages`)
	// console.log(messages)

	if (messages.length == 0) {
		return(
			<div className="flex items-center justify-center min-h-screen sm:p-12 flex flex-col items-center justify-center p-6">
				<div className="flex flex-col h-full w-full sm:w-3/5 m-auto rounded-md pb-20">
					<div className='text-6xl font-semibold text-white p-10 text-center'>Ask for alpha</div>

					<div className="flex space-x-4">
						<textarea
							value={inputText}
							onChange={(e) => setInputText(e.target.value)}
							onKeyPress={handleKeyPress}
							placeholder="Ask Slant..."
							className="flex-1 rounded resize-none border p-3 focus:outline-none focus:ring-2 focus:ring-[#1373eb] focus:border-transparent"
							rows={1}
						/>
						<button
							onClick={handleSend}
							disabled={!inputText.trim() || isLoading}
							className="bg-slant-blue text-white p-3 rounded-lg hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-slant-blue focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed"
						>
							<Send className="w-5 h-5" />
						</button>
					</div>
				</div>
			</div>
		)
	}
	const messageDivs = messages.map((message) => {
		const isUser = message.sender === 'user';
		if (isUser) {
			return (
				<div key={message.id} className={`flex justify-end`}>
					<div className={`chat-container user-message max-w-[70%] rounded-lg p-3 ${isUser ? 'text-white' : ''}`}>
						<div className="text-base">{message.content.split('\n').map((line, i) => (
							<p key={i}>{line}</p>
						))}</div>
					</div>
				</div>
			)
		}
		else {
			const content = DOMPurify.sanitize(message.content)
			// Add target="_blank" to all links in the content
			const contentWithNewTabLinks = content.replace(/<a\s+(?:[^>]*?)href=/g, '<a target="_blank" href=');
			// console.log(`typeof message.data?.highcharts = ${typeof message.data?.highcharts}`)
			const highcharts = message.data?.highcharts;
			const highchartsOptions = highcharts.series && highcharts.series.length > 0 ? highcharts : null;
			// const highchartsData = message.data?.highcharts_data;
			return (
				<div key={message.id} className={`flex justify-start`}>
					<div className={`chat-container max-w-[85%] rounded-lg p-3 ${isUser ? 'text-black' : ''}`}>
						{message.data && message.data.highcharts && (
							<div>
								<HighchartsReact highcharts={Highcharts} options={highchartsOptions} />
							</div>
						)}
						<div className="text-base" dangerouslySetInnerHTML={{ __html: contentWithNewTabLinks }} />
					</div>
				</div>
			)
		}
	})
	return (
		<div className="sm:p-12 pt-10 pb-14 flex flex-col items-center justify-center">
		<div className="flex flex-col h-full w-full sm:w-3/5 mx-auto rounded-md">
			<div className="flex-1 overflow-y-auto p-4 space-y-4">
				{messageDivs}
				{isLoading && (
					<div className="flex justify-start">
						<div className="rounded-lg p-3 max-w-[70%]">
							{/* <div className="flex space-x-2">
								<div className="w-2 h-2 bg-gray-300 rounded-full animate-bounce" />
								<div className="w-2 h-2 bg-gray-300 rounded-full animate-bounce delay-100" />
								<div className="w-2 h-2 bg-gray-300 rounded-full animate-bounce delay-200" />
							</div> */}
							<QueryStatus text={status} />
						</div>
					</div>
				)}
				<div ref={messagesEndRef} />
			</div>
			<div className="fixed bottom-0 left-0 right-0 sm:w-3/5 mx-auto">
				<div className="p-4 bg-gradient-to-t from-[#101218] to-transparent">
				</div>
				<div className="bg-[#101218] p-4 pt-2 rounded-b-md">
					<div className="flex space-x-4">
						<textarea
							value={inputText}
							onChange={(e) => setInputText(e.target.value)}
							onKeyPress={handleKeyPress}
							placeholder="Ask Slant..."
							className="flex-1 rounded resize-none border p-2 focus:outline-none focus:ring-2 focus:ring-[#1373eb] focus:border-transparent"
							rows={1}
						/>
						<button
							onClick={handleSend}
							disabled={!inputText.trim() || isLoading}
							className="bg-slant-blue text-white p-2 rounded-lg hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-slant-blue focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed"
						>
							<Send className="w-5 h-5" />
						</button>
					</div>
				</div>
			</div>
		</div>
		</div>
	);
};

export default ChatInterface;