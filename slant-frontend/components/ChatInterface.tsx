// components/ChatInterface.tsx
"use client";

import React, { useState, useRef, useEffect } from 'react';
import { Send } from 'lucide-react';
import DOMPurify from "dompurify";
import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';
import Message from '@/types/Message';
// import type { SeriesOptionsType } from 'highcharts';

import { motion, AnimatePresence } from "framer-motion";
// import HighchartsData from '@/types/HighchartsData';
// import HighchartsDataSeries from '@/types/HighchartsDataSeries';
import ChatData from '@/types/ChatData';
// import 'highcharts/modules/exporting';

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

interface ChatInterfaceProps {
	userId: string;
	conversationId: string;
}


const ChatInterface = ({ userId, conversationId }: ChatInterfaceProps) => {
	const [messages, setMessages] = useState<Message[]>([]);
	const [inputText, setInputText] = useState('');
	// const [inputHeight, setInputHeight] = useState(0);
	const [isLoading, setIsLoading] = useState(false);
	const [status, setStatus] = useState('Analyzing query');
	const messagesEndRef = useRef<HTMLDivElement>(null);
	const chartRef = useRef<HighchartsReact.RefObject>(null);
	const hasExportedRef = useRef(false);
	// const [status, setStatus] = useState('');
	const scrollToBottom = () => {
		messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
	};
	const textareaRef = useRef<HTMLTextAreaElement>(null);

	const safeExportChartToS3 = (chart: Highcharts.Chart) => {
		if (hasExportedRef.current) return;
		hasExportedRef.current = true;
		exportChartToS3(chart);
	};

	const exportChartToS3 = async (chart: Highcharts.Chart) => {
		console.log(`exportChartToS3`)
		// chartRef.current = { chart };

		try {
			// Get the Highcharts chart instance
			// const svg = Highcharts.charts[0]?.getSVG();
			const svg = chart.getSVG();
			if (!svg) {
				console.error("Could not get SVG from chart");
				return;
			}
			const canvas = document.createElement('canvas');
			canvas.width = 600;  // adjust as needed
			canvas.height = 400;
			const ctx = canvas.getContext('2d');
	
			const img = new Image();
			const svgBlob = new Blob([svg], { type: 'image/svg+xml;charset=utf-8' });
			const url = URL.createObjectURL(svgBlob);
	
			img.onload = async function () {
				ctx?.drawImage(img, 0, 0);
				const pngDataUrl = canvas.toDataURL('image/png');
	
				// Convert base64 to blob
				const byteString = atob(pngDataUrl.split(',')[1]);
				const mimeString = pngDataUrl.split(',')[0].split(':')[1].split(';')[0];
				const ab = new ArrayBuffer(byteString.length);
				const ia = new Uint8Array(ab);
				for (let i = 0; i < byteString.length; i++) {
					ia[i] = byteString.charCodeAt(i);
				}
				const blob = new Blob([ab], { type: mimeString });
	
				// Get a presigned URL from your backend
				const res = await fetch('http://127.0.0.1:5000/api/get-upload-url', {
					method: 'POST',
					headers: { 'Content-Type': 'application/json' },
					body: JSON.stringify({ filename: `chart-${conversationId}.png` })
				});
				const { uploadUrl, fileUrl } = await res.json();
				console.log(`uploadUrl = ${uploadUrl}`)
				console.log(`fileUrl = ${fileUrl}`)
	
				// Upload to S3
				await fetch(uploadUrl, {
					method: 'PUT',
					headers: { 'Content-Type': 'image/png' },
					body: blob
				});
	
				// console.log('Uploaded to:', fileUrl);
				// alert('Chart PNG uploaded to S3!');
	
				URL.revokeObjectURL(url);
			};
	
			img.src = url;
	
		} catch (error) {
			console.error('Error exporting chart to S3:', error);
		}
	}
	
	

	const exportToSql = (sqlString: string) => {
		console.log(`sqlString`)
		console.log(sqlString)
		// const sqlString = "-- Example SQL query\nSELECT * FROM example_table WHERE value > 100;";
	
		const blob = new Blob([sqlString], { type: "text/sql" });
		const url = URL.createObjectURL(blob);
	
		const link = document.createElement("a");
		link.href = url;
		link.download = "query.sql";
		document.body.appendChild(link);
		link.click();
		document.body.removeChild(link);
	
		URL.revokeObjectURL(url);
	};
	

	const exportToCsv = (csvString: string) => {
		console.log(`csvString`)
		console.log(csvString)
		// const csvString = "column1,column2,column3\nvalue1,value2,value3\nvalue4,value5,value6";
		// const csvString = csvJson.map((row: any) => Object.values(row).join(',')).join('\n');
		const blob = new Blob([csvString], { type: "text/csv" });
		const url = URL.createObjectURL(blob);
	
		const link = document.createElement("a");
		link.href = url;
		link.download = "data.csv";
		document.body.appendChild(link);
		link.click();
		document.body.removeChild(link);
	
		URL.revokeObjectURL(url);
	};
	


	const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
		setInputText(e.target.value);
		if (textareaRef.current) {
		  textareaRef.current.style.height = 'auto'; // reset height
		  textareaRef.current.style.height = `${textareaRef.current.scrollHeight}px`; // set to scrollHeight
		//   setInputHeight(textareaRef.current.scrollHeight);
		}
	  };
	
	  useEffect(() => {
		if (textareaRef.current) {
		  textareaRef.current.style.height = `${textareaRef.current.scrollHeight}px`;
		}
		import('highcharts/modules/exporting').then((module) => {
			module.default(Highcharts);
		});
	  }, []);

	useEffect(() => {
		scrollToBottom();
		// setconversationId(crypto.randomUUID());
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
				conversation_id: conversationId,
				user_id: userId,
			});
			console.log(`params`)
			console.log(params.toString())
			// const eventSource = new EventSource(`http://127.0.0.1:5000/ask?${params.toString()}`);
			const eventSource = new EventSource(`http://127.0.0.1:5000/ask_analyst?${params.toString()}`);
			// const eventSource = new EventSource(`http://slant-backend-production.up.railway.app/ask_analyst?${params.toString()}`);
			// const eventSource = new EventSource(`https://slant-backend-production.up.railway.app/ask?${params.toString()}`);

			eventSource.onmessage = (event) => {
				console.log("SSE update:");
				console.log(event);
				try {
				  const data = JSON.parse(event.data);
				  console.log('eventSource.onmessage data');
				  console.log(data);

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
						const flipsideData = parsedData?.flipside_sql_query_result;
						const flipsideSql = parsedData?.flipside_sql_query;
						let highchartsOptionsList = parsedData?.highcharts || [];
						// let highchartsDataList = parsedData?.highcharts_datas || [];
						console.log(`highchartsOptionsList`)
						console.log(highchartsOptionsList)
						// console.log(`highchartsDataList`)
						// console.log(highchartsDataList)
						const chatData: ChatData[] = [];
						try {
							if (typeof highchartsOptionsList === "string") {
								highchartsOptionsList = JSON.parse(highchartsOptionsList)
							}
							// if (typeof highchartsDataList === "string") {
							// 	highchartsDataList = JSON.parse(highchartsDataList)
							// }
							console.log(`highchartsOptionsList`)
							console.log(highchartsOptionsList)
							for (let i = 0; i < highchartsOptionsList.length; i++) {
								const highchartsOptions = highchartsOptionsList[i];
								// const highchartsData = highchartsDataList[i];
								console.log(`highchartsOptions`)
								console.log(highchartsOptions)
								console.log(`highchartsOptions.xAxis`)
								console.log(highchartsOptions.xAxis)
								// const highchartsDataParsed: HighchartsData = typeof highchartsData === "string" ? JSON.parse(highchartsData) : highchartsData;
								// console.log(`highchartsDataParsed`)
								// console.log(highchartsDataParsed)
								highchartsOptions['credits'] = {'enabled': false}
								// const { x, series, mode } = highchartsDataParsed;
								// console.log(`highcharts mode = ${mode}`)
								// console.log(`highcharts series`)
								// console.log(series)
								// console.log(`highcharts x`)
								// console.log(x)
	
								// const scales = {};
	
								// if (mode === "timestamp") {
								// 	// Each data point already has its own `x` in the backend
								// 	highchartsOptions.xAxis = { type: "datetime" };
								// 	// highchartsOptions.series = series;
	
								// 	// eslint-disable-next-line @typescript-eslint/no-explicit-any
								// 	highchartsOptions.series = highchartsOptions.series.map((seriesConfig: any) => {
								// 		const matchedSeries = series.find(
								// 			(s: HighchartsDataSeries) => s.name.toLowerCase() === seriesConfig.column.toLowerCase() || s.name.toLowerCase() === seriesConfig.name.toLowerCase()
								// 		);
								// 		if (matchedSeries) {
								// 			return {
								// 				...seriesConfig,
								// 				data: matchedSeries.data
								// 			};
								// 		}
								// 		return seriesConfig;
								// 	});
								//   } else {
								// 	// highchartsOptions.xAxis = { categories: x };
								// 	highchartsOptions.series = series.map((s: HighchartsDataSeries) => ({
								// 	  ...s,
								// 	  data: s.data
								// 	}));
								// 	console.log(`highchartsOptions`)
								// 	console.log(highchartsOptions)
								// }
								chatData.push({
									highcharts: highchartsOptions,
									highcharts_data: null,
									flipside_data: flipsideData,
									flipside_sql: flipsideSql,
									// highcharts_data: highchartsData,
									// flipside_data: highchartsData
								});
								// const series = highchartsOptions.series.map(({ name, column }: { name: string, column: string }) => ({
								// 	name,
								// 	data: highchartsData.map((row: any) => row[column])
								// }));
								// console.log(`series`)
								// console.log(series)
								// highchartsOptions.series = series;
								// console.log(`highchartsOptions`)
								// console.log(highchartsOptions)
							}

						} catch (error) {
							console.error('Error parsing highcharts options:', error);
						}
						console.log(`chatData`)
						console.log(chatData)
						const botMessage: Message = {
							id: (Date.now() + 1).toString(),
							content: data.response,
							data: chatData,
							sender: 'bot',
							timestamp: new Date(),
						// data: null,
							query: query
						};
						console.log(`botMessage`)
						console.log(botMessage)
						setMessages(prev => [...prev, botMessage]);
						console.log(`252`)
					}
		  
				//   setMessages((prev) => [...prev, data]);
		  
				} catch (err) {
				  console.error("Failed to parse SSE message:", err);
				}
			};

			eventSource.onerror = (err) => {
			  console.log("SSE connection error:");
			  console.log(err);
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
				"conversation_id": conversationId
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
		const curText = inputText.trim();
		if (!curText) return;

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
		// const placeholder = inputText == '' || true ? <div className="text-black text-sm"><ol>
		// 	I can research any topic on Solana and provide in-depth data analysis. For best results:
		// 		<li>Include example transaction ids with description</li>
		// 		<li>Any relevant program ids, addresses, etc</li>
		// 		<li>Details on timeframe</li>
		// 	</ol>
		// </div> : null;
		return(
			<div className="flex items-center justify-center min-h-screen sm:p-12 flex flex-col items-center justify-center p-6">
				<div className="flex flex-col h-full w-full sm:w-3/5 m-auto rounded-md pb-20">
					<div className='text-6xl font-semibold text-white p-10 text-center'>Ask for alpha</div>
					<div className="relative flex space-x-4">
					<div className="bg-white flex w-full h-full text-black">
					{inputText === '' ? (
						<div
							className={`transition-all duration-300 ease-in-out bg-transparent text-gray-600 absolute bottom-[20px] left-8 max-w-3/4 z-1`}
							// style={{ top: `${inputHeight - 90}px` }}
						>
							<ul className="list-disc list-inside">
								<li>Include example transaction ids with description</li>
								<li>Any relevant program ids, addresses, etc</li>
								<li>Details on timeframe</li>
							</ul>
						</div>
						) : (
							<div
								className="transition-all duration-300 ease-in-out bg-transparent text-gray-600 absolute bottom-[65px] left-4 max-w-3/4 z-1"
								// style={{ top: `${inputHeight - 90}px` }}
							>
								Provide as many details as possible for best results
							</div>
						)
					}
					<textarea
						ref={textareaRef}
						value={inputText}
						onChange={handleChange}
						onKeyPress={handleKeyPress}
						placeholder="I can research any topic on Solana and provide in-depth data analysis. For best results:"
						className={`z-1 placeholder-gray-600 bg-transparent min-h-36 flex-1 rounded resize-none border p-4 pb-[${100}px] focus:outline-none focus:ring-2 focus:ring-[#1373eb] focus:border-transparent`}
						rows={1}
					/>
					</div>
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
	const messageDivs = messages.map( (message) => {
		const isUser = message.sender === 'user';
		if (isUser) {
			return (
				<div key={message.id} className={`flex justify-end break-words whitespace-normal`}>
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
			const chatDataDivs = message?.data || [];

			const flipsideSql = chatDataDivs.length > 0 ? chatDataDivs[0].flipside_sql : null;
			const flipsideData = chatDataDivs.length > 0 ? chatDataDivs[0].flipside_data : null;
			
			const highchartsDivs = chatDataDivs.map((chatData: ChatData, index: number) => {
				const highcharts = chatData.highcharts;
				const highchartsOptions = highcharts.series && highcharts.series.length > 0 ? highcharts : null;
				// if (highchartsOptions) {
				// 	highchartsOptions = {
				// 		...highchartsOptions,
				// 		yAxis: {
				// 			...highchartsOptions.yAxis,
				// 			labels: {
				// 			...((highchartsOptions.yAxis && highchartsOptions.yAxis.labels) || {}),
				// 			formatter: function () {
				// 				if (this.value >= 1e9) {
				// 				return (this.value / 1e9) + 'B';
				// 				} else if (this.value >= 1e6) {
				// 				return (this.value / 1e6) + 'M';
				// 				} else if (this.value >= 1e3) {
				// 				return (this.value / 1e3) + 'K';
				// 				}
				// 				return this.value;
				// 			}
				// 			}
				// 		}
				// 	};
				// }
				// const highchartsData = message.data?.highcharts_data;
				const divKey = `${message.id}-${index}`;
				return (
					<div key={divKey}>
						<HighchartsReact highcharts={Highcharts} options={highchartsOptions} ref={chartRef} callback={safeExportChartToS3}/>
					</div>
				)
			})
			const buttonsDiv = highchartsDivs.length === 0 ? null : (
				<div className="flex space-x-2 mt-2">
					<button
						onClick={() => exportToSql(flipsideSql)}
						className=" bg-slant-dark-blue px-4 py-2 rounded hover:bg-blue-700"
					>
						<span className="font-size-2 pr-2">📝</span>Export SQL
					</button>
					<button
						onClick={() => exportToCsv(flipsideData)}
						className="bg-slant-dark-blue px-4 py-2 rounded hover:bg-blue-700"
					>
						<span className="pr-2">📊</span>Export CSV
					</button>
				</div>
			);
			
			return(
				
				<div key={message.id} className={`flex justify-start`}>
					<div className={`chat-container max-w-[85%] rounded-lg p-3 ${isUser ? 'text-black' : ''}`}>
						{highchartsDivs}
						<div className="text-base" dangerouslySetInnerHTML={{ __html: contentWithNewTabLinks }} />
						{buttonsDiv}
					</div>
				</div>

			);
		}
	})
	return (
		<div className="sm:p-12 pt-10 pb-14 flex flex-col items-center justify-center">
		<div className="flex flex-col h-full w-full sm:w-3/5 mx-auto rounded-md">
			<div className="flex-1 overflow-y-auto pt-8 p-4 space-y-4">
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
							className="text-black flex-1 rounded resize-none border p-2 focus:outline-none focus:ring-2 focus:ring-[#1373eb] focus:border-transparent"
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