import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"
// import Message from "@/types/ChatData"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

// export function getHighchartsOptions(message: Message) {

//   return message.data?.highcharts
// }