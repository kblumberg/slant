'use client';

import { usePathname } from 'next/navigation';
import Link from 'next/link';
import { cn } from '@/lib/utils'; // optional: utility for conditional classNames

const navItems = [
  { label: 'News', emoji: '📰', path: '/news' },
  { label: 'Analytics', emoji: '📊', path: '/analytics' },
  { label: 'Research', emoji: '🔬', path: '/research' },
];

const ChatPanel = () => {
  const pathname = usePathname();

  return (
    <div className="fixed top-[70px] right-0 h-full w-48 bg-[#101218] text-white border-r border-[#2a2e39] py-6 z-10">
      <nav className="flex flex-col items-start space-y-4 pl-6 pr-4">
        Chat
      </nav>
    </div>
  );
};

export default ChatPanel;
