'use client';

import { usePathname } from 'next/navigation';
import Link from 'next/link';
import { cn } from '@/lib/utils'; // optional: utility for conditional classNames


const navItems = [
  { label: 'News', emoji: '📰', path: '/news' },
  { label: 'Analytics', emoji: '📊', path: '/analytics' },
  { label: 'Research', emoji: '🔬', path: '/research' },
];

const SidePanel = () => {
  const pathname = usePathname();

  return (
    <div className="fixed top-[70px] left-0 h-full bg-[#101218] text-white border-r border-[#2a2e39] py-6 z-10">
      <nav className="flex flex-col items-start space-y-4">
        {navItems.map(({ label, emoji, path }) => (
          <Link
            key={label}
            href={path}
            className={cn(
              'flex items-center text-sm font-medium py-2 px-3 pl-6 pr-4 rounded-lg transition-colors hover:bg-[#2a2e39] w-full sm:px-12 sm:py-4',
              pathname === path ? 'bg-[#2a2e39] text-white' : 'text-gray-400'
            )}
          >
            <span className="mr-2 text-lg">{emoji}</span>
            <span className="hidden sm:inline">
            {label}
            </span>
          </Link>
        ))}
      </nav>
    </div>
  );
};

export default SidePanel;
