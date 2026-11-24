import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';
import type { Followed } from './types';

export function getFollowedKey(followed: Followed): number {
	if (followed.type == 'favorite') {
		return followed.fid;
	} else if (followed.type == 'collection') {
		return followed.sid;
	} else {
		return followed.mid;
	}
}

export function cn(...inputs: ClassValue[]) {
	return twMerge(clsx(inputs));
}

export function parseDailyDownloadLimit(value: string): number {
	const input = value.trim();
	if (input === '') return 0;

	const limit = Number(input);
	if (!/^\d+$/.test(input) || limit > 4294967295) {
		throw new Error('每日下载上限请输入 0 到 4294967295 之间的整数');
	}
	return limit;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type WithoutChild<T> = T extends { child?: any } ? Omit<T, 'child'> : T;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type WithoutChildren<T> = T extends { children?: any } ? Omit<T, 'children'> : T;
export type WithoutChildrenOrChild<T> = WithoutChildren<WithoutChild<T>>;
export type WithElementRef<T, U extends HTMLElement = HTMLElement> = T & { ref?: U | null };
