import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

/** Merges conditional classes without retaining conflicting Tailwind utilities. */
export const cn = (...inputs: ReadonlyArray<ClassValue>): string => twMerge(clsx(inputs));
