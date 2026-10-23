import type { ReactNode } from "react";

function Svg(props: { children: ReactNode; size?: number }) {
  const size = props.size ?? 20;
  return <svg aria-hidden="true" fill="none" height={size} stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.8" viewBox="0 0 24 24" width={size}>{props.children}</svg>;
}

export const ArrowUp = () => <Svg><path d="M12 19V5M5 12l7-7 7 7" /></Svg>;

export const Chevron = () => <Svg size={16}><path d="M6 9l6 6 6-6" /></Svg>;

export const Check = () => <Svg size={18}><path d="M5 12.5l4.5 4.5L19 7.5" /></Svg>;

export const Compose = () => <Svg><path d="M12 4H6a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-6" /><path d="M17.5 3.5a2.1 2.1 0 0 1 3 3L12 15l-4 1 1-4z" /></Svg>;

export const Plus = () => <Svg><path d="M12 5v14M5 12h14" /></Svg>;

export const Sidebar = () => <Svg><rect height="16" rx="3" width="18" x="3" y="4" /><path d="M9 4v16" /></Svg>;

export const Stop = () => <svg aria-hidden="true" height="14" viewBox="0 0 14 14" width="14"><rect fill="currentColor" height="12" rx="2" width="12" x="1" y="1" /></svg>;

export const Trash = () => <Svg size={16}><path d="M4 7h16M10 11v6M14 11v6M6 7l1 12a2 2 0 0 0 2 2h6a2 2 0 0 0 2-2l1-12M9 7V4h6v3" /></Svg>;
