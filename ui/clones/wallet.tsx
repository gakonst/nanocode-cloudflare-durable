import { Wallet } from "nanocode-ts/machines";
import { type MouseEvent, useState } from "react";
import { login, logout, setTab, useWallet, where } from "./hand.ts";

export function WalletButton(props: { className?: string }) {
  const wallet = useWallet();
  const [hint, setHint] = useState(false);
  if (wallet.connected) return <button className={props.className} onClick={() => void logout()} title="Log out and revoke the key" type="button">{Wallet.short(wallet.connected.address)}</button>;
  return (
    <span className="nc-wallet">
      <button className={props.className} disabled={wallet.busy} onClick={() => void login()} type="button">{wallet.busy ? "Connecting…" : "Connect Tempo Wallet"}</button>
      {wallet.device && <a className="nc-wallet-code" href={wallet.device.verificationUriFull ?? wallet.device.verificationUri} onClick={event => setHint(guard(event))} rel="noreferrer" target="_blank">Approve code {wallet.device.userCode}</a>}
      {hint && <span className="nc-wallet-error">⌘-click the code (Ctrl-click on Windows) to approve it in a normal tab, or open it on your phone.</span>}
      {wallet.error && <span className="nc-wallet-error">{wallet.error}</span>}
    </span>
  );
}

export function HandBadge(props: { className?: string }) {
  const { hand, tab } = useWallet();
  return <button className={props.className} data-state={tab ? hand.state : "server"} onClick={() => setTab(!tab)} title={tab ? "Code Mode cells run in this browser tab with your wallet: Python, native WebAssembly and a fetch that pays MPP services. Click to run them on Cloudflare again." : "Code Mode cells run in the Durable Object beside the agent. Click to run them in this tab instead, for Python, native WebAssembly and a fetch that pays MPP services from your wallet."} type="button">{where(hand, tab)}</button>;
}

export function Cloudflare(props: { className?: string }) {
  return (
    <a className={props.className} href="https://nanocode.xyz/cloudflare/" rel="noreferrer" target="_blank">
      <svg aria-hidden="true" height="14" viewBox="0 0 24 24" width="14"><path d="M19.5 20H6a5 5 0 0 1-.9-9.92A7 7 0 0 1 18.4 9.1 5.5 5.5 0 0 1 19.5 20z" fill="#F38020" /></svg>
      on Cloudflare
    </a>
  );
}

function guard(event: MouseEvent<HTMLAnchorElement>) {
  const pointer = (event.nativeEvent as PointerEvent).pointerType;
  const mouse = pointer ? pointer === "mouse" : matchMedia("(pointer: fine)").matches;
  if (!mouse || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return false;
  if (!Wallet.framed() || !Wallet.sandboxed()) return false;
  event.preventDefault();
  return true;
}
