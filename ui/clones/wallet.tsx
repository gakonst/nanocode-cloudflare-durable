import { Wallet } from "nanocode-ts/machines";
import { login, logout, useWallet } from "./hand.ts";

export function WalletButton(props: { className?: string }) {
  const wallet = useWallet();
  if (wallet.connected) return <button className={props.className} onClick={() => void logout()} title="Log out and revoke the key" type="button">{Wallet.short(wallet.connected.address)}</button>;
  return (
    <span className="nc-wallet">
      <button className={props.className} disabled={wallet.busy} onClick={() => void login()} type="button">{wallet.busy ? "Connecting…" : "Connect Tempo Wallet"}</button>
      {wallet.device && <a className="nc-wallet-code" href={wallet.device.verificationUriFull ?? wallet.device.verificationUri} rel="noreferrer" target="_blank">Approve code {wallet.device.userCode}</a>}
      {wallet.error && <span className="nc-wallet-error">{wallet.error}</span>}
    </span>
  );
}

export function HandBadge(props: { className?: string }) {
  const { hand } = useWallet();
  const label = hand.state === "attached" ? "Code runs in this tab" : hand.state === "connecting" ? "Attaching this tab…" : "Code runs on Cloudflare";
  return <span className={props.className} data-state={hand.state} title="The agent is durable in a Durable Object; its Code Mode cells run in this browser tab with your wallet.">{label}</span>;
}

export function Cloudflare(props: { className?: string }) {
  return (
    <a className={props.className} href="https://nanocode.xyz/cloudflare/" rel="noreferrer" target="_blank">
      <svg aria-hidden="true" height="14" viewBox="0 0 24 24" width="14"><path d="M19.5 20H6a5 5 0 0 1-.9-9.92A7 7 0 0 1 18.4 9.1 5.5 5.5 0 0 1 19.5 20z" fill="#F38020" /></svg>
      on Cloudflare
    </a>
  );
}

