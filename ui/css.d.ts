declare module "*.css";
declare module "*.mmd" {
  const svg: string;
  export default svg;
}
declare module "nanocode-ts/nanocode_wasm.wasm" {
  const url: string;
  export default url;
}
