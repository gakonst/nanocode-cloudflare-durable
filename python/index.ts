export { Python as Pyodide } from "nanocode-ts/cloudflare/python";

export default {
  fetch: () => new Response(null, { status: 404 }),
};
