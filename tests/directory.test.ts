import { expect } from "@esm-bundle/chai";
import { init, Directory } from "..";

const decoder = new TextDecoder("utf-8");
const encoder = new TextEncoder();

const initialized = (async () => {
  await init({
    // module: new URL("../dist/wasmer_js_bg.wasm", import.meta.url),
    log: "warn",
  });
})();

describe("In-Memory Directory", function () {
  this.timeout("60s").beforeAll(async () => await initialized);

  it("read empty dir", async () => {
    const dir = new Directory();

    const contents = await dir.readDir("/");

    expect(contents.length).to.equal(0);
  });

  it("can round-trip a file", async () => {
    const dir = new Directory();

    await dir.writeFile("/file.txt", encoder.encode("Hello, World!"));
    const contents = await dir.readFile("/file.txt");

    expect(decoder.decode(contents)).to.equal("Hello, World!");
  });

  it("read dir with file", async () => {
    const dir = new Directory();

    await dir.writeFile("/file.txt", new Uint8Array());
    const contents = await dir.readDir("/");

    expect(contents).to.deep.equal([{ name: "file.txt", type: "file" }]);
  });

  it("create child dir", async () => {
    const dir = new Directory();

    await dir.createDir("/tmp/");

    expect(await dir.readDir("/")).to.deep.equal([
      { name: "tmp", type: "dir" },
    ]);
  });

  it("truncates when overwriting with shorter content (gh196)", async () => {
    const dir = new Directory();

    // Long first version, then overwrite with a shorter one — the #196 repro.
    const longContent = "line1\nline2\nline3\nline4\nline5\nline6\n";
    const shortContent = "new1\nnew2\n";
    await dir.writeFile("/hono.ts", encoder.encode(longContent));
    await dir.writeFile("/hono.ts", encoder.encode(shortContent));

    const contents = decoder.decode(await dir.readFile("/hono.ts"));

    // Must be exactly the short content — no stale tail of the longer version.
    expect(contents).to.equal(shortContent);
  });

  it("can be created with DirectoryInit", async () => {
    const dir = new Directory({
      "/file.txt": "file",
      "/another/nested/file.txt": "another",
    });

    expect(await dir.readTextFile("/file.txt")).to.equal("file");
    expect(await dir.readTextFile("/another/nested/file.txt")).to.equal(
      "another",
    );
  });
});
