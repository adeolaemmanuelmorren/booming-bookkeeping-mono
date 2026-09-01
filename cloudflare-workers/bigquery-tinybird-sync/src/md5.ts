const HEX = "0123456789abcdef";

export function md5Hex(value: string): string {
  const bytes = new TextEncoder().encode(value);
  const words = paddedWords(bytes);

  let a = 0x67452301;
  let b = -0x10325477;
  let c = -0x67452302;
  let d = 0x10325476;

  for (let offset = 0; offset < words.length; offset += 16) {
    const initialA = a;
    const initialB = b;
    const initialC = c;
    const initialD = d;

    [a, b, c, d] = runRoundOne(a, b, c, d, words, offset);
    [a, b, c, d] = runRoundTwo(a, b, c, d, words, offset);
    [a, b, c, d] = runRoundThree(a, b, c, d, words, offset);
    [a, b, c, d] = runRoundFour(a, b, c, d, words, offset);

    a = add(a, initialA);
    b = add(b, initialB);
    c = add(c, initialC);
    d = add(d, initialD);
  }

  return [a, b, c, d].map(littleEndianHex).join("");
}

function paddedWords(bytes: Uint8Array): Int32Array {
  const paddedLength = Math.ceil((bytes.length + 9) / 64) * 64;
  const padded = new Uint8Array(paddedLength);
  padded.set(bytes);
  padded[bytes.length] = 0x80;

  const bitLength = BigInt(bytes.length) * 8n;
  for (let index = 0; index < 8; index += 1) {
    padded[padded.length - 8 + index] = Number((bitLength >> BigInt(index * 8)) & 0xffn);
  }

  const words = new Int32Array(padded.length / 4);
  for (let index = 0; index < padded.length; index += 4) {
    words[index / 4] = padded[index]
      | (padded[index + 1] << 8)
      | (padded[index + 2] << 16)
      | (padded[index + 3] << 24);
  }

  return words;
}

function runRoundOne(
  a: number,
  b: number,
  c: number,
  d: number,
  words: Int32Array,
  offset: number,
): [number, number, number, number] {
  const shifts = [7, 12, 17, 22];
  const constants = [
    -680876936, -389564586, 606105819, -1044525330,
    -176418897, 1200080426, -1473231341, -45705983,
    1770035416, -1958414417, -42063, -1990404162,
    1804603682, -40341101, -1502002290, 1236535329,
  ];

  for (let index = 0; index < 16; index += 1) {
    const next = mix((b & c) | (~b & d), a, b, words[offset + index], shifts[index % 4], constants[index]);
    [a, b, c, d] = [d, next, b, c];
  }

  return [a, b, c, d];
}

function runRoundTwo(
  a: number,
  b: number,
  c: number,
  d: number,
  words: Int32Array,
  offset: number,
): [number, number, number, number] {
  const shifts = [5, 9, 14, 20];
  const constants = [
    -165796510, -1069501632, 643717713, -373897302,
    -701558691, 38016083, -660478335, -405537848,
    568446438, -1019803690, -187363961, 1163531501,
    -1444681467, -51403784, 1735328473, -1926607734,
  ];

  for (let index = 0; index < 16; index += 1) {
    const wordIndex = (1 + (5 * index)) % 16;
    const next = mix((b & d) | (c & ~d), a, b, words[offset + wordIndex], shifts[index % 4], constants[index]);
    [a, b, c, d] = [d, next, b, c];
  }

  return [a, b, c, d];
}

function runRoundThree(
  a: number,
  b: number,
  c: number,
  d: number,
  words: Int32Array,
  offset: number,
): [number, number, number, number] {
  const shifts = [4, 11, 16, 23];
  const constants = [
    -378558, -2022574463, 1839030562, -35309556,
    -1530992060, 1272893353, -155497632, -1094730640,
    681279174, -358537222, -722521979, 76029189,
    -640364487, -421815835, 530742520, -995338651,
  ];

  for (let index = 0; index < 16; index += 1) {
    const wordIndex = (5 + (3 * index)) % 16;
    const next = mix(b ^ c ^ d, a, b, words[offset + wordIndex], shifts[index % 4], constants[index]);
    [a, b, c, d] = [d, next, b, c];
  }

  return [a, b, c, d];
}

function runRoundFour(
  a: number,
  b: number,
  c: number,
  d: number,
  words: Int32Array,
  offset: number,
): [number, number, number, number] {
  const shifts = [6, 10, 15, 21];
  const constants = [
    -198630844, 1126891415, -1416354905, -57434055,
    1700485571, -1894986606, -1051523, -2054922799,
    1873313359, -30611744, -1560198380, 1309151649,
    -145523070, -1120210379, 718787259, -343485551,
  ];

  for (let index = 0; index < 16; index += 1) {
    const wordIndex = (7 * index) % 16;
    const next = mix(c ^ (b | ~d), a, b, words[offset + wordIndex], shifts[index % 4], constants[index]);
    [a, b, c, d] = [d, next, b, c];
  }

  return [a, b, c, d];
}

function mix(
  expression: number,
  a: number,
  b: number,
  word: number,
  shift: number,
  constant: number,
): number {
  const sum = add(add(a, expression), add(word, constant));
  return add((sum << shift) | (sum >>> (32 - shift)), b);
}

function add(left: number, right: number): number {
  return (left + right) | 0;
}

function littleEndianHex(value: number): string {
  let result = "";
  for (let index = 0; index < 4; index += 1) {
    const byte = (value >>> (index * 8)) & 0xff;
    result += HEX[(byte >>> 4) & 0x0f] + HEX[byte & 0x0f];
  }
  return result;
}
