import { createHash } from "node:crypto";

const compare = (a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b));
const priority = (type) =>
  ({ email: 1, canonical_email: 2, user_id: 3, anonymous_id: 4 })[type] ?? 5;
const min = (a, b) => (a === null ? b : b === null ? a : a < b ? a : b);
const max = (a, b) => (a === null ? b : b === null ? a : a > b ? a : b);

// Only the current, non-deleted fact contributes connections. No graph runs in SQL.
export class IdentityGraph {
  keys = new Map();
  nodes = [];
  constructor(maxIdentifiers = 30000000) {
    this.maxIdentifiers = maxIdentifiers;
  }
  find(id) {
    let root = id;
    while (this.nodes[root].parent !== root) root = this.nodes[root].parent;
    while (id !== root) {
      const next = this.nodes[id].parent;
      this.nodes[id].parent = root;
      id = next;
    }
    return root;
  }
  addFact(fact) {
    if (fact.factDeleted) return;
    let anchor;
    for (const key of new Set(
      fact.evidenceKeys.filter((key) => key.indexOf(":") > 0),
    )) {
      let id = this.keys.get(key);
      if (id === undefined) {
        if (this.nodes.length >= this.maxIdentifiers)
          throw new Error("Identifier capacity exceeded");
        id = this.nodes.length;
        this.keys.set(key, id);
        const separator = key.indexOf(":");
        this.nodes.push({
          parent: id,
          size: 1,
          key,
          type: key.slice(0, separator),
          value: key.slice(separator + 1),
          first: null,
          last: null,
          hasNull: false,
          source: fact.factKind,
        });
      }
      const node = this.nodes[id];
      node.first = min(node.first, fact.observedAt);
      node.last = max(node.last, fact.observedAt);
      node.hasNull ||= fact.observedAt === null;
      if (compare(fact.factKind, node.source) < 0) node.source = fact.factKind;
      if (anchor === undefined) {
        anchor = id;
        continue;
      }
      let a = this.find(anchor),
        b = this.find(id);
      if (a === b) continue;
      if (this.nodes[a].size < this.nodes[b].size) [a, b] = [b, a];
      this.nodes[b].parent = a;
      this.nodes[a].size += this.nodes[b].size;
    }
  }
  winnerOrder(a, b) {
    const order = priority(a.type) - priority(b.type);
    if (order) return order;
    const ta = a.hasNull ? null : a.first,
      tb = b.hasNull ? null : b.first;
    if (ta !== tb) {
      if (ta === null) return -1;
      if (tb === null) return 1;
      return compare(ta, tb);
    }
    return compare(a.value, b.value);
  }
  resolve() {
    const profiles = new Map();
    for (let id = 0; id < this.nodes.length; id++) {
      const node = this.nodes[id],
        root = this.find(id);
      const prior = profiles.get(root);
      if (!prior) {
        profiles.set(root, {
          winner: node,
          count: 1,
          first: node.first,
          last: node.last,
        });
        continue;
      }
      if (this.winnerOrder(node, prior.winner) < 0) prior.winner = node;
      prior.count++;
      prior.first = min(prior.first, node.first);
      prior.last = max(prior.last, node.last);
    }
    for (const profile of profiles.values())
      profile.id = createHash("md5").update(profile.winner.key).digest("hex");
    return profiles;
  }
}
