import type { SourceReplacement } from "../fivetran/contracts.ts";
import type { PendingIdentityFact } from "./engine.ts";

export interface IdentityFactReceiver {
  enqueue(facts: PendingIdentityFact[]): Promise<unknown>;
}

/** Publishes graph facts only. Raw source records and conversion models are owned elsewhere. */
export class IdentityOnlyReplacementPublisher {
  private readonly receiver: IdentityFactReceiver;
  constructor(receiver: IdentityFactReceiver) {
    this.receiver = receiver;
  }

  async publishSourceReplacements(replacements: SourceReplacement[]): Promise<void> {
    for (const replacement of replacements) {
      const facts = replacement.source_evidence.identityFacts as PendingIdentityFact[] | undefined;
      if (!Array.isArray(facts)) throw new Error("Identity replacement facts are missing");
      for (const fact of facts) {
        if (!fact || typeof fact.eventId !== "string" || typeof fact.factKey !== "string" || !Number.isSafeInteger(fact.sourceFactVersion)) {
          throw new Error("Identity replacement fact is invalid");
        }
      }
      for (let offset = 0; offset < facts.length; offset += 500) {
        await this.receiver.enqueue(facts.slice(offset, offset + 500));
      }
    }
  }
}
