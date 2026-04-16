# Team Conventions for OAGIS Mapping

This file is loaded at server startup and appended to the LLM's system prompt.
Anything you write here becomes part of the recommendation context for every
query. Edit freely — no code changes needed, just restart the server.

Keep entries short and prescriptive. The LLM follows instructions best when
they're phrased as rules, not prose.

---

## Path notation

- Use forward-slash XPath style with a leading slash: `/ItemMaster/ItemID/ID`
- Never use BOD-prefixed paths (e.g. `/GetItemMaster/DataArea/ItemMaster/...`) —
  we always map to the Noun level, and the BOD wrapper is implied.
- For attributes on elements, use the `@` notation: `/Invoice/InvoiceHeader/TotalAmount/Amount/@currencyID`
- For predicate-filtered paths on generic Property bags, use
  `[Name=...]`: `/ItemMaster/Specification/Property[Name=Weight]/Value`

## Noun selection conventions

- Use **ItemMaster** for part/item *definitions* (design-time, per SKU).
- Use **ItemInstance** for per-unit physical tracking (serial numbers, lot IDs,
  manufacture dates).
- Use **PurchaseOrder** for buy-side transactions; **SalesOrder** for sell-side.
- Use **Shipment** (not Delivery) for outbound logistics events.

## Preferred extension patterns

- When a source attribute has no canonical OAGIS location, prefer the
  `Specification/Property[Name=...]/Value` pattern on ItemMaster or ItemInstance
  over adding custom elements.
- Flag for human review anything that would require a UserArea extension.

## Team terminology mapping

- "TDP attribute" = source attribute from a Technical Data Package
- "Eng attribute" = engineering-originated attribute (CAD, PLM)
- "Ops attribute" = operations/MES-originated attribute
