# Team Conventions for OAGIS Mapping

This file is loaded at server startup and appended to the LLM's system prompt.
Anything you write here becomes part of the recommendation context for every
query. Edit freely — no code changes needed, just restart the server.

Keep entries short and prescriptive. The LLM follows instructions best when
they're phrased as rules, not prose.

---

## Path notation

- Use dot-notation XPath style: `ItemMaster.ItemID.ID`
- For attributes on elements, use the `[typeCode=""]` notation: `Invoice.InvoiceHeader.TotalAmount.Amount[typeCode="CurrencyId"]`

## Noun selection conventions

- Use **ItemMaster** for part/item *definitions* (design-time, per SKU).
- Use **ItemInstance** for per-unit physical tracking (serial numbers, lot IDs,
  manufacture dates).
- Use **PurchaseOrder** for buy-side transactions; **SalesOrder** for sell-side.
- Use **Shipment** (not Delivery) for outbound logistics events.

## Preferred extension patterns

- When a source attribute has no canonical OAGIS location, prefer the
  `Extension` pattern over adding custom elements.
- Flag for human review anything that would require a UserArea extension.
- For any true/false flag use `Classification.Indicators.Indicator`

## Team terminology mapping

- "TDP attribute" = source attribute from a Technical Data Package
- "Eng attribute" = engineering-originated attribute (CAD, PLM)
- "Ops attribute" = operations/MES-originated attribute
