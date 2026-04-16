"""Create a synthetic OAGIS mapping spreadsheet for testing."""
import pandas as pd

rows = [
    # ItemMaster-family mappings
    ("PartNumber", "String(40)", "Unique identifier for a manufactured part", "Aerospace TDP", "/ItemMaster/ItemID/ID", "Primary item key"),
    ("PartName", "String(120)", "Human-readable part name", "Aerospace TDP", "/ItemMaster/Name", "Display name"),
    ("PartDescription", "String(500)", "Long-form description of the part", "Aerospace TDP", "/ItemMaster/Description", "Free text"),
    ("PartRevisionCode", "String(10)", "Engineering revision letter/number", "Aerospace TDP", "/ItemMaster/RevisionID", "Ties to CAD rev"),
    ("PartWeight", "Decimal", "Weight of a single part", "Aerospace TDP", "/ItemMaster/Specification/Property[Name=Weight]/Value", "UOM in measurement element"),
    ("PartWeightUOM", "String(10)", "Unit of measure for weight", "Aerospace TDP", "/ItemMaster/Specification/Property[Name=Weight]/Value/@unitCode", "lb, kg, g"),
    ("PartMaterial", "String(60)", "Material composition", "Aerospace TDP", "/ItemMaster/Specification/Property[Name=Material]/Value", "e.g. Titanium"),
    ("SerialNumber", "String(50)", "Unique serial identifier for a physical instance", "Manufacturing MES", "/ItemInstance/SerialNumberID", "Per-instance, not per-item"),
    ("LotNumber", "String(30)", "Batch or lot identifier", "Manufacturing MES", "/ItemInstance/LotID", "Batch tracking"),
    ("ManufactureDate", "Date", "Date of manufacture", "Manufacturing MES", "/ItemInstance/ManufactureDateTime", "ISO 8601"),

    # PurchaseOrder family
    ("PONumber", "String(30)", "Purchase order identifier", "ERP Procurement", "/PurchaseOrder/PurchaseOrderHeader/DocumentID/ID", "Primary PO key"),
    ("POLineNumber", "Int", "Line number on a purchase order", "ERP Procurement", "/PurchaseOrder/PurchaseOrderLine/LineNumber", "Sequential"),
    ("POLineQuantity", "Decimal", "Quantity ordered for a PO line", "ERP Procurement", "/PurchaseOrder/PurchaseOrderLine/Quantity", ""),
    ("POLineUnitPrice", "Decimal", "Unit price on a PO line", "ERP Procurement", "/PurchaseOrder/PurchaseOrderLine/UnitPrice/Amount", "Currency in @currencyID"),
    ("POBuyerCode", "String(20)", "Buyer identifier at our company", "ERP Procurement", "/PurchaseOrder/PurchaseOrderHeader/Buyer/PartyID/ID", ""),
    ("POSupplierCode", "String(20)", "Supplier party code", "ERP Procurement", "/PurchaseOrder/PurchaseOrderHeader/Supplier/PartyID/ID", ""),
    ("POCreatedDate", "Date", "Date the PO was created", "ERP Procurement", "/PurchaseOrder/PurchaseOrderHeader/DocumentDateTime", ""),

    # Invoice family
    ("InvoiceNumber", "String(30)", "Invoice identifier", "AP System", "/Invoice/InvoiceHeader/DocumentID/ID", ""),
    ("InvoiceAmount", "Decimal", "Invoice total amount", "AP System", "/Invoice/InvoiceHeader/TotalAmount/Amount", ""),
    ("InvoiceCurrency", "String(3)", "Currency code for an invoice", "AP System", "/Invoice/InvoiceHeader/TotalAmount/Amount/@currencyID", "ISO 4217"),
    ("InvoiceDate", "Date", "Date of invoice issuance", "AP System", "/Invoice/InvoiceHeader/DocumentDateTime", ""),

    # Shipment
    ("ShipmentID", "String(30)", "Shipment identifier", "Logistics", "/Shipment/ShipmentHeader/DocumentID/ID", ""),
    ("ShipmentTrackingNumber", "String(40)", "Carrier tracking number", "Logistics", "/Shipment/ShipmentHeader/TrackingID", ""),
    ("CarrierCode", "String(20)", "Carrier party code", "Logistics", "/Shipment/ShipmentHeader/Carrier/PartyID/ID", ""),
    ("ShipDate", "Date", "Actual ship date", "Logistics", "/Shipment/ShipmentHeader/ActualShipDateTime", ""),

    # Address block
    ("SupplierStreet", "String(120)", "Supplier street address", "ERP Procurement", "/PurchaseOrder/PurchaseOrderHeader/Supplier/Location/Address/LineOne", ""),
    ("SupplierCity", "String(60)", "Supplier city", "ERP Procurement", "/PurchaseOrder/PurchaseOrderHeader/Supplier/Location/Address/CityName", ""),
    ("SupplierState", "String(40)", "Supplier state or province", "ERP Procurement", "/PurchaseOrder/PurchaseOrderHeader/Supplier/Location/Address/CountrySubDivisionCode", ""),
    ("SupplierPostal", "String(20)", "Supplier postal code", "ERP Procurement", "/PurchaseOrder/PurchaseOrderHeader/Supplier/Location/Address/PostalCode", ""),
    ("SupplierCountry", "String(3)", "Supplier country code", "ERP Procurement", "/PurchaseOrder/PurchaseOrderHeader/Supplier/Location/Address/CountryCode", "ISO 3166-1 alpha-2"),
]

df = pd.DataFrame(
    rows,
    columns=["Attribute Name", "Data Type", "Description", "Source System", "OAGIS Path", "Notes"],
)

out = "/home/user/workspace/oagis-mapper/sample_mappings.xlsx"
with pd.ExcelWriter(out, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="Mappings", index=False)
print("wrote", out, "rows:", len(df))
