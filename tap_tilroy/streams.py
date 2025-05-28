# All stream classes have been removed. Ready to start over.

import typing as t
from singer_sdk import typing as th  # JSON Schema typing helpers
from tap_tilroy.client import TilroyStream
import requests
from datetime import datetime, timedelta

class ShopsStream(TilroyStream):
    """Stream for Tilroy shops."""
    name = "shops"
    path = "/shopapi/production/shops"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = None
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None

    def post_process(self, row: dict, context: t.Optional[dict] = None) -> dict:
        """Post process the record to flatten nested properties.

        Args:
            row: Record to process.
            context: Stream partition or context dictionary.

        Returns:
            Processed record with flattened properties.
        """
        # Flatten type
        if "type" in row:
            type_obj = row["type"]
            row["type_tilroyId"] = type_obj.get("tilroyId")
            row["type_code"] = type_obj.get("code")
            del row["type"]

        # Flatten subType
        if "subType" in row:
            sub_type = row["subType"]
            row["subType_tilroyId"] = sub_type.get("tilroyId")
            row["subType_code"] = sub_type.get("code")
            del row["subType"]

        # Flatten language
        if "language" in row:
            lang = row["language"]
            row["language_tilroyId"] = lang.get("tilroyId")
            row["language_code"] = lang.get("code")
            del row["language"]

        # Flatten country
        if "country" in row:
            country = row["country"]
            row["country_tilroyId"] = country.get("tilroyId")
            row["country_countryCode"] = country.get("countryCode")
            del row["country"]

        return row

    schema = th.PropertiesList(
        th.Property("tilroyId", th.StringType),
        th.Property("sourceId", th.StringType, required=False),
        th.Property("number", th.StringType),
        th.Property("name", th.StringType),
        th.Property("type_tilroyId", th.StringType),
        th.Property("type_code", th.StringType),
        th.Property("subType_tilroyId", th.StringType),
        th.Property("subType_code", th.StringType),
        th.Property("language_tilroyId", th.StringType),
        th.Property("language_code", th.StringType),
        th.Property("latitude", th.StringType, required=False),
        th.Property("longitude", th.StringType, required=False),
        th.Property("postalCode", th.StringType, required=False),
        th.Property("street", th.StringType, required=False),
        th.Property("houseNumber", th.StringType, required=False),
        th.Property("legalEntityId", th.IntegerType),
        th.Property("country_tilroyId", th.StringType),
        th.Property("country_countryCode", th.StringType),
    ).to_dict()

class ProductsStream(TilroyStream):
    """Stream for Tilroy products."""
    name = "products"
    path = "/product-bulk/production/products"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = None
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None
    default_count = 1000  # Override default count to 1000 for products

    def post_process(self, row: dict, context: t.Optional[dict] = None) -> dict:
        """Post process the record to flatten nested properties.

        Args:
            row: Record to process.
            context: Stream partition or context dictionary.

        Returns:
            Processed record with flattened properties.
        """
        # Flatten brand
        if "brand" in row:
            brand = row["brand"]
            row["brand_code"] = brand.get("code")
            # Keep brand descriptions as array since it's a one-to-many relationship
            row["brand_descriptions"] = brand.get("descriptions", [])
            del row["brand"]

        return row

    schema = th.PropertiesList(
        th.Property("tilroyId", th.StringType),
        th.Property("sourceId", th.StringType, required=False),
        th.Property("code", th.StringType),
        th.Property(
            "descriptions",
            th.ArrayType(
                th.ObjectType(
                    th.Property("languageCode", th.StringType),
                    th.Property("standard", th.StringType),
                )
            ),
        ),
        th.Property("brand_code", th.StringType),
        th.Property(
            "brand_descriptions",
            th.ArrayType(
                th.ObjectType(
                    th.Property("languageCode", th.StringType, required=False),
                    th.Property("standard", th.StringType, required=False),
                )
            ),
        ),
        th.Property(
            "colours",
            th.ArrayType(
                th.ObjectType(
                    th.Property("tilroyId", th.StringType),
                    th.Property("sourceId", th.StringType, required=False),
                    th.Property("code", th.StringType),
                    th.Property(
                        "skus",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("tilroyId", th.StringType),
                                th.Property("sourceId", th.StringType, required=False),
                                th.Property("costPrice", th.NumberType),
                                th.Property(
                                    "barcodes",
                                    th.ArrayType(
                                        th.ObjectType(
                                            th.Property("code", th.StringType),
                                            th.Property("quantity", th.IntegerType),
                                            th.Property("isInternal", th.BooleanType),
                                        )
                                    ),
                                ),
                                th.Property(
                                    "size",
                                    th.ObjectType(
                                        th.Property("code", th.StringType),
                                    ),
                                ),
                                th.Property(
                                    "lifeStatus",
                                    th.ObjectType(
                                        th.Property("code", th.StringType),
                                    ),
                                ),
                                th.Property(
                                    "rrp",
                                    th.ArrayType(th.ObjectType()),
                                ),
                            )
                        ),
                    ),
                    th.Property(
                        "pictures",
                        th.ArrayType(th.ObjectType()),
                    ),
                )
            ),
        ),
        th.Property("isUsed", th.BooleanType),
    ).to_dict()

class PurchaseOrdersStream(TilroyStream):
    """Stream for Tilroy purchase orders."""
    name = "purchase_orders"
    path = "/purchaseapi/production/purchaseorders"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = "orderDate"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None
    default_count = 100  # Default count per page

    def post_process(self, row: dict, context: t.Optional[dict] = None) -> dict:
        """Post process the record to flatten nested properties.

        Args:
            row: Record to process.
            context: Stream partition or context dictionary.

        Returns:
            Processed record with flattened properties.
        """
        # Flatten supplier
        if "supplier" in row:
            supplier = row["supplier"]
            row["supplier_tilroyId"] = supplier.get("tilroyId")
            row["supplier_code"] = supplier.get("code")
            row["supplier_name"] = supplier.get("name")
            del row["supplier"]

        # Flatten warehouse
        if "warehouse" in row:
            warehouse = row["warehouse"]
            row["warehouse_number"] = warehouse.get("number")
            row["warehouse_name"] = warehouse.get("name")
            del row["warehouse"]

        # Flatten currency
        if "currency" in row:
            currency = row["currency"]
            row["currency_code"] = currency.get("code")
            del row["currency"]

        # Flatten prices
        if "prices" in row:
            prices = row["prices"]
            if "tenantCurrency" in prices:
                tenant = prices["tenantCurrency"]
                row["prices_tenantCurrency_standardVatExc"] = tenant.get("standardVatExc")
                row["prices_tenantCurrency_standardVatInc"] = tenant.get("standardVatInc")
                row["prices_tenantCurrency_vatExc"] = tenant.get("vatExc")
                row["prices_tenantCurrency_vatInc"] = tenant.get("vatInc")
            if "supplierCurrency" in prices:
                supplier = prices["supplierCurrency"]
                row["prices_supplierCurrency_standardVatExc"] = supplier.get("standardVatExc")
                row["prices_supplierCurrency_standardVatInc"] = supplier.get("standardVatInc")
                row["prices_supplierCurrency_vatExc"] = supplier.get("vatExc")
                row["prices_supplierCurrency_vatInc"] = supplier.get("vatInc")
            del row["prices"]

        # Flatten created user
        if "created" in row and "user" in row["created"]:
            created_user = row["created"]["user"]
            row["created_user_login"] = created_user.get("login")
            row["created_user_sourceId"] = created_user.get("sourceId")
            row["created_timestamp"] = row["created"].get("timestamp")
            del row["created"]

        # Flatten modified user
        if "modified" in row and "user" in row["modified"]:
            modified_user = row["modified"]["user"]
            row["modified_user_login"] = modified_user.get("login")
            row["modified_user_sourceId"] = modified_user.get("sourceId")
            row["modified_timestamp"] = row["modified"].get("timestamp")
            del row["modified"]

        return row

    schema = th.PropertiesList(
        th.Property("tilroyId", th.StringType),
        th.Property("number", th.StringType),
        th.Property("orderDate", th.StringType),
        th.Property("supplier_tilroyId", th.IntegerType),
        th.Property("supplier_code", th.StringType),
        th.Property("supplier_name", th.StringType),
        th.Property("supplierReference", th.StringType, required=False),
        th.Property("requestedDeliveryDate", th.StringType),
        th.Property("warehouse_number", th.IntegerType),
        th.Property("warehouse_name", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("prices_tenantCurrency_standardVatExc", th.CustomType({"type": ["number", "string"]})),
        th.Property("prices_tenantCurrency_standardVatInc", th.CustomType({"type": ["number", "string"]})),
        th.Property("prices_tenantCurrency_vatExc", th.CustomType({"type": ["number", "string"]})),
        th.Property("prices_tenantCurrency_vatInc", th.CustomType({"type": ["number", "string"]})),
        th.Property("prices_supplierCurrency_standardVatExc", th.CustomType({"type": ["number", "string"]})),
        th.Property("prices_supplierCurrency_standardVatInc", th.CustomType({"type": ["number", "string"]})),
        th.Property("prices_supplierCurrency_vatExc", th.CustomType({"type": ["number", "string"]})),
        th.Property("prices_supplierCurrency_vatInc", th.CustomType({"type": ["number", "string"]})),
        th.Property("status", th.StringType),
        th.Property("created_user_login", th.StringType),
        th.Property("created_user_sourceId", th.StringType, required=False),
        th.Property("created_timestamp", th.StringType),
        th.Property("modified_user_login", th.StringType),
        th.Property("modified_user_sourceId", th.StringType, required=False),
        th.Property("modified_timestamp", th.StringType),
        th.Property("lines", th.ArrayType(th.ObjectType(
            th.Property("sku_tilroyId", th.StringType),
            th.Property("sku_sourceId", th.StringType),
            th.Property("warehouse_number", th.IntegerType),
            th.Property("warehouse_name", th.StringType),
            th.Property("created_user_login", th.StringType),
            th.Property("created_user_sourceId", th.StringType, required=False),
            th.Property("modified_user_login", th.StringType),
            th.Property("modified_user_sourceId", th.StringType, required=False),
            th.Property("status", th.StringType),
            th.Property("requestedDeliveryDate", th.StringType),
            th.Property("qty_ordered", th.IntegerType),
            th.Property("qty_delivered", th.IntegerType),
            th.Property("qty_backOrder", th.IntegerType),
            th.Property("qty_cancelled", th.IntegerType),
            th.Property("prices_tenantCurrency_vatExc", th.CustomType({"type": ["number", "string"]})),
            th.Property("prices_tenantCurrency_vatInc", th.CustomType({"type": ["number", "string"]})),
            th.Property("prices_tenantCurrency_unitVatExc", th.CustomType({"type": ["number", "string"]})),
            th.Property("prices_tenantCurrency_unitVatInc", th.CustomType({"type": ["number", "string"]})),
            th.Property("prices_tenantCurrency_standardUnitVatExc", th.CustomType({"type": ["number", "string"]})),
            th.Property("prices_tenantCurrency_standardVatExc", th.CustomType({"type": ["number", "string"]})),
            th.Property("prices_tenantCurrency_standardVatInc", th.CustomType({"type": ["number", "string"]})),
            th.Property("prices_tenantCurrency_standardUnitVatInc", th.CustomType({"type": ["number", "string"]})),
            th.Property("prices_supplierCurrency_vatExc", th.CustomType({"type": ["number", "string"]})),
            th.Property("prices_supplierCurrency_vatInc", th.CustomType({"type": ["number", "string"]})),
            th.Property("prices_supplierCurrency_unitVatExc", th.CustomType({"type": ["number", "string"]})),
            th.Property("prices_supplierCurrency_unitVatInc", th.CustomType({"type": ["number", "string"]})),
            th.Property("prices_supplierCurrency_standardUnitVatExc", th.CustomType({"type": ["number", "string"]})),
            th.Property("prices_supplierCurrency_standardVatExc", th.CustomType({"type": ["number", "string"]})),
            th.Property("prices_supplierCurrency_standardVatInc", th.CustomType({"type": ["number", "string"]})),
            th.Property("prices_supplierCurrency_standardUnitVatInc", th.CustomType({"type": ["number", "string"]})),
            th.Property("discount_amount", th.CustomType({"type": ["number", "string"]})),
            th.Property("discount_percentage", th.CustomType({"type": ["number", "string"]})),
            th.Property("discount_total", th.CustomType({"type": ["number", "string"]})),
            th.Property("discount_newStandardPrice", th.CustomType({"type": ["number", "string"]})),
            th.Property("id", th.StringType),
        ))),
    ).to_dict()

class StockChangesStream(TilroyStream):
    """Stream for Tilroy stock changes."""
    name = "stock_changes"
    path = "/stockapi/production/export/stockdeltas"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = "timestamp"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None

    def post_process(self, row: dict, context: t.Optional[dict] = None) -> dict:
        """Post process the record to flatten nested properties.

        Args:
            row: Record to process.
            context: Stream partition or context dictionary.

        Returns:
            Processed record with flattened properties.
        """
        # Flatten shop
        if "shop" in row:
            shop = row["shop"]
            row["shop_number"] = shop.get("number")
            row["shop_sourceId"] = shop.get("sourceId")
            del row["shop"]

        # Flatten product
        if "product" in row:
            product = row["product"]
            row["product_code"] = product.get("code")
            row["product_sourceId"] = product.get("sourceId")
            del row["product"]

        # Flatten colour
        if "colour" in row:
            colour = row["colour"]
            row["colour_code"] = colour.get("code")
            row["colour_sourceId"] = colour.get("sourceId")
            del row["colour"]

        # Flatten size
        if "size" in row:
            size = row["size"]
            row["size_code"] = size.get("code")
            del row["size"]

        # Flatten sku
        if "sku" in row:
            sku = row["sku"]
            row["sku_barcode"] = sku.get("barcode")
            row["sku_sourceId"] = sku.get("sourceId")
            del row["sku"]

        return row

    schema = th.PropertiesList(
        th.Property("tilroyId", th.StringType),
        th.Property("timestamp", th.StringType),
        th.Property("sourceId", th.StringType),
        th.Property("modificationType", th.StringType),
        th.Property("reason", th.StringType),
        th.Property("shop_number", th.IntegerType),
        th.Property("shop_sourceId", th.StringType, required=False),
        th.Property("product_code", th.StringType),
        th.Property("product_sourceId", th.StringType),
        th.Property("colour_code", th.StringType),
        th.Property("colour_sourceId", th.StringType),
        th.Property("size_code", th.StringType),
        th.Property("sku_barcode", th.StringType),
        th.Property("sku_sourceId", th.StringType),
        th.Property("qtyDelta", th.IntegerType),
        th.Property("qtyTransferredDelta", th.IntegerType),
        th.Property("qtyReservedDelta", th.IntegerType),
        th.Property("qtyRequestedDelta", th.IntegerType),
        th.Property("cause", th.StringType, required=False),
    ).to_dict()

class SalesStream(TilroyStream):
    """Stream for Tilroy sales."""
    name = "sales"
    path = "/saleapi/production/export/sales"
    primary_keys: t.ClassVar[list[str]] = ["idTilroySale"]
    replication_key = "saleDate"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None

    def get_url_params(
        self,
        context: t.Optional[dict],
        next_page_token: t.Optional[t.Any] = None,
    ) -> dict[str, t.Any]:
        """Get URL query parameters.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token for the next page of results.

        Returns:
            Dictionary of URL query parameters.
        """
        params = super().get_url_params(context, next_page_token)
        
        # Get the start date from the bookmark or use config
        start_date = self.get_starting_time(context)
        if not start_date:
            # Get start date from config
            config_start_date = self.config["start_date"]
            start_date = datetime.strptime(config_start_date, "%Y-%m-%d")
        else:
            # If we have a bookmark, go back 1 day to ensure we don't miss any records
            start_date = start_date - timedelta(days=1)
        
        # Format the date as YYYY-MM-DD and ensure it's a string
        params["dateFrom"] = start_date.strftime("%Y-%m-%d")
        
        # Add dateTo parameter to ensure we get all records up to now
        params["dateTo"] = datetime.now().strftime("%Y-%m-%d")
        
        return params

    def post_process(self, row: dict, context: t.Optional[dict] = None) -> dict:
        """Post process the record to flatten nested properties.

        Args:
            row: Record to process.
            context: Stream partition or context dictionary.

        Returns:
            Processed record with flattened properties.
        """
        # Flatten customer
        if "customer" in row:
            row["customer_idTilroy"] = row["customer"].get("idTilroy")
            row["customer_idSource"] = row["customer"].get("idSource")
            del row["customer"]

        # Flatten vatTypeCalculation
        if "vatTypeCalculation" in row:
            vat_calc = row["vatTypeCalculation"]
            row["vatTypeCalculation_UseCalculation"] = vat_calc.get("UseCalculation")
            row["vatTypeCalculation_IdVatType"] = vat_calc.get("IdVatType")
            row["vatTypeCalculation_VatTypeCode"] = vat_calc.get("VatTypeCode")
            row["vatTypeCalculation_VatExempt"] = vat_calc.get("VatExempt")
            row["vatTypeCalculation_IsVatIncl"] = vat_calc.get("IsVatIncl")
            row["vatTypeCalculation_IsIntraComm"] = vat_calc.get("IsIntraComm")
            row["vatTypeCalculation_IsExport"] = vat_calc.get("IsExport")
            row["vatTypeCalculation_IsCustom"] = vat_calc.get("IsCustom")
            row["vatTypeCalculation_IdCountryFrom"] = vat_calc.get("IdCountryFrom")
            row["vatTypeCalculation_CountryFromIsIntrastat"] = vat_calc.get("CountryFromIsIntrastat")
            row["vatTypeCalculation_IdCountryTo"] = vat_calc.get("IdCountryTo")
            row["vatTypeCalculation_CountryToIsIntrastat"] = vat_calc.get("CountryToIsIntrastat")
            row["vatTypeCalculation_Invoice"] = vat_calc.get("Invoice")
            row["vatTypeCalculation_VatNumber"] = vat_calc.get("VatNumber")
            row["vatTypeCalculation_IdCustomer"] = vat_calc.get("IdCustomer")
            del row["vatTypeCalculation"]

        # Flatten shop
        if "shop" in row:
            shop = row["shop"]
            row["shop_idTilroy"] = shop.get("idTilroy")
            row["shop_idSource"] = shop.get("idSource")
            row["shop_number"] = shop.get("number")
            row["shop_name"] = shop.get("name")
            row["shop_country"] = shop.get("country")
            del row["shop"]

        # Flatten till
        if "till" in row:
            till = row["till"]
            row["till_idTilroy"] = till.get("idTilroy")
            row["till_number"] = till.get("number")
            row["till_idSource"] = till.get("idSource")
            del row["till"]

        # Flatten legalEntity
        if "legalEntity" in row:
            legal = row["legalEntity"]
            row["legalEntity_idTilroy"] = legal.get("idTilroy")
            row["legalEntity_code"] = legal.get("code")
            row["legalEntity_name"] = legal.get("name")
            row["legalEntity_vatNr"] = legal.get("vatNr")
            del row["legalEntity"]

        return row

    schema = th.PropertiesList(
        th.Property("idTilroySale", th.StringType),
        th.Property("idTenant", th.StringType),
        th.Property("idSession", th.StringType),
        # Flattened customer fields
        th.Property("customer_idTilroy", th.StringType, required=False),
        th.Property("customer_idSource", th.StringType, required=False),
        th.Property("idSourceCustomer", th.StringType, required=False),
        # Flattened vatTypeCalculation fields
        th.Property("vatTypeCalculation_UseCalculation", th.BooleanType),
        th.Property("vatTypeCalculation_IdVatType", th.StringType),
        th.Property("vatTypeCalculation_VatTypeCode", th.StringType),
        th.Property("vatTypeCalculation_VatExempt", th.BooleanType),
        th.Property("vatTypeCalculation_IsVatIncl", th.BooleanType),
        th.Property("vatTypeCalculation_IsIntraComm", th.BooleanType),
        th.Property("vatTypeCalculation_IsExport", th.BooleanType),
        th.Property("vatTypeCalculation_IsCustom", th.BooleanType),
        th.Property("vatTypeCalculation_IdCountryFrom", th.IntegerType),
        th.Property("vatTypeCalculation_CountryFromIsIntrastat", th.BooleanType),
        th.Property("vatTypeCalculation_IdCountryTo", th.IntegerType),
        th.Property("vatTypeCalculation_CountryToIsIntrastat", th.BooleanType),
        th.Property("vatTypeCalculation_Invoice", th.BooleanType),
        th.Property("vatTypeCalculation_VatNumber", th.StringType),
        th.Property("vatTypeCalculation_IdCustomer", th.StringType),
        # Flattened shop fields
        th.Property("shop_idTilroy", th.StringType),
        th.Property("shop_idSource", th.StringType, required=False),
        th.Property("shop_number", th.IntegerType),
        th.Property("shop_name", th.StringType),
        th.Property("shop_country", th.StringType),
        # Flattened till fields
        th.Property("till_idTilroy", th.StringType),
        th.Property("till_number", th.IntegerType),
        th.Property("till_idSource", th.StringType, required=False),
        # Main sale fields
        th.Property("saleDate", th.StringType),
        th.Property("eTicket", th.BooleanType),
        th.Property("orderDate", th.StringType, required=False),
        th.Property("totalAmountStandard", th.NumberType),
        th.Property("totalAmountSell", th.NumberType),
        th.Property("totalAmountDiscount", th.NumberType),
        th.Property("totalAmountSellRounded", th.NumberType),
        th.Property("totalAmountSellRoundedPart", th.NumberType),
        th.Property("totalAmountSellNotRoundedPart", th.NumberType),
        th.Property("totalAmountOutstanding", th.NumberType),
        # Keep arrays as they are
        th.Property(
            "lines",
            th.ArrayType(
                th.ObjectType(
                    th.Property("idTilroySaleLine", th.StringType),
                    th.Property("type", th.StringType),
                    th.Property(
                        "sku",
                        th.ObjectType(
                            th.Property("idTilroy", th.StringType),
                            th.Property("idSource", th.StringType),
                        ),
                    ),
                    th.Property("description", th.StringType),
                    th.Property("quantity", th.IntegerType),
                    th.Property("quantityReturned", th.IntegerType),
                    th.Property("quantityNet", th.IntegerType),
                    th.Property("costPrice", th.NumberType),
                    th.Property("sellPrice", th.NumberType),
                    th.Property("standardPrice", th.NumberType),
                    th.Property("promoPrice", th.NumberType),
                    th.Property("rrp", th.NumberType),
                    th.Property("retailPrice", th.NumberType),
                    th.Property("discount", th.NumberType),
                    th.Property("discountType", th.IntegerType),
                    th.Property("lineTotalCost", th.NumberType),
                    th.Property("lineTotalStandard", th.NumberType),
                    th.Property("lineTotalSell", th.NumberType),
                    th.Property("lineTotalDiscount", th.NumberType),
                    th.Property("lineTotalVatExcl", th.NumberType),
                    th.Property("lineTotalVat", th.NumberType),
                    th.Property("vatPercentage", th.NumberType),
                    th.Property("code", th.StringType),
                    th.Property("comments", th.StringType, required=False),
                    th.Property("serialNumberSale", th.StringType, required=False),
                    th.Property("webDescription", th.StringType),
                    th.Property("colour", th.StringType),
                    th.Property("size", th.StringType),
                    th.Property("ean", th.StringType),
                    th.Property("timestamp", th.StringType),
                ),
            ),
        ),
        th.Property("totalAmountPaid", th.NumberType),
        th.Property(
            "payments",
            th.ArrayType(
                th.ObjectType(
                    th.Property("idTilroySalePayment", th.StringType),
                    th.Property(
                        "paymentType",
                        th.ObjectType(
                            th.Property("idTilroy", th.StringType),
                            th.Property("code", th.StringType),
                            th.Property("idSource", th.StringType, required=False),
                            th.Property(
                                "descriptions",
                                th.ArrayType(
                                    th.ObjectType(
                                        th.Property("description", th.StringType),
                                        th.Property("languageCode", th.StringType),
                                    ),
                                ),
                            ),
                            th.Property("reporting", th.BooleanType),
                        ),
                    ),
                    th.Property("amount", th.NumberType),
                    th.Property("paymentReference", th.StringType),
                    th.Property("timestamp", th.StringType),
                    th.Property("isPaid", th.BooleanType),
                ),
            ),
        ),
        th.Property(
            "vat",
            th.ArrayType(
                th.ObjectType(
                    th.Property("idTilroy", th.StringType),
                    th.Property("vatPercentage", th.NumberType),
                    th.Property("vatKind", th.StringType),
                    th.Property("amountNet", th.NumberType),
                    th.Property("amountLines", th.NumberType),
                    th.Property("amountTaxable", th.NumberType),
                    th.Property("amountVat", th.NumberType),
                    th.Property("vatAmount", th.NumberType),
                    th.Property("totalAmount", th.NumberType),
                    th.Property("timestamp", th.StringType),
                ),
            ),
        ),
        # Flattened legalEntity fields
        th.Property("legalEntity_idTilroy", th.StringType),
        th.Property("legalEntity_code", th.StringType),
        th.Property("legalEntity_name", th.StringType),
        th.Property("legalEntity_vatNr", th.StringType),
    ).to_dict()
