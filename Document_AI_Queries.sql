CREATE OR REPLACE FUNCTION extract_purchase_order(file_name STRING)
RETURNS OBJECT
LANGUAGE SQL
AS
$$
AI_EXTRACT(
  file => TO_FILE('@TLAQUANET_ANALYTICS.PUBLIC.POS_AI', file_name),
  responseFormat => PARSE_JSON(
'{
  "schema":{
    "type":"object",
    "properties":{
      "Line_Items":{
        "description":"Find the line items from the purchase order",
        "type":"object",
        "properties":{
          "description":{"type":"array"},
          "item_number":{"type":"array"},
          "line_total":{"type":"array"},
          "quantity":{"type":"array"},
          "unit_price":{"type":"array"}
        },
        "column_ordering":["item_number","description","quantity","unit_price","line_total"]
      },
      "buyer_company":{"type":"string"},
      "delivery_date":{"type":"string"},
      "order_date":{"type":"string"},
      "po_number":{"type":"string"},
      "subtotal":{"type":"string"},
      "supplier_name":{"type":"string", "description":"Supplier name. Example: ÓOX Technologies"},
      "tax":{"type":"string"},
      "total_amount":{"type":"string"}
    }
  }
}'
  )
)
$$;


SELECT extract_purchase_order('PO1.pdf');

SELECT
    result:response:po_number::STRING AS po_number,
    result:response:buyer_company::STRING AS buyer_company,
    result:response:supplier_name::STRING AS supplier_name,
    result:response:order_date::STRING AS order_date,
    result:response:delivery_date::STRING AS delivery_date,
    result:response:subtotal::STRING AS subtotal,
    result:response:tax::STRING AS tax,
    result:response:total_amount::STRING AS total_amount
FROM (
    SELECT extract_purchase_order_fix('PO1.pdf') AS result
);

SELECT
    result:response:Line_Items:item_number,
    result:response:Line_Items:description,
    result:response:Line_Items:quantity,
    result:response:Line_Items:unit_price,
    result:response:Line_Items:line_total
FROM (
    SELECT extract_purchase_order('PO1.pdf') AS result
);


SELECT
    item.index + 1 AS line_number,
    item.value::STRING AS item_number,
    desc.value::STRING AS description,
    qty.value::STRING AS quantity,
    price.value::STRING AS unit_price,
    total.value::STRING AS line_total
FROM (
    SELECT extract_purchase_order('PO1.pdf') AS result
),
LATERAL FLATTEN(input => result:response:Line_Items:item_number) item,
LATERAL FLATTEN(input => result:response:Line_Items:description) desc,
LATERAL FLATTEN(input => result:response:Line_Items:quantity) qty,
LATERAL FLATTEN(input => result:response:Line_Items:unit_price) price,
LATERAL FLATTEN(input => result:response:Line_Items:line_total) total
WHERE
    item.index = desc.index
    AND item.index = qty.index
    AND item.index = price.index
    AND item.index = total.index;



CREATE OR REPLACE TABLE parsed_purchase_orders AS
SELECT
    relative_path AS file_name,
    result:response:po_number::STRING AS po_number,
    result:response:buyer_company::STRING AS buyer_company,
    result:response:supplier_name::STRING AS supplier_name,
    result:response:order_date::STRING AS order_date,
    result:response:delivery_date::STRING AS delivery_date,
    result:response:subtotal::STRING AS subtotal,
    result:response:tax::STRING AS tax,
    result:response:total_amount::STRING AS total_amount
FROM (
    SELECT
        relative_path,
        extract_purchase_order(relative_path) AS result
    FROM DIRECTORY(@TLAQUANET_ANALYTICS.PUBLIC.POS_AI)
);

CREATE OR REPLACE TABLE purchase_order_line_items AS
SELECT
    r.relative_path AS file_name,
    r.result:response:po_number::STRING AS po_number,
    item.index + 1 AS line_number,
    item.value::STRING AS item_number,
    desc.value::STRING AS description,
    qty.value::STRING AS quantity,
    price.value::STRING AS unit_price,
    total.value::STRING AS line_total
FROM (
    SELECT
        relative_path,
        extract_purchase_order(relative_path) AS result
    FROM DIRECTORY(@TLAQUANET_ANALYTICS.PUBLIC.POS_AI)
) r
,LATERAL FLATTEN(input => r.result:response:Line_Items:item_number) item
,LATERAL FLATTEN(input => r.result:response:Line_Items:description) desc
,LATERAL FLATTEN(input => r.result:response:Line_Items:quantity) qty
,LATERAL FLATTEN(input => r.result:response:Line_Items:unit_price) price
,LATERAL FLATTEN(input => r.result:response:Line_Items:line_total) total
WHERE
    item.index = desc.index
    AND item.index = qty.index
    AND item.index = price.index
    AND item.index = total.index;
-- ---------------------------------------------------------------
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-5-sonnet',
    PROMPT('Is this image {0} a cat or a dog? Answer with only "cat" or "dog".', 
           TO_FILE('@TLAQUANET_ANALYTICS.PUBLIC.TLAQ_IMAGES', 'dog.jpg'))
) as Animal_Type;