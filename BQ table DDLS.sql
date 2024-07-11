create table gleaming-kit-427622-m1.retail.orders(
  order_id integer,
  order_date timestamp,
  order_customer_id integer,
  order_status string
)

create table gleaming-kit-427622-m1.retail.order_items(
  order_item_id integer,
  order_item_order_id integer,
  order_item_product_id integer,
  order_item_quantity integer,
  order_item_subtotal decimal,
  order_item_product_price decimal
)

create table gleaming-kit-427622-m1.retail.products(
  product_id integer,
  product_category_id integer,
  product_name string,
  product_description string,
  product_price decimal,
  product_image string
)