CREATE TYPE order_status AS ENUM (
    'created',
    'payment_success',
    'payment_failed'
);

CREATE TABLE orders (
    id int4 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username varchar(30),
    product_name varchar(30),
    quantity int4,
    status order_status,
    created_at timestamp(0) without time zone
);