CREATE TYPE payment_status AS ENUM (
    'success',
    'failed'
);

CREATE TABLE payments (
    id int4 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id int4 REFERENCES orders (id),
    status payment_status,
    created_at timestamp(0) without time zone
);