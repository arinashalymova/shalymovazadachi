CREATE TABLE IF NOT EXISTS customers (
    customerid SERIAL PRIMARY KEY,
    firstname VARCHAR(100) NOT NULL,
    lastname VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    productid SERIAL PRIMARY KEY,
    productname VARCHAR(255) NOT NULL UNIQUE,
    price DECIMAL(10,2) NOT NULL CHECK (price > 0)
);

CREATE TABLE IF NOT EXISTS orders (
    orderid SERIAL PRIMARY KEY,
    customerid INT NOT NULL REFERENCES customers(customerid),
    orderdate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    totalamount DECIMAL(10,2) NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS orderitems (
    orderitemid SERIAL PRIMARY KEY,
    orderid INT NOT NULL REFERENCES orders(orderid) ON DELETE CASCADE,
    productid INT NOT NULL REFERENCES products(productid),
    quantity INT NOT NULL CHECK (quantity > 0),
    subtotal DECIMAL(10,2) NOT NULL CHECK (subtotal >= 0)
);

INSERT INTO customers (firstname, lastname, email) VALUES
('Иван', 'Петров', 'ivan.petrov@example.com'),
('Елена', 'Козлова', 'elena.kozlova@example.com'),
('Дмитрий', 'Смирнов', 'dmitry.smirnov@example.com')
ON CONFLICT (email) DO NOTHING;

INSERT INTO products (productname, price) VALUES
('Беспроводная мышь', 1299.99),
('USB-хаб Type-C', 899.50),
('Подставка для ноутбука', 2499.00)
ON CONFLICT (productname) DO NOTHING;
