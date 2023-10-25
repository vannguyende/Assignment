CREATE TABLE online_retail (
    InvoiceID VARCHAR(10) NOT NULL,
    StockCode VARCHAR(10) NOT NULL,
    Description VARCHAR(50),
    Quantity INTEGER,
    InvoiceDate TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT '1970-01-01 00:00:01',
    Price FLOAT,
    CustomerID INTEGER,
    Country VARCHAR(20),
    PRIMARY KEY (InvoiceID, StockCode)
);