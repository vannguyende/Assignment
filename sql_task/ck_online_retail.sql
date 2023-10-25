CREATE TABLE online_retail
(
    InvoiceID String,
    StockCode String,
    Description String,
    Quantity UInt32,
    InvoiceDate DateTime64(3, 'Asia/Ho_Chi_Minh'),
    Price Float32,
    CustomerID UInt32,
    Country String
)
ENGINE = MergeTree
PRIMARY KEY (InvoiceID, StockCode)
ORDER BY (InvoiceID, StockCode)