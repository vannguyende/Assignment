-- Count the number of unique Invoices
SELECT COUNT(DISTINCT InvoiceID) AS NumberOfInvoice
FROM online_retail

-- Calculate the average price of each product in the order
SELECT InvoiceID,
       round(avg(Price), 1) as AvgPricePerProduct
FROM online_retail
GROUP BY InvoiceID;