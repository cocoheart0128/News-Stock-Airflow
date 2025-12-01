-- DELETE FROM stock_prices WHERE Date BETWEEN  {{ params.start_date }}  AND {{ params.end_date }} ; postgresql version
DELETE FROM stock_prices WHERE Date BETWEEN  :start_date AND :end_date;