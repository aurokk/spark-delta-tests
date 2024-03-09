# spark-delta-tests

```
------------------------------------------- benchmark: 5 tests ------------------------------------------
Name (time in s)        Min     Max    Mean  StdDev  Median     IQR  Outliers     OPS  Rounds  Iterations
---------------------------------------------------------------------------------------------------------
no stats             5.4070  8.1767  5.7911  0.4327  5.6439  0.4292      15;4  0.1727     100           1
stats                5.4241  9.2070  5.8019  0.5011  5.6280  0.2965     12;13  0.1724     100           1
stats, z-order       6.8795  11.7472 7.3257  0.6281  7.1299  0.3802     10;10  0.1365     100           1
no stats, LC         1.5442  3.2210  1.8575  0.3248  1.7537  0.1757     11;12  0.5384     100           1
stats, LC            1.5514  3.2586  1.8424  0.3294  1.7093  0.2205     11;11  0.5428     100           1
---------------------------------------------------------------------------------------------------------
```
