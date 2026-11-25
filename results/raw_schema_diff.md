| column | 2011-01 | 2012-01 | 2013-01 | 2014-01 | 2015-01 | 2016-01 | 2017-01 | 2018-01 | 2019-01 | 2020-01 | 2021-01 | 2022-01 | 2023-01 | 2024-01 | 2025-01 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| vendorid | int64 (`VendorID`) | int64 (`VendorID`) | int64 (`VendorID`) | int64 (`VendorID`) | int64 (`VendorID`) | int64 (`VendorID`) | int64 (`VendorID`) | int64 (`VendorID`) | int64 (`VendorID`) | int64 (`VendorID`) | int64 (`VendorID`) | int64 (`VendorID`) | int64 (`VendorID`) | int32 (`VendorID`) | int32 (`VendorID`) |
| tpep_pickup_datetime | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] |
| tpep_dropoff_datetime | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] | timestamp[us] |
| passenger_count | int64 | int64 | int64 | int64 | int64 | int64 | int64 | int64 | double | double | double | double | double | int64 | int64 |
| trip_distance | double | double | double | double | double | double | double | double | double | double | double | double | double | double | double |
| ratecodeid | int64 (`RatecodeID`) | int64 (`RatecodeID`) | int64 (`RatecodeID`) | int64 (`RatecodeID`) | int64 (`RatecodeID`) | int64 (`RatecodeID`) | int64 (`RatecodeID`) | int64 (`RatecodeID`) | double (`RatecodeID`) | double (`RatecodeID`) | double (`RatecodeID`) | double (`RatecodeID`) | double (`RatecodeID`) | int64 (`RatecodeID`) | int64 (`RatecodeID`) |
| store_and_fwd_flag | string | string | string | string | string | string | string | string | string | string | string | string | string | large_string | large_string |
| pulocationid | int64 (`PULocationID`) | int64 (`PULocationID`) | int64 (`PULocationID`) | int64 (`PULocationID`) | int64 (`PULocationID`) | int64 (`PULocationID`) | int64 (`PULocationID`) | int64 (`PULocationID`) | int64 (`PULocationID`) | int64 (`PULocationID`) | int64 (`PULocationID`) | int64 (`PULocationID`) | int64 (`PULocationID`) | int32 (`PULocationID`) | int32 (`PULocationID`) |
| dolocationid | int64 (`DOLocationID`) | int64 (`DOLocationID`) | int64 (`DOLocationID`) | int64 (`DOLocationID`) | int64 (`DOLocationID`) | int64 (`DOLocationID`) | int64 (`DOLocationID`) | int64 (`DOLocationID`) | int64 (`DOLocationID`) | int64 (`DOLocationID`) | int64 (`DOLocationID`) | int64 (`DOLocationID`) | int64 (`DOLocationID`) | int32 (`DOLocationID`) | int32 (`DOLocationID`) |
| payment_type | int64 | int64 | int64 | int64 | int64 | int64 | int64 | int64 | int64 | int64 | int64 | int64 | int64 | int64 | int64 |
| fare_amount | double | double | double | double | double | double | double | double | double | double | double | double | double | double | double |
| extra | double | double | double | double | double | double | double | double | double | double | double | double | double | double | double |
| mta_tax | double | double | double | double | double | double | double | double | double | double | double | double | double | double | double |
| tip_amount | double | double | double | double | double | double | double | double | double | double | double | double | double | double | double |
| tolls_amount | double | double | double | double | double | double | double | double | double | double | double | double | double | double | double |
| improvement_surcharge | double | double | double | double | double | double | double | double | double | double | double | double | double | double | double |
| total_amount | double | double | double | double | double | double | double | double | double | double | double | double | double | double | double |
| congestion_surcharge | double | null | null | double | null | null | null | double | double | double | double | double | double | double | double |
| airport_fee | double | null | null | null | null | null | null | double | null | null | double | double | double | double (`Airport_fee`) | double (`Airport_fee`) |
| cbd_congestion_fee | — | — | — | — | — | — | — | — | — | — | — | — | — | — | double |

— = column not in that file. `Name` = different letter case.
