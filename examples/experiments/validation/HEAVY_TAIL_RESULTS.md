# Heavy-tail comparison: frozen results

Read [the protocol and method definitions](HEAVY_TAILS.md).
Brackets are pointwise exact 95% Monte Carlo intervals; rates are percentages.
Availability and joint success are separate from coverage among available intervals.

## heavy-tail-primary.json.gz

R=2000; B=1999; seed=214202610; gamma=0.7.

| Scenario | Method | Available | Absolute-effect coverage [MC95] | Reject zero [MC95] | Width median / p99 |
|---|---|---:|---|---|---:|
| binary_null | percentile | 2000/2000 | 94.80 [93.73, 95.73] | 5.20 [4.27, 6.27] | 0.104 / 0.12 |
| binary_null | basic | 2000/2000 | 94.50 [93.41, 95.46] | 5.50 [4.54, 6.59] | 0.104 / 0.12 |
| binary_null | bca | 2000/2000 | 94.25 [93.14, 95.23] | 5.75 [4.77, 6.86] | 0.104 / 0.12 |
| binary_null | bootstrap_t | 2000/2000 | 93.80 [92.65, 94.82] | 6.20 [5.18, 7.35] | 0.105 / 0.12 |
| binary_null | bootstrap_t_symmetric | 2000/2000 | 94.20 [93.08, 95.18] | 5.80 [4.82, 6.92] | 0.105 / 0.12 |
| binary_null | m_out_of_n_basic | 2000/2000 | 93.80 [92.65, 94.82] | 6.20 [5.18, 7.35] | 0.101 / 0.12 |
| binary_null | welch | 2000/2000 | 94.20 [93.08, 95.18] | 5.80 [4.82, 6.92] | 0.106 / 0.119 |
| binary_null | wilson_newcombe | 2000/2000 | 94.25 [93.14, 95.23] | 5.75 [4.77, 6.86] | 0.107 / 0.12 |
| binary_effect | percentile | 2000/2000 | 95.20 [94.17, 96.09] | 18.65 [16.96, 20.43] | 0.112 / 0.124 |
| binary_effect | basic | 2000/2000 | 95.25 [94.22, 96.14] | 19.25 [17.54, 21.05] | 0.112 / 0.124 |
| binary_effect | bca | 2000/2000 | 95.00 [93.95, 95.91] | 19.20 [17.49, 21.00] | 0.112 / 0.126 |
| binary_effect | bootstrap_t | 2000/2000 | 94.95 [93.90, 95.87] | 20.90 [19.14, 22.75] | 0.111 / 0.126 |
| binary_effect | bootstrap_t_symmetric | 2000/2000 | 95.05 [94.01, 95.96] | 20.25 [18.51, 22.08] | 0.112 / 0.126 |
| binary_effect | m_out_of_n_basic | 2000/2000 | 95.65 [94.66, 96.50] | 20.60 [18.85, 22.44] | 0.111 / 0.129 |
| binary_effect | welch | 2000/2000 | 95.25 [94.22, 96.14] | 19.85 [18.12, 21.67] | 0.112 / 0.125 |
| binary_effect | wilson_newcombe | 2000/2000 | 95.35 [94.33, 96.23] | 19.80 [18.07, 21.62] | 0.113 / 0.125 |
| binary_unequal_null | percentile | 2000/2000 | 95.45 [94.44, 96.32] | 4.55 [3.68, 5.56] | 0.128 / 0.158 |
| binary_unequal_null | basic | 2000/2000 | 94.05 [92.92, 95.05] | 5.95 [4.95, 7.08] | 0.128 / 0.158 |
| binary_unequal_null | bca | 2000/2000 | 96.05 [95.10, 96.86] | 3.95 [3.14, 4.90] | 0.13 / 0.16 |
| binary_unequal_null | bootstrap_t | 2000/2000 | 96.25 [95.32, 97.04] | 3.75 [2.96, 4.68] | 0.135 / 0.165 |
| binary_unequal_null | bootstrap_t_symmetric | 2000/2000 | 95.95 [94.99, 96.77] | 4.05 [3.23, 5.01] | 0.136 / 0.167 |
| binary_unequal_null | m_out_of_n_basic | 2000/2000 | 92.45 [91.20, 93.57] | 7.55 [6.43, 8.80] | 0.128 / 0.16 |
| binary_unequal_null | welch | 2000/2000 | 94.95 [93.90, 95.87] | 5.05 [4.13, 6.10] | 0.13 / 0.16 |
| binary_unequal_null | wilson_newcombe | 2000/2000 | 96.10 [95.16, 96.91] | 3.90 [3.09, 4.84] | 0.131 / 0.158 |
| rare_null | percentile | 96/2000 | 100.00 [96.23, 100.00] | 0.00 [0.00, 3.77] | 0.024 / 0.0322 |
| rare_null | basic | 96/2000 | 100.00 [96.23, 100.00] | 0.00 [0.00, 3.77] | 0.024 / 0.0322 |
| rare_null | bca | 96/2000 | 100.00 [96.23, 100.00] | 0.00 [0.00, 3.77] | 0.024 / 0.0324 |
| rare_null | bootstrap_t | 0/2000 | unavailable | unavailable | — |
| rare_null | bootstrap_t_symmetric | 0/2000 | unavailable | unavailable | — |
| rare_null | m_out_of_n_basic | 96/2000 | 100.00 [96.23, 100.00] | 0.00 [0.00, 3.77] | 0.0185 / 0.0281 |
| rare_null | welch | 781/2000 | 100.00 [99.53, 100.00] | 0.00 [0.00, 0.47] | 0.0158 / 0.0272 |
| rare_null | wilson_newcombe | 2000/2000 | 100.00 [99.82, 100.00] | 0.00 [0.00, 0.18] | 0.0303 / 0.0398 |
| rare_effect | percentile | 264/2000 | 100.00 [98.61, 100.00] | 0.00 [0.00, 1.39] | 0.024 / 0.036 |
| rare_effect | basic | 264/2000 | 100.00 [98.61, 100.00] | 0.00 [0.00, 1.39] | 0.024 / 0.036 |
| rare_effect | bca | 264/2000 | 100.00 [98.61, 100.00] | 0.00 [0.00, 1.39] | 0.024 / 0.036 |
| rare_effect | bootstrap_t | 0/2000 | unavailable | unavailable | — |
| rare_effect | bootstrap_t_symmetric | 0/2000 | unavailable | unavailable | — |
| rare_effect | m_out_of_n_basic | 264/2000 | 100.00 [98.61, 100.00] | 0.00 [0.00, 1.39] | 0.0185 / 0.0369 |
| rare_effect | welch | 1376/2000 | 100.00 [99.73, 100.00] | 1.67 [1.06, 2.50] | 0.0222 / 0.0314 |
| rare_effect | wilson_newcombe | 2000/2000 | 100.00 [99.82, 100.00] | 0.05 [0.00, 0.28] | 0.0338 / 0.043 |
| zero_boundary | percentile | 0/2000 | unavailable | unavailable | — |
| zero_boundary | basic | 0/2000 | unavailable | unavailable | — |
| zero_boundary | bca | 0/2000 | unavailable | unavailable | — |
| zero_boundary | bootstrap_t | 0/2000 | unavailable | unavailable | — |
| zero_boundary | bootstrap_t_symmetric | 0/2000 | unavailable | unavailable | — |
| zero_boundary | m_out_of_n_basic | 0/2000 | unavailable | unavailable | — |
| zero_boundary | welch | 0/2000 | unavailable | unavailable | — |
| zero_boundary | wilson_newcombe | 2000/2000 | 100.00 [99.82, 100.00] | 0.00 [0.00, 0.18] | 0.0303 / 0.0303 |
| one_boundary | percentile | 0/2000 | unavailable | unavailable | — |
| one_boundary | basic | 0/2000 | unavailable | unavailable | — |
| one_boundary | bca | 0/2000 | unavailable | unavailable | — |
| one_boundary | bootstrap_t | 0/2000 | unavailable | unavailable | — |
| one_boundary | bootstrap_t_symmetric | 0/2000 | unavailable | unavailable | — |
| one_boundary | m_out_of_n_basic | 0/2000 | unavailable | unavailable | — |
| one_boundary | welch | 0/2000 | unavailable | unavailable | — |
| one_boundary | wilson_newcombe | 2000/2000 | 100.00 [99.82, 100.00] | 0.00 [0.00, 0.18] | 0.0303 / 0.0303 |
| money_null | percentile | 2000/2000 | 91.10 [89.77, 92.31] | 8.90 [7.69, 10.23] | 40.6 / 354 |
| money_null | basic | 2000/2000 | 98.55 [97.92, 99.03] | 1.45 [0.97, 2.08] | 40.6 / 354 |
| money_null | bca | 2000/2000 | 79.85 [78.02, 81.59] | 20.15 [18.41, 21.98] | 45.7 / 579 |
| money_null | bootstrap_t | 2000/2000 | 86.95 [85.39, 88.40] | 13.05 [11.60, 14.61] | 48.2 / 2.39e+03 |
| money_null | bootstrap_t_symmetric | 2000/2000 | 99.50 [99.08, 99.76] | 0.50 [0.24, 0.92] | 54.3 / 4.06e+03 |
| money_null | m_out_of_n_basic | 2000/2000 | 99.25 [98.77, 99.58] | 0.75 [0.42, 1.23] | 40.2 / 296 |
| money_null | welch | 2000/2000 | 97.60 [96.83, 98.23] | 2.40 [1.77, 3.17] | 41.3 / 413 |
| money_effect | percentile | 2000/2000 | 87.05 [85.50, 88.49] | 13.60 [12.13, 15.18] | 48 / 393 |
| money_effect | basic | 2000/2000 | 94.80 [93.73, 95.73] | 2.85 [2.17, 3.68] | 48 / 393 |
| money_effect | bca | 2000/2000 | 77.90 [76.02, 79.70] | 25.00 [23.12, 26.96] | 54.5 / 644 |
| money_effect | bootstrap_t | 2000/2000 | 82.55 [80.81, 84.19] | 18.10 [16.44, 19.86] | 58.1 / 2.78e+03 |
| money_effect | bootstrap_t_symmetric | 2000/2000 | 96.60 [95.71, 97.35] | 1.30 [0.85, 1.90] | 66.4 / 4.52e+03 |
| money_effect | m_out_of_n_basic | 2000/2000 | 96.05 [95.10, 96.86] | 1.50 [1.01, 2.13] | 47.5 / 330 |
| money_effect | welch | 2000/2000 | 92.40 [91.15, 93.52] | 4.80 [3.91, 5.83] | 48.7 / 497 |
| money_unequal_null | percentile | 2000/2000 | 82.80 [81.07, 84.43] | 17.20 [15.57, 18.93] | 38.6 / 331 |
| money_unequal_null | basic | 2000/2000 | 91.80 [90.51, 92.97] | 8.20 [7.03, 9.49] | 38.6 / 331 |
| money_unequal_null | bca | 2000/2000 | 74.55 [72.58, 76.45] | 25.45 [23.55, 27.42] | 43.2 / 525 |
| money_unequal_null | bootstrap_t | 2000/2000 | 79.05 [77.20, 80.81] | 20.95 [19.19, 22.80] | 46.7 / 2.49e+03 |
| money_unequal_null | bootstrap_t_symmetric | 2000/2000 | 95.40 [94.39, 96.28] | 4.60 [3.72, 5.61] | 51.9 / 3.56e+03 |
| money_unequal_null | m_out_of_n_basic | 2000/2000 | 93.90 [92.76, 94.91] | 6.10 [5.09, 7.24] | 38.8 / 342 |
| money_unequal_null | welch | 2000/2000 | 89.55 [88.13, 90.86] | 10.45 [9.14, 11.87] | 39.3 / 370 |
| money_large_null | percentile | 2000/2000 | 91.80 [90.51, 92.97] | 8.20 [7.03, 9.49] | 27.7 / 149 |
| money_large_null | basic | 2000/2000 | 97.85 [97.11, 98.44] | 2.15 [1.56, 2.89] | 27.7 / 149 |
| money_large_null | bca | 2000/2000 | 84.30 [82.63, 85.87] | 15.70 [14.13, 17.37] | 29.4 / 245 |
| money_large_null | bootstrap_t | 2000/2000 | 88.10 [86.60, 89.49] | 11.90 [10.51, 13.40] | 29.8 / 476 |
| money_large_null | bootstrap_t_symmetric | 2000/2000 | 98.75 [98.16, 99.19] | 1.25 [0.81, 1.84] | 31.4 / 794 |
| money_large_null | m_out_of_n_basic | 2000/2000 | 99.25 [98.77, 99.58] | 0.75 [0.42, 1.23] | 28.4 / 141 |
| money_large_null | welch | 2000/2000 | 96.65 [95.76, 97.39] | 3.35 [2.61, 4.24] | 27.9 / 171 |
| ratio_null | percentile | 2000/2000 | 94.60 [93.52, 95.55] | 5.40 [4.45, 6.48] | 15.6 / 18.3 |
| ratio_null | basic | 2000/2000 | 94.40 [93.30, 95.37] | 5.60 [4.63, 6.70] | 15.6 / 18.3 |
| ratio_null | bca | 2000/2000 | 94.15 [93.03, 95.14] | 5.85 [4.86, 6.97] | 15.6 / 18.4 |
| ratio_null | bootstrap_t | 2000/2000 | 94.45 [93.35, 95.41] | 5.55 [4.59, 6.65] | 15.8 / 18.5 |
| ratio_null | bootstrap_t_symmetric | 2000/2000 | 94.85 [93.79, 95.78] | 5.15 [4.22, 6.21] | 15.8 / 18.6 |
| ratio_null | m_out_of_n_basic | 2000/2000 | 94.60 [93.52, 95.55] | 5.40 [4.45, 6.48] | 15.6 / 18.3 |
| ratio_null | welch | 0/2000 | unavailable | unavailable | — |
| ratio_effect | percentile | 2000/2000 | 94.40 [93.30, 95.37] | 52.90 [50.68, 55.11] | 15.7 / 18.4 |
| ratio_effect | basic | 2000/2000 | 94.40 [93.30, 95.37] | 52.65 [50.43, 54.86] | 15.7 / 18.4 |
| ratio_effect | bca | 2000/2000 | 94.30 [93.19, 95.28] | 52.70 [50.48, 54.91] | 15.7 / 18.5 |
| ratio_effect | bootstrap_t | 2000/2000 | 94.30 [93.19, 95.28] | 51.35 [49.13, 53.56] | 15.8 / 18.8 |
| ratio_effect | bootstrap_t_symmetric | 2000/2000 | 94.65 [93.57, 95.60] | 51.85 [49.63, 54.06] | 15.9 / 18.8 |
| ratio_effect | m_out_of_n_basic | 2000/2000 | 94.70 [93.63, 95.64] | 52.50 [50.28, 54.71] | 15.7 / 18.6 |
| ratio_effect | welch | 0/2000 | unavailable | unavailable | — |
| ratio_unequal_null | percentile | 2000/2000 | 93.85 [92.71, 94.86] | 6.15 [5.14, 7.29] | 18.9 / 25.1 |
| ratio_unequal_null | basic | 2000/2000 | 93.90 [92.76, 94.91] | 6.10 [5.09, 7.24] | 18.9 / 25.1 |
| ratio_unequal_null | bca | 2000/2000 | 93.80 [92.65, 94.82] | 6.20 [5.18, 7.35] | 18.9 / 25.8 |
| ratio_unequal_null | bootstrap_t | 2000/2000 | 94.30 [93.19, 95.28] | 5.70 [4.72, 6.81] | 19.3 / 27.2 |
| ratio_unequal_null | bootstrap_t_symmetric | 2000/2000 | 94.65 [93.57, 95.60] | 5.35 [4.40, 6.43] | 19.4 / 27.4 |
| ratio_unequal_null | m_out_of_n_basic | 2000/2000 | 93.35 [92.17, 94.40] | 6.65 [5.60, 7.83] | 18.8 / 24.5 |
| ratio_unequal_null | welch | 0/2000 | unavailable | unavailable | — |
| ratio_sparse_null | percentile | 0/2000 | unavailable | unavailable | — |
| ratio_sparse_null | basic | 0/2000 | unavailable | unavailable | — |
| ratio_sparse_null | bca | 0/2000 | unavailable | unavailable | — |
| ratio_sparse_null | bootstrap_t | 0/2000 | unavailable | unavailable | — |
| ratio_sparse_null | bootstrap_t_symmetric | 0/2000 | unavailable | unavailable | — |
| ratio_sparse_null | m_out_of_n_basic | 0/2000 | unavailable | unavailable | — |
| ratio_sparse_null | welch | 0/2000 | unavailable | unavailable | — |
| money_reversed_null | percentile | 2000/2000 | 82.90 [81.18, 84.53] | 17.10 [15.47, 18.82] | 39.9 / 309 |
| money_reversed_null | basic | 2000/2000 | 91.90 [90.62, 93.06] | 8.10 [6.94, 9.38] | 39.9 / 309 |
| money_reversed_null | bca | 2000/2000 | 74.60 [72.63, 76.50] | 25.40 [23.50, 27.37] | 44.7 / 496 |
| money_reversed_null | bootstrap_t | 2000/2000 | 79.35 [77.51, 81.11] | 20.65 [18.89, 22.49] | 47.4 / 2.67e+03 |
| money_reversed_null | bootstrap_t_symmetric | 2000/2000 | 95.25 [94.22, 96.14] | 4.75 [3.86, 5.78] | 52.5 / 4.31e+03 |
| money_reversed_null | m_out_of_n_basic | 2000/2000 | 94.15 [93.03, 95.14] | 5.85 [4.86, 6.97] | 40.3 / 278 |
| money_reversed_null | welch | 2000/2000 | 89.50 [88.07, 90.81] | 10.50 [9.19, 11.93] | 40.9 / 367 |
| money_unequal_effect | percentile | 2000/2000 | 90.25 [88.87, 91.52] | 29.45 [27.46, 31.50] | 46.2 / 416 |
| money_unequal_effect | basic | 2000/2000 | 97.75 [97.00, 98.35] | 16.90 [15.28, 18.62] | 46.2 / 416 |
| money_unequal_effect | bca | 2000/2000 | 79.20 [77.35, 80.96] | 37.35 [35.22, 39.51] | 51.5 / 644 |
| money_unequal_effect | bootstrap_t | 2000/2000 | 85.55 [83.93, 87.06] | 33.60 [31.53, 35.72] | 54.6 / 2.65e+03 |
| money_unequal_effect | bootstrap_t_symmetric | 2000/2000 | 98.95 [98.40, 99.35] | 11.40 [10.04, 12.87] | 59.8 / 3.8e+03 |
| money_unequal_effect | m_out_of_n_basic | 2000/2000 | 98.65 [98.04, 99.11] | 13.30 [11.84, 14.87] | 45.9 / 453 |
| money_unequal_effect | welch | 2000/2000 | 96.25 [95.32, 97.04] | 20.70 [18.94, 22.54] | 46.5 / 460 |
| money_reversed_effect | percentile | 2000/2000 | 75.00 [73.04, 76.88] | 10.95 [9.61, 12.40] | 46.5 / 512 |
| money_reversed_effect | basic | 2000/2000 | 79.55 [77.71, 81.30] | 4.50 [3.63, 5.50] | 46.5 / 512 |
| money_reversed_effect | bca | 2000/2000 | 72.30 [70.28, 74.25] | 20.40 [18.65, 22.23] | 52.9 / 831 |
| money_reversed_effect | bootstrap_t | 2000/2000 | 73.60 [71.61, 75.52] | 14.45 [12.94, 16.07] | 58.2 / 5.31e+03 |
| money_reversed_effect | bootstrap_t_symmetric | 2000/2000 | 83.30 [81.59, 84.91] | 3.35 [2.61, 4.24] | 65.8 / 9.01e+03 |
| money_reversed_effect | m_out_of_n_basic | 2000/2000 | 80.70 [78.90, 82.41] | 3.65 [2.87, 4.57] | 46.2 / 494 |
| money_reversed_effect | welch | 2000/2000 | 78.65 [76.79, 80.43] | 5.45 [4.50, 6.54] | 47.4 / 626 |

Rejection is false-positive rate only for `_null` and deterministic zero/one boundary scenarios; under `_effect` it is power.

### Monetary arm and relative-effect coverage

| Scenario | Method | Estimand | Available | Coverage [MC95] |
|---|---|---|---:|---|
| money_null | percentile | control | 2000/2000 | 73.70 [71.71, 75.62] |
| money_null | percentile | treatment | 2000/2000 | 72.30 [70.28, 74.25] |
| money_null | percentile | relative_effect | 2000/2000 | 91.10 [89.77, 92.31] |
| money_null | basic | control | 2000/2000 | 66.60 [64.48, 68.67] |
| money_null | basic | treatment | 2000/2000 | 65.20 [63.07, 67.29] |
| money_null | basic | relative_effect | 2000/2000 | 73.00 [71.00, 74.94] |
| money_null | bca | control | 2000/2000 | 80.65 [78.85, 82.36] |
| money_null | bca | treatment | 2000/2000 | 79.55 [77.71, 81.30] |
| money_null | bca | relative_effect | 2000/2000 | 86.65 [85.08, 88.11] |
| money_null | bootstrap_t | control | 2000/2000 | 86.65 [85.08, 88.11] |
| money_null | bootstrap_t | treatment | 2000/2000 | 85.40 [83.78, 86.92] |
| money_null | bootstrap_t | relative_effect | 2000/2000 | 89.30 [87.86, 90.62] |
| money_null | bootstrap_t_symmetric | control | 2000/2000 | 81.80 [80.04, 83.47] |
| money_null | bootstrap_t_symmetric | treatment | 2000/2000 | 81.15 [79.37, 82.84] |
| money_null | bootstrap_t_symmetric | relative_effect | 2000/2000 | 87.65 [86.13, 89.06] |
| money_null | m_out_of_n_basic | control | 2000/2000 | 58.20 [56.00, 60.37] |
| money_null | m_out_of_n_basic | treatment | 2000/2000 | 56.85 [54.65, 59.03] |
| money_null | m_out_of_n_basic | relative_effect | 2000/2000 | 83.10 [81.38, 84.72] |
| money_null | welch | control | 2000/2000 | 71.00 [68.96, 72.98] |
| money_null | welch | treatment | 2000/2000 | 69.20 [67.12, 71.22] |
| money_null | welch | relative_effect | 0/2000 | unavailable |
| money_effect | percentile | control | 2000/2000 | 72.30 [70.28, 74.25] |
| money_effect | percentile | treatment | 2000/2000 | 71.80 [69.77, 73.76] |
| money_effect | percentile | relative_effect | 2000/2000 | 89.60 [88.18, 90.90] |
| money_effect | basic | control | 2000/2000 | 66.25 [64.13, 68.32] |
| money_effect | basic | treatment | 2000/2000 | 65.00 [62.86, 67.09] |
| money_effect | basic | relative_effect | 2000/2000 | 73.55 [71.56, 75.47] |
| money_effect | bca | control | 2000/2000 | 79.50 [77.66, 81.25] |
| money_effect | bca | treatment | 2000/2000 | 77.70 [75.81, 79.51] |
| money_effect | bca | relative_effect | 2000/2000 | 85.55 [83.93, 87.06] |
| money_effect | bootstrap_t | control | 2000/2000 | 85.60 [83.98, 87.11] |
| money_effect | bootstrap_t | treatment | 2000/2000 | 84.10 [82.42, 85.68] |
| money_effect | bootstrap_t | relative_effect | 2000/2000 | 88.55 [87.07, 89.91] |
| money_effect | bootstrap_t_symmetric | control | 2000/2000 | 81.15 [79.37, 82.84] |
| money_effect | bootstrap_t_symmetric | treatment | 2000/2000 | 79.40 [77.56, 81.15] |
| money_effect | bootstrap_t_symmetric | relative_effect | 2000/2000 | 88.70 [87.23, 90.06] |
| money_effect | m_out_of_n_basic | control | 2000/2000 | 57.20 [55.00, 59.38] |
| money_effect | m_out_of_n_basic | treatment | 2000/2000 | 57.85 [55.65, 60.03] |
| money_effect | m_out_of_n_basic | relative_effect | 2000/2000 | 83.95 [82.27, 85.53] |
| money_effect | welch | control | 2000/2000 | 69.80 [67.73, 71.81] |
| money_effect | welch | treatment | 2000/2000 | 69.30 [67.23, 71.32] |
| money_effect | welch | relative_effect | 0/2000 | unavailable |
| money_unequal_null | percentile | control | 2000/2000 | 65.25 [63.12, 67.34] |
| money_unequal_null | percentile | treatment | 2000/2000 | 77.40 [75.50, 79.22] |
| money_unequal_null | percentile | relative_effect | 1997/2000 | 82.92 [81.20, 84.55] |
| money_unequal_null | basic | control | 2000/2000 | 57.90 [55.70, 60.08] |
| money_unequal_null | basic | treatment | 2000/2000 | 70.85 [68.80, 72.83] |
| money_unequal_null | basic | relative_effect | 1997/2000 | 80.37 [78.56, 82.09] |
| money_unequal_null | bca | control | 2000/2000 | 73.95 [71.97, 75.86] |
| money_unequal_null | bca | treatment | 2000/2000 | 83.10 [81.38, 84.72] |
| money_unequal_null | bca | relative_effect | 1997/2000 | 84.88 [83.23, 86.42] |
| money_unequal_null | bootstrap_t | control | 1997/2000 | 83.73 [82.03, 85.32] |
| money_unequal_null | bootstrap_t | treatment | 2000/2000 | 87.45 [85.92, 88.87] |
| money_unequal_null | bootstrap_t | relative_effect | 1997/2000 | 86.93 [85.37, 88.38] |
| money_unequal_null | bootstrap_t_symmetric | control | 1997/2000 | 78.77 [76.91, 80.54] |
| money_unequal_null | bootstrap_t_symmetric | treatment | 2000/2000 | 83.15 [81.44, 84.77] |
| money_unequal_null | bootstrap_t_symmetric | relative_effect | 1997/2000 | 93.54 [92.37, 94.58] |
| money_unequal_null | m_out_of_n_basic | control | 2000/2000 | 49.40 [47.19, 51.62] |
| money_unequal_null | m_out_of_n_basic | treatment | 2000/2000 | 65.85 [63.72, 67.93] |
| money_unequal_null | m_out_of_n_basic | relative_effect | 1997/2000 | 90.94 [89.59, 92.16] |
| money_unequal_null | welch | control | 2000/2000 | 62.90 [60.74, 65.02] |
| money_unequal_null | welch | treatment | 2000/2000 | 75.00 [73.04, 76.88] |
| money_unequal_null | welch | relative_effect | 0/2000 | unavailable |
| money_large_null | percentile | control | 2000/2000 | 80.05 [78.23, 81.78] |
| money_large_null | percentile | treatment | 2000/2000 | 80.75 [78.95, 82.46] |
| money_large_null | percentile | relative_effect | 2000/2000 | 91.80 [90.51, 92.97] |
| money_large_null | basic | control | 2000/2000 | 74.35 [72.38, 76.25] |
| money_large_null | basic | treatment | 2000/2000 | 75.90 [73.96, 77.76] |
| money_large_null | basic | relative_effect | 2000/2000 | 80.95 [79.16, 82.65] |
| money_large_null | bca | control | 2000/2000 | 84.65 [82.99, 86.20] |
| money_large_null | bca | treatment | 2000/2000 | 84.35 [82.68, 85.92] |
| money_large_null | bca | relative_effect | 2000/2000 | 87.50 [85.97, 88.92] |
| money_large_null | bootstrap_t | control | 2000/2000 | 87.75 [86.23, 89.16] |
| money_large_null | bootstrap_t | treatment | 2000/2000 | 88.10 [86.60, 89.49] |
| money_large_null | bootstrap_t | relative_effect | 2000/2000 | 90.80 [89.45, 92.03] |
| money_large_null | bootstrap_t_symmetric | control | 2000/2000 | 84.30 [82.63, 85.87] |
| money_large_null | bootstrap_t_symmetric | treatment | 2000/2000 | 84.55 [82.89, 86.11] |
| money_large_null | bootstrap_t_symmetric | relative_effect | 2000/2000 | 92.30 [91.04, 93.43] |
| money_large_null | m_out_of_n_basic | control | 2000/2000 | 65.70 [63.57, 67.78] |
| money_large_null | m_out_of_n_basic | treatment | 2000/2000 | 67.15 [65.04, 69.21] |
| money_large_null | m_out_of_n_basic | relative_effect | 2000/2000 | 91.10 [89.77, 92.31] |
| money_large_null | welch | control | 2000/2000 | 78.15 [76.27, 79.94] |
| money_large_null | welch | treatment | 2000/2000 | 78.55 [76.68, 80.33] |
| money_large_null | welch | relative_effect | 0/2000 | unavailable |
| money_reversed_null | percentile | control | 2000/2000 | 78.25 [76.38, 80.04] |
| money_reversed_null | percentile | treatment | 2000/2000 | 64.70 [62.56, 66.80] |
| money_reversed_null | percentile | relative_effect | 2000/2000 | 82.90 [81.18, 84.53] |
| money_reversed_null | basic | control | 2000/2000 | 71.95 [69.92, 73.91] |
| money_reversed_null | basic | treatment | 2000/2000 | 57.15 [54.95, 59.33] |
| money_reversed_null | basic | relative_effect | 2000/2000 | 63.30 [61.14, 65.42] |
| money_reversed_null | bca | control | 2000/2000 | 84.05 [82.37, 85.63] |
| money_reversed_null | bca | treatment | 2000/2000 | 72.30 [70.28, 74.25] |
| money_reversed_null | bca | relative_effect | 2000/2000 | 81.50 [79.73, 83.18] |
| money_reversed_null | bootstrap_t | control | 2000/2000 | 87.90 [86.39, 89.30] |
| money_reversed_null | bootstrap_t | treatment | 1999/2000 | 83.54 [81.84, 85.14] |
| money_reversed_null | bootstrap_t | relative_effect | 1999/2000 | 87.79 [86.28, 89.20] |
| money_reversed_null | bootstrap_t_symmetric | control | 2000/2000 | 83.60 [81.90, 85.20] |
| money_reversed_null | bootstrap_t_symmetric | treatment | 1999/2000 | 78.49 [76.62, 80.27] |
| money_reversed_null | bootstrap_t_symmetric | relative_effect | 1999/2000 | 83.09 [81.38, 84.71] |
| money_reversed_null | m_out_of_n_basic | control | 2000/2000 | 66.00 [63.88, 68.08] |
| money_reversed_null | m_out_of_n_basic | treatment | 2000/2000 | 50.30 [48.08, 52.51] |
| money_reversed_null | m_out_of_n_basic | relative_effect | 2000/2000 | 69.90 [67.84, 71.90] |
| money_reversed_null | welch | control | 2000/2000 | 75.80 [73.86, 77.66] |
| money_reversed_null | welch | treatment | 2000/2000 | 62.90 [60.74, 65.02] |
| money_reversed_null | welch | relative_effect | 0/2000 | unavailable |
| money_unequal_effect | percentile | control | 2000/2000 | 64.05 [61.90, 66.16] |
| money_unequal_effect | percentile | treatment | 2000/2000 | 75.40 [73.45, 77.27] |
| money_unequal_effect | percentile | relative_effect | 1998/2000 | 83.23 [81.52, 84.85] |
| money_unequal_effect | basic | control | 2000/2000 | 56.65 [54.44, 58.84] |
| money_unequal_effect | basic | treatment | 2000/2000 | 68.05 [65.96, 70.09] |
| money_unequal_effect | basic | relative_effect | 1998/2000 | 79.98 [78.16, 81.71] |
| money_unequal_effect | bca | control | 2000/2000 | 71.90 [69.87, 73.86] |
| money_unequal_effect | bca | treatment | 2000/2000 | 82.35 [80.61, 84.00] |
| money_unequal_effect | bca | relative_effect | 1998/2000 | 83.88 [82.20, 85.47] |
| money_unequal_effect | bootstrap_t | control | 1998/2000 | 81.93 [80.17, 83.60] |
| money_unequal_effect | bootstrap_t | treatment | 2000/2000 | 86.65 [85.08, 88.11] |
| money_unequal_effect | bootstrap_t | relative_effect | 1998/2000 | 84.98 [83.34, 86.52] |
| money_unequal_effect | bootstrap_t_symmetric | control | 1998/2000 | 76.43 [74.50, 78.27] |
| money_unequal_effect | bootstrap_t_symmetric | treatment | 2000/2000 | 81.95 [80.19, 83.61] |
| money_unequal_effect | bootstrap_t_symmetric | relative_effect | 1998/2000 | 91.94 [90.66, 93.10] |
| money_unequal_effect | m_out_of_n_basic | control | 2000/2000 | 48.75 [46.54, 50.97] |
| money_unequal_effect | m_out_of_n_basic | treatment | 2000/2000 | 62.25 [60.08, 64.38] |
| money_unequal_effect | m_out_of_n_basic | relative_effect | 1998/2000 | 88.94 [87.48, 90.28] |
| money_unequal_effect | welch | control | 2000/2000 | 61.70 [59.53, 63.84] |
| money_unequal_effect | welch | treatment | 2000/2000 | 73.45 [71.46, 75.37] |
| money_unequal_effect | welch | relative_effect | 0/2000 | unavailable |
| money_reversed_effect | percentile | control | 2000/2000 | 76.25 [74.32, 78.10] |
| money_reversed_effect | percentile | treatment | 2000/2000 | 64.80 [62.66, 66.90] |
| money_reversed_effect | percentile | relative_effect | 2000/2000 | 82.55 [80.81, 84.19] |
| money_reversed_effect | basic | control | 2000/2000 | 70.10 [68.04, 72.10] |
| money_reversed_effect | basic | treatment | 2000/2000 | 56.75 [54.55, 58.94] |
| money_reversed_effect | basic | relative_effect | 2000/2000 | 62.70 [60.54, 64.82] |
| money_reversed_effect | bca | control | 2000/2000 | 82.95 [81.23, 84.57] |
| money_reversed_effect | bca | treatment | 2000/2000 | 73.40 [71.41, 75.33] |
| money_reversed_effect | bca | relative_effect | 2000/2000 | 80.70 [78.90, 82.41] |
| money_reversed_effect | bootstrap_t | control | 2000/2000 | 87.05 [85.50, 88.49] |
| money_reversed_effect | bootstrap_t | treatment | 1999/2000 | 82.79 [81.06, 84.42] |
| money_reversed_effect | bootstrap_t | relative_effect | 1999/2000 | 87.49 [85.96, 88.91] |
| money_reversed_effect | bootstrap_t_symmetric | control | 2000/2000 | 82.10 [80.35, 83.76] |
| money_reversed_effect | bootstrap_t_symmetric | treatment | 1999/2000 | 77.89 [76.00, 79.69] |
| money_reversed_effect | bootstrap_t_symmetric | relative_effect | 1999/2000 | 82.49 [80.75, 84.13] |
| money_reversed_effect | m_out_of_n_basic | control | 2000/2000 | 64.75 [62.61, 66.85] |
| money_reversed_effect | m_out_of_n_basic | treatment | 2000/2000 | 49.05 [46.84, 51.27] |
| money_reversed_effect | m_out_of_n_basic | relative_effect | 2000/2000 | 69.10 [67.02, 71.12] |
| money_reversed_effect | welch | control | 2000/2000 | 74.15 [72.17, 76.06] |
| money_reversed_effect | welch | treatment | 2000/2000 | 62.25 [60.08, 64.38] |
| money_reversed_effect | welch | relative_effect | 0/2000 | unavailable |

### Paired absolute-effect coverage improvements

Joint coverage differences include unavailable outcomes in the denominator. MCSE uses paired trial indicators.

| Scenario | Method | Gain (pp) | Paired MCSE (pp) | Improved / worsened trials |
|---|---|---:|---:|---:|
| money_null | basic | 7.45 | 0.59 | 149 / 0 |
| money_null | bca | -11.25 | 0.71 | 0 / 225 |
| money_null | bootstrap_t | -4.15 | 0.48 | 7 / 90 |
| money_null | bootstrap_t_symmetric | 8.40 | 0.62 | 168 / 0 |
| money_null | m_out_of_n_basic | 8.15 | 0.61 | 163 / 0 |
| money_null | welch | 6.50 | 0.55 | 130 / 0 |
| money_effect | basic | 7.75 | 0.62 | 160 / 5 |
| money_effect | bca | -9.15 | 0.65 | 1 / 184 |
| money_effect | bootstrap_t | -4.50 | 0.49 | 6 / 96 |
| money_effect | bootstrap_t_symmetric | 9.55 | 0.66 | 193 / 2 |
| money_effect | m_out_of_n_basic | 9.00 | 0.66 | 185 / 5 |
| money_effect | welch | 5.35 | 0.51 | 109 / 2 |
| money_unequal_null | basic | 9.00 | 0.65 | 183 / 3 |
| money_unequal_null | bca | -8.25 | 0.62 | 2 / 167 |
| money_unequal_null | bootstrap_t | -3.75 | 0.47 | 8 / 83 |
| money_unequal_null | bootstrap_t_symmetric | 12.60 | 0.75 | 254 / 2 |
| money_unequal_null | m_out_of_n_basic | 11.10 | 0.72 | 226 / 4 |
| money_unequal_null | welch | 6.75 | 0.57 | 136 / 1 |
| money_large_null | basic | 6.05 | 0.54 | 122 / 1 |
| money_large_null | bca | -7.50 | 0.59 | 0 / 150 |
| money_large_null | bootstrap_t | -3.70 | 0.46 | 6 / 80 |
| money_large_null | bootstrap_t_symmetric | 6.95 | 0.57 | 139 / 0 |
| money_large_null | m_out_of_n_basic | 7.45 | 0.59 | 149 / 0 |
| money_large_null | welch | 4.85 | 0.48 | 97 / 0 |
| money_reversed_null | basic | 9.00 | 0.66 | 184 / 4 |
| money_reversed_null | bca | -8.30 | 0.62 | 1 / 167 |
| money_reversed_null | bootstrap_t | -3.55 | 0.49 | 14 / 85 |
| money_reversed_null | bootstrap_t_symmetric | 12.35 | 0.74 | 249 / 2 |
| money_reversed_null | m_out_of_n_basic | 11.25 | 0.72 | 228 / 3 |
| money_reversed_null | welch | 6.60 | 0.56 | 133 / 1 |
| money_unequal_effect | basic | 7.50 | 0.59 | 150 / 0 |
| money_unequal_effect | bca | -11.05 | 0.70 | 0 / 221 |
| money_unequal_effect | bootstrap_t | -4.70 | 0.51 | 7 / 101 |
| money_unequal_effect | bootstrap_t_symmetric | 8.70 | 0.63 | 174 / 0 |
| money_unequal_effect | m_out_of_n_basic | 8.40 | 0.62 | 168 / 0 |
| money_unequal_effect | welch | 6.00 | 0.53 | 120 / 0 |
| money_reversed_effect | basic | 4.55 | 0.62 | 124 / 33 |
| money_reversed_effect | bca | -2.70 | 0.50 | 24 / 78 |
| money_reversed_effect | bootstrap_t | -1.40 | 0.41 | 20 / 48 |
| money_reversed_effect | bootstrap_t_symmetric | 8.30 | 0.65 | 175 / 9 |
| money_reversed_effect | m_out_of_n_basic | 5.70 | 0.68 | 154 / 40 |
| money_reversed_effect | welch | 3.65 | 0.49 | 85 / 12 |

### Separately declared production tests

These p-values are not derived from bootstrap interval exclusion. Ratio tests are unavailable.

| Scenario | Test | Available [MC95] | Rejection over all trials [MC95] |
|---|---|---|---|
| binary_null | fisher | 100.00 [99.82, 100.00] | 4.10 [3.27, 5.06] |
| binary_effect | fisher | 100.00 [99.82, 100.00] | 15.70 [14.13, 17.37] |
| binary_unequal_null | fisher | 100.00 [99.82, 100.00] | 3.60 [2.83, 4.51] |
| rare_null | fisher | 100.00 [99.82, 100.00] | 0.00 [0.00, 0.18] |
| rare_effect | fisher | 100.00 [99.82, 100.00] | 0.00 [0.00, 0.18] |
| zero_boundary | fisher | 100.00 [99.82, 100.00] | 0.00 [0.00, 0.18] |
| one_boundary | fisher | 100.00 [99.82, 100.00] | 0.00 [0.00, 0.18] |
| money_null | welch | 100.00 [99.82, 100.00] | 2.40 [1.77, 3.17] |
| money_effect | welch | 100.00 [99.82, 100.00] | 4.80 [3.91, 5.83] |
| money_unequal_null | welch | 100.00 [99.82, 100.00] | 10.45 [9.14, 11.87] |
| money_large_null | welch | 100.00 [99.82, 100.00] | 3.35 [2.61, 4.24] |
| ratio_null | unavailable | 0.00 [0.00, 0.18] | 0.00 [0.00, 0.18] |
| ratio_effect | unavailable | 0.00 [0.00, 0.18] | 0.00 [0.00, 0.18] |
| ratio_unequal_null | unavailable | 0.00 [0.00, 0.18] | 0.00 [0.00, 0.18] |
| ratio_sparse_null | unavailable | 0.00 [0.00, 0.18] | 0.00 [0.00, 0.18] |
| money_reversed_null | welch | 100.00 [99.82, 100.00] | 10.50 [9.19, 11.93] |
| money_unequal_effect | welch | 100.00 [99.82, 100.00] | 20.70 [18.94, 22.54] |
| money_reversed_effect | welch | 100.00 [99.82, 100.00] | 5.45 [4.50, 6.54] |

## heavy-tail-resample-sensitivity.json.gz

R=1000; B=9999; seed=214202610; gamma=0.7.

| Scenario | Method | Available | Absolute-effect coverage [MC95] | Reject zero [MC95] | Width median / p99 |
|---|---|---:|---|---|---:|
| money_null | percentile | 1000/1000 | 90.70 [88.73, 92.43] | 9.30 [7.57, 11.27] | 40 / 397 |
| money_null | basic | 1000/1000 | 98.70 [97.79, 99.31] | 1.30 [0.69, 2.21] | 40 / 397 |
| money_null | bca | 1000/1000 | 80.10 [77.49, 82.53] | 19.90 [17.47, 22.51] | 44.9 / 666 |
| money_null | bootstrap_t | 1000/1000 | 87.10 [84.86, 89.12] | 12.90 [10.88, 15.14] | 48.1 / 2.93e+03 |
| money_null | bootstrap_t_symmetric | 1000/1000 | 99.60 [98.98, 99.89] | 0.40 [0.11, 1.02] | 52.5 / 5.06e+03 |
| money_null | m_out_of_n_basic | 1000/1000 | 99.40 [98.70, 99.78] | 0.60 [0.22, 1.30] | 39.4 / 328 |
| money_null | welch | 1000/1000 | 97.60 [96.45, 98.46] | 2.40 [1.54, 3.55] | 40.8 / 440 |
| money_unequal_null | percentile | 1000/1000 | 81.70 [79.16, 84.05] | 18.30 [15.95, 20.84] | 37.5 / 309 |
| money_unequal_null | basic | 1000/1000 | 92.60 [90.80, 94.15] | 7.40 [5.85, 9.20] | 37.5 / 309 |
| money_unequal_null | bca | 1000/1000 | 74.00 [71.16, 76.69] | 26.00 [23.31, 28.84] | 40.9 / 508 |
| money_unequal_null | bootstrap_t | 1000/1000 | 78.20 [75.51, 80.72] | 21.80 [19.28, 24.49] | 44.8 / 2.13e+03 |
| money_unequal_null | bootstrap_t_symmetric | 1000/1000 | 95.60 [94.14, 96.79] | 4.40 [3.21, 5.86] | 49.7 / 3.4e+03 |
| money_unequal_null | m_out_of_n_basic | 1000/1000 | 94.60 [93.01, 95.92] | 5.40 [4.08, 6.99] | 37.2 / 342 |
| money_unequal_null | welch | 1000/1000 | 88.80 [86.68, 90.69] | 11.20 [9.31, 13.32] | 37.9 / 365 |

Rejection is false-positive rate only for `_null` and deterministic zero/one boundary scenarios; under `_effect` it is power.


## heavy-tail-m-sensitivity.json.gz

R=1000; B=1999; seed=214202610; gamma=0.85.

| Scenario | Method | Available | Absolute-effect coverage [MC95] | Reject zero [MC95] | Width median / p99 |
|---|---|---:|---|---|---:|
| money_null | percentile | 1000/1000 | 90.80 [88.84, 92.52] | 9.20 [7.48, 11.16] | 40 / 401 |
| money_null | basic | 1000/1000 | 98.70 [97.79, 99.31] | 1.30 [0.69, 2.21] | 40 / 401 |
| money_null | bca | 1000/1000 | 80.10 [77.49, 82.53] | 19.90 [17.47, 22.51] | 44.5 / 636 |
| money_null | bootstrap_t | 1000/1000 | 87.00 [84.76, 89.02] | 13.00 [10.98, 15.24] | 47.3 / 2.94e+03 |
| money_null | bootstrap_t_symmetric | 1000/1000 | 99.70 [99.13, 99.94] | 0.30 [0.06, 0.87] | 52.4 / 5.01e+03 |
| money_null | m_out_of_n_basic | 1000/1000 | 98.70 [97.79, 99.31] | 1.30 [0.69, 2.21] | 39.8 / 402 |
| money_null | welch | 1000/1000 | 97.60 [96.45, 98.46] | 2.40 [1.54, 3.55] | 40.8 / 440 |
| money_unequal_null | percentile | 1000/1000 | 81.50 [78.95, 83.86] | 18.50 [16.14, 21.05] | 37.2 / 306 |
| money_unequal_null | basic | 1000/1000 | 92.00 [90.14, 93.61] | 8.00 [6.39, 9.86] | 37.2 / 306 |
| money_unequal_null | bca | 1000/1000 | 73.70 [70.85, 76.41] | 26.30 [23.59, 29.15] | 40.6 / 503 |
| money_unequal_null | bootstrap_t | 1000/1000 | 77.30 [74.58, 79.86] | 22.70 [20.14, 25.42] | 44.9 / 2.22e+03 |
| money_unequal_null | bootstrap_t_symmetric | 1000/1000 | 95.70 [94.25, 96.87] | 4.30 [3.13, 5.75] | 49.4 / 3.46e+03 |
| money_unequal_null | m_out_of_n_basic | 1000/1000 | 93.60 [91.90, 95.04] | 6.40 [4.96, 8.10] | 37.4 / 293 |
| money_unequal_null | welch | 1000/1000 | 88.80 [86.68, 90.69] | 11.20 [9.31, 13.32] | 37.9 / 365 |

Rejection is false-positive rate only for `_null` and deterministic zero/one boundary scenarios; under `_effect` it is power.


## heavy-tail-independent.json.gz

R=1000; B=1999; seed=214202611; gamma=0.7.

| Scenario | Method | Available | Absolute-effect coverage [MC95] | Reject zero [MC95] | Width median / p99 |
|---|---|---:|---|---|---:|
| money_unequal_null | percentile | 1000/1000 | 84.10 [81.68, 86.31] | 15.90 [13.69, 18.32] | 40.6 / 355 |
| money_unequal_null | basic | 1000/1000 | 92.00 [90.14, 93.61] | 8.00 [6.39, 9.86] | 40.6 / 355 |
| money_unequal_null | bca | 1000/1000 | 76.60 [73.85, 79.19] | 23.40 [20.81, 26.15] | 46.6 / 562 |
| money_unequal_null | bootstrap_t | 1000/1000 | 80.30 [77.70, 82.72] | 19.70 [17.28, 22.30] | 48.9 / 2.93e+03 |
| money_unequal_null | bootstrap_t_symmetric | 1000/1000 | 94.60 [93.01, 95.92] | 5.40 [4.08, 6.99] | 55.4 / 4.08e+03 |
| money_unequal_null | m_out_of_n_basic | 1000/1000 | 93.90 [92.23, 95.30] | 6.10 [4.70, 7.77] | 40.2 / 344 |
| money_unequal_null | welch | 1000/1000 | 90.20 [88.19, 91.97] | 9.80 [8.03, 11.81] | 41.4 / 409 |
| money_unequal_effect | percentile | 1000/1000 | 91.30 [89.38, 92.97] | 28.70 [25.91, 31.61] | 45.7 / 475 |
| money_unequal_effect | basic | 1000/1000 | 98.70 [97.79, 99.31] | 17.00 [14.72, 19.47] | 45.7 / 475 |
| money_unequal_effect | bca | 1000/1000 | 80.10 [77.49, 82.53] | 34.90 [31.94, 37.95] | 51.2 / 749 |
| money_unequal_effect | bootstrap_t | 1000/1000 | 85.70 [83.38, 87.81] | 32.50 [29.60, 35.50] | 54.2 / 5.14e+03 |
| money_unequal_effect | bootstrap_t_symmetric | 1000/1000 | 99.20 [98.43, 99.65] | 11.60 [9.68, 13.75] | 59.8 / 8.66e+03 |
| money_unequal_effect | m_out_of_n_basic | 1000/1000 | 99.00 [98.17, 99.52] | 14.10 [12.00, 16.41] | 45.1 / 462 |
| money_unequal_effect | welch | 1000/1000 | 97.30 [96.10, 98.21] | 20.60 [18.13, 23.24] | 47 / 587 |

Rejection is false-positive rate only for `_null` and deterministic zero/one boundary scenarios; under `_effect` it is power.


## heavy-tail-reversed-independent.json.gz

R=1000; B=1999; seed=214202611; gamma=0.7.

| Scenario | Method | Available | Absolute-effect coverage [MC95] | Reject zero [MC95] | Width median / p99 |
|---|---|---:|---|---|---:|
| money_reversed_effect | percentile | 1000/1000 | 77.40 [74.68, 79.96] | 9.30 [7.57, 11.27] | 48.5 / 414 |
| money_reversed_effect | basic | 1000/1000 | 80.30 [77.70, 82.72] | 4.60 [3.39, 6.09] | 48.5 / 414 |
| money_reversed_effect | bca | 1000/1000 | 75.00 [72.19, 77.66] | 17.80 [15.48, 20.31] | 55.1 / 672 |
| money_reversed_effect | bootstrap_t | 1000/1000 | 76.70 [73.95, 79.29] | 12.10 [10.14, 14.28] | 58.5 / 4.1e+03 |
| money_reversed_effect | bootstrap_t_symmetric | 1000/1000 | 83.90 [81.47, 86.13] | 2.90 [1.95, 4.14] | 66 / 5.65e+03 |
| money_reversed_effect | m_out_of_n_basic | 1000/1000 | 80.70 [78.11, 83.10] | 3.20 [2.20, 4.49] | 49.2 / 428 |
| money_reversed_effect | welch | 1000/1000 | 79.70 [77.07, 82.15] | 5.30 [3.99, 6.88] | 49.6 / 505 |

Rejection is false-positive rate only for `_null` and deterministic zero/one boundary scenarios; under `_effect` it is power.

