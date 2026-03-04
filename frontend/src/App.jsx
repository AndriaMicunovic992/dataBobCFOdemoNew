import React, { useState, useEffect, useRef, useMemo, useCallback } from "react";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell } from "recharts";
import _ from "lodash";

// ─── DATA ───────────────────────────────────────────────────────
// Data
const _G = [
["2024-12", 391, 8, 106, -3434.0, 2], ["2024-12", 439, 8, 106, 446.88, 2], ["2024-12", 399, 8, 106, -13416.64, 1], ["2024-01", 207, 8, 106, -670.0, 1], ["2025-01", 130, 8, 106, 2.29, 1], ["2024-12", 217, 8, 106, -39589.05, 1], ["2025-12", 118, 8, 106, 907.5, 2], ["2024-12", 197, 8, 106, -1460.0, 2], ["2025-01", 187, 8, 106, -251.33, 3], ["2024-12", 411, 8, 106, -12060.02, 3],
["2024-12", 191, 8, 106, -154.77, 3], ["2025-12", 182, 8, 106, -2350.24, 1], ["2025-12", 406, 8, 106, -820.95, 1], ["2025-01", 197, 8, 106, -420.0, 1], ["2024-12", 408, 8, 106, -1374.08, 1], ["2025-12", 172, 8, 106, -200.0, 2], ["2024-12", 398, 8, 106, -3909.75, 1], ["2024-12", 397, 8, 106, -7909.71, 1], ["2025-12", 203, 8, 106, -220.0, 1], ["2024-01", 157, 8, 106, -478.33, 1],
["2024-12", 127, 8, 106, 4108.56, 3], ["2025-12", 401, 8, 106, -10181.32, 1], ["2025-01", 202, 8, 106, -507.0, 1], ["2025-12", 193, 8, 106, -180.0, 1], ["2024-12", 402, 8, 106, -52241.96, 1], ["2025-01", 154, 8, 106, -6528.08, 2], ["2025-01", 172, 8, 106, -200.0, 1], ["2025-12", 419, 8, 106, -319.5, 1], ["2025-12", 235, 8, 106, -284.52, 3], ["2024-01", 439, 8, 106, 446.89, 3],
["2025-12", 417, 8, 106, -193.74, 2], ["2025-01", 439, 8, 106, 446.88, 2], ["2024-01", 404, 8, 106, -86.21, 1], ["2025-12", 162, 8, 106, -234.0, 2], ["2025-12", 187, 8, 106, -257.71, 4], ["2025-12", 197, 8, 106, -670.0, 1], ["2024-01", 154, 8, 106, -6515.88, 3], ["2024-12", 148, 8, 106, -145.0, 1], ["2024-12", 419, 8, 106, -369.14, 1], ["2024-12", 410, 8, 106, -83.0, 2],
["2024-12", 237, 8, 106, 0.09, 1], ["2025-12", 173, 8, 106, -830.0, 2], ["2025-12", 407, 8, 106, -50.0, 1], ["2024-12", 407, 8, 106, 3629.76, 2], ["2024-12", 193, 8, 106, -1010.1, 2], ["2024-12", 187, 8, 106, -102.49, 2], ["2025-12", 153, 8, 106, -1204.24, 4], ["2025-01", 127, 8, 106, -301.64, 1], ["2024-12", 235, 8, 106, -403.4, 1], ["2025-01", 407, 8, 106, -3060.06, 1],
["2024-12", 139, 8, 106, 19700.0, 1], ["2025-01", 173, 8, 106, -830.0, 1], ["2025-01", 417, 8, 106, -253.69, 1], ["2024-12", 166, 8, 106, -239.4, 2], ["2024-12", 157, 8, 106, -445.0, 1], ["2024-12", 394, 8, 106, -656.69, 2], ["2024-12", 154, 8, 106, -4647.72, 2], ["2025-01", 403, 8, 106, -1000.3, 1], ["2024-01", 191, 8, 106, -452.53, 3], ["2024-12", 417, 8, 106, -4545.0, 1],
["2025-12", 174, 8, 106, -830.0, 2], ["2025-12", 465, 8, 106, 2135.56, 2], ["2025-12", 394, 8, 106, -952.35, 3], ["2024-01", 394, 8, 106, 0.0, 3], ["2024-12", 403, 8, 106, -981.21, 1], ["2025-12", 127, 8, 106, -610.0, 1], ["2024-01", 403, 8, 106, -966.63, 1], ["2025-01", 170, 8, 106, -85.5, 1], ["2025-12", 405, 8, 106, -4067.0, 1], ["2025-01", 411, 8, 106, -697.24, 2],
["2024-12", 153, 8, 106, 1922.48, 3], ["2025-01", 401, 8, 106, -12832.64, 1], ["2025-12", 170, 8, 106, -1283.32, 2], ["2025-12", 452, 8, 106, -330.0, 2], ["2024-12", 135, 8, 106, -14033.38, 1], ["2025-01", 174, 8, 106, -830.0, 1], ["2024-01", 406, 8, 106, -783.75, 1], ["2025-12", 191, 8, 106, -119.7, 2], ["2025-01", 157, 8, 106, -445.0, 1], ["2024-01", 440, 8, 106, -1002.82, 2],
["2024-12", 440, 8, 106, -1009.87, 3], ["2025-12", 440, 8, 106, -1051.26, 3], ["2025-01", 440, 8, 106, -1009.87, 2], ["2025-01", 158, 8, 106, -611.32, 3], ["2025-12", 158, 8, 106, -137.54, 2], ["2024-12", 158, 8, 106, -194.38, 2], ["2024-01", 158, 8, 106, -16.31, 2], ["2025-12", 236, 8, 106, -872.77, 5], ["2024-01", 236, 8, 106, -141.8, 3], ["2024-12", 236, 8, 106, -709.54, 5],
["2025-01", 236, 8, 106, -30.5, 4], ["2025-12", 119, 8, 61, -15.0, 1], ["2025-12", 127, 8, 58, -108.0, 1], ["2025-12", 130, 8, 61, 14.76, 2], ["2025-01", 120, 8, 61, 30.0, 1], ["2024-12", 127, 8, 65, -110.0, 1], ["2025-01", 127, 8, 61, -110.0, 1], ["2025-01", 127, 8, 75, -4.4, 1], ["2025-12", 127, 8, 56, -5.0, 1], ["2025-01", 127, 8, 56, -15.0, 1],
["2025-01", 127, 8, 58, -98.0, 1], ["2024-12", 127, 8, 58, -42.27, 1], ["2024-01", 118, 8, 55, -450.0, 1], ["2024-12", 455, 8, null, 0.0, 1], ["2024-01", 374, 8, null, 0.0, 1], ["2024-01", 375, 8, null, 39877.73, 2], ["2025-01", 374, 8, null, -27142.57, 2], ["2024-12", 376, 8, null, 85362.65, 3], ["2025-01", 362, 8, null, 0.0, 1], ["2025-12", 376, 8, null, 78002.49, 3],
["2024-12", 362, 8, null, 0.0, 1], ["2024-12", 454, 8, null, 0.0, 1], ["2025-01", 66, 8, null, 8715.37, 23], ["2025-12", 427, 8, null, -1830.0, 17], ["2025-12", 32, 8, null, -9262.52, 19], ["2024-01", 362, 8, null, 0.0, 1], ["2025-12", 362, 8, null, 0.0, 1], ["2024-12", 415, 8, 106, 0.0, 1], ["2025-12", 213, 8, 106, 13750.0, 2], ["2025-01", 164, 8, 106, -650.0, 2],
["2024-01", 226, 8, 106, 0.0, 1], ["2024-01", 227, 8, 106, 0.0, 1], ["2025-01", 207, 8, 106, 0.0, 1], ["2025-01", 346, 8, 106, 327.2, 7], ["2025-12", 346, 8, 106, 586.0, 6], ["2024-12", 346, 8, 106, 134.0, 7], ["2025-12", 412, 8, 106, -1138.32, 6], ["2025-01", 412, 8, 106, -916.62, 5], ["2024-12", 412, 8, 106, -5940.56, 7], ["2024-01", 412, 8, 106, -3317.79, 4],
["2024-01", 171, 8, 106, -631.66, 2], ["2025-12", 171, 8, 106, -253.72, 3], ["2025-01", 171, 8, 106, -346.3, 5], ["2024-12", 171, 8, 106, 387.69, 4], ["2025-12", 202, 8, 106, 220.0, 1], ["2025-12", 439, 8, 106, 454.89, 2], ["2025-12", 217, 8, 106, 18.76, 1], ["2024-01", 173, 8, 106, -716.0, 2], ["2024-01", 410, 8, 106, -87.0, 1], ["2025-01", 235, 8, 106, -291.83, 2],
["2024-01", 197, 8, 106, -300.0, 1], ["2024-12", 213, 8, 106, 7453.63, 2], ["2024-12", 170, 8, 106, -1332.82, 2], ["2025-12", 227, 8, 106, 304.8, 3], ["2024-12", 178, 8, 106, -34.0, 1], ["2024-12", 164, 8, 106, -5557.0, 1], ["2025-01", 191, 8, 106, -432.93, 2], ["2024-12", 173, 8, 106, -1310.74, 2], ["2024-01", 153, 8, 106, -74.34, 4], ["2024-01", 178, 8, 106, -34.0, 1],
["2024-01", 148, 8, 106, -3536.49, 3], ["2025-01", 193, 8, 106, 478.19, 2], ["2024-12", 401, 8, 106, 42350.78, 1], ["2024-12", 130, 8, 106, 2.91, 1], ["2025-01", 394, 8, 106, -365.5, 2], ["2025-01", 213, 8, 106, -1250.0, 1], ["2024-12", 465, 8, 106, 28677.55, 1], ["2025-01", 449, 8, 106, -119.09, 1], ["2024-01", 401, 8, 106, -12195.53, 1], ["2024-12", 449, 8, 106, -140.91, 1],
["2025-01", 135, 8, 106, -21505.61, 1], ["2024-01", 411, 8, 106, -593.85, 2], ["2024-01", 449, 8, 106, -140.91, 1], ["2025-12", 130, 8, 106, 83.85, 3], ["2025-12", 449, 8, 106, -181.47, 1], ["2025-12", 135, 8, 106, -18765.6, 1], ["2025-01", 465, 8, 106, 2500.0, 1], ["2025-01", 452, 8, 106, -170.0, 1], ["2024-12", 156, 8, 106, -256.96, 1], ["2024-01", 118, 8, 106, 900.0, 1],
["2025-01", 166, 8, 106, -8.31, 1], ["2025-12", 164, 8, 106, 550.0, 2], ["2025-12", 148, 8, 106, -866.0, 2], ["2024-01", 135, 8, 106, -20916.56, 1], ["2024-01", 405, 8, 106, -3953.63, 1], ["2024-12", 396, 8, 106, -3997.54, 1], ["2025-12", 154, 8, 106, -6447.72, 1], ["2025-01", 405, 8, 106, -4067.0, 1], ["2024-01", 213, 8, 106, -1059.0, 1], ["2025-12", 157, 8, 106, -466.67, 1],
["2025-01", 406, 8, 106, -811.05, 1], ["2024-12", 231, 8, 106, -6721.66, 1], ["2025-12", 411, 8, 106, -290.0, 1], ["2025-12", 404, 8, 106, -90.3, 1], ["2024-12", 404, 8, 106, -87.51, 1], ["2024-12", 405, 8, 106, -4082.01, 1], ["2025-01", 404, 8, 106, -89.22, 1], ["2024-01", 407, 8, 106, -3060.06, 1], ["2024-12", 207, 8, 106, 1340.0, 1], ["2024-01", 193, 8, 106, -218.47, 2],
["2024-12", 400, 8, 106, -3335.64, 1], ["2025-12", 403, 8, 106, -1012.51, 1], ["2025-12", 391, 8, 106, -1575.0, 1], ["2025-01", 156, 8, 106, -348.32, 1], ["2024-12", 227, 8, 106, 13373.77, 2], ["2024-12", 406, 8, 106, -795.57, 1], ["2025-12", 393, 8, 106, -1973.39, 11], ["2024-12", 393, 8, 106, -2970.51, 5], ["2024-01", 393, 8, 106, -143.2, 2], ["2025-01", 393, 8, 106, 5087.07, 2],
["2025-01", 113, 8, 106, 1448.4, 12], ["2024-12", 113, 8, 106, 802.6, 9], ["2025-12", 113, 8, 106, 698.3, 8], ["2024-12", 122, 8, 106, -1080.77, 12], ["2025-12", 122, 8, 106, -2321.02, 11], ["2025-01", 122, 8, 106, -603.16, 3], ["2024-01", 122, 8, 106, -106.48, 2], ["2025-12", 114, 8, 106, 953.73, 4], ["2024-01", 114, 8, 106, 73.37, 3], ["2025-01", 114, 8, 106, 2190.67, 5],
["2024-12", 114, 8, 106, 125.05, 8], ["2024-12", 121, 8, null, 132.6, 12], ["2025-12", 121, 8, null, 167.7, 10], ["2025-01", 121, 8, null, 234.0, 11], ["2025-12", 119, 8, null, 3326.9, 11], ["2025-01", 119, 8, null, 3487.8, 12], ["2024-12", 119, 8, null, 2332.5, 12], ["2024-01", 119, 8, null, 76.6, 2], ["2024-12", 123, 8, 106, -6757.54, 19], ["2025-01", 123, 8, 106, -3300.98, 21],
["2025-12", 123, 8, 106, -7157.24, 19], ["2024-01", 123, 8, 106, -307.79, 6], ["2024-12", 386, 8, null, -7909.71, 1], ["2025-01", 100, 8, null, 16485.71, 2], ["2025-12", 380, 8, null, 9197.56, 2], ["2025-12", 100, 8, null, 14952.26, 2], ["2024-12", 367, 8, null, -1374.08, 1], ["2025-01", 380, 8, null, -281.46, 2], ["2025-12", 377, 8, null, 2470.75, 2], ["2024-12", 380, 8, null, 8719.84, 2],
["2024-12", 378, 8, null, 0.0, 2], ["2024-12", 456, 8, null, -1050.0, 1], ["2024-01", 202, 8, null, -432.73, 1], ["2024-12", 472, 8, null, 893.0, 1], ["2024-01", 378, 8, null, 0.0, 2], ["2024-12", 47, 8, null, -1247.34, 1], ["2025-01", 376, 8, null, -1871.03, 1], ["2025-12", 453, 8, null, -13.51, 1], ["2024-01", 380, 8, null, 177.06, 2], ["2024-12", 351, 8, null, 36148.0, 1],
["2024-12", 389, 8, null, -3335.64, 1], ["2025-12", 374, 8, null, -3874.62, 2], ["2024-12", 388, 8, null, -13416.64, 1], ["2024-01", 453, 8, null, -110.7, 1], ["2024-12", 374, 8, null, 62750.73, 3], ["2025-01", 127, 8, null, -28.2, 1], ["2025-12", 111, 8, null, 6721.66, 1], ["2025-01", 375, 8, null, 44292.56, 3], ["2025-01", 379, 8, null, -15.48, 2], ["2024-01", 379, 8, null, 7.9, 2],
["2024-12", 120, 8, null, 250.0, 2], ["2025-12", 308, 8, null, -351.57, 2], ["2025-12", 471, 8, null, 2135.56, 2], ["2025-01", 356, 8, null, 0.0, 2], ["2024-12", 111, 8, null, 507494.74, 2], ["2024-01", 100, 8, null, -7499.0, 1], ["2024-12", 357, 8, null, 0.0, 2], ["2025-01", 371, 8, null, 0.0, 3], ["2024-12", 368, 8, null, -0.01, 1], ["2024-12", 371, 8, null, 0.0, 2],
["2024-01", 377, 8, null, 500.09, 2], ["2024-12", 426, 8, null, -104578.01, 1], ["2024-12", 377, 8, null, 2580.54, 2], ["2024-01", 355, 8, null, 0.0, 6], ["2025-01", 355, 8, null, 0.0, 3], ["2025-12", 355, 8, null, -0.0, 5], ["2024-12", 355, 8, null, -0.0, 3], ["2024-01", 40, 8, null, -1028.15, 5], ["2024-12", 40, 8, null, -68655.21, 4], ["2025-01", 40, 8, null, 17444.79, 5],
["2025-12", 40, 8, null, 1866.0, 2], ["2025-01", 358, 8, null, 0.0, 4], ["2025-12", 358, 8, null, 0.0, 6], ["2024-12", 358, 8, null, -0.0, 6], ["2025-12", 372, 8, null, 0.0, 6], ["2025-01", 372, 8, null, -0.0, 4], ["2024-12", 372, 8, null, 0.0, 6], ["2025-01", 2, 8, null, 489.53, 9], ["2024-01", 2, 8, null, 232.97, 5], ["2025-12", 2, 8, null, -1997.29, 9],
["2024-12", 2, 8, null, 266.38, 6], ["2024-01", 121, 8, null, 3.9, 1], ["2024-12", 353, 8, null, 0.0, 22], ["2025-12", 353, 8, null, -0.0, 23], ["2024-01", 353, 8, null, 0.0, 12], ["2025-01", 353, 8, null, 0.0, 16], ["2024-12", 118, 8, null, 850.0, 12], ["2025-01", 118, 8, null, 1373.34, 13], ["2025-12", 118, 8, null, 845.0, 9], ["2024-01", 118, 8, null, 13.9, 2],
["2025-01", 378, 8, null, 0.0, 2], ["2025-12", 12, 8, null, 2000.0, 1], ["2024-12", 308, 8, null, -56.36, 1], ["2025-12", 127, 8, null, -100.0, 1], ["2024-12", 375, 8, null, -77128.17, 3], ["2025-01", 357, 8, null, 0.0, 3], ["2024-01", 227, 8, null, 13.3, 1], ["2025-12", 467, 8, null, -952.35, 3], ["2024-12", 379, 8, null, 847.46, 2], ["2024-12", 453, 8, null, -9.66, 1],
["2025-12", 120, 8, null, 158.72, 1], ["2025-12", 110, 8, null, -6721.66, 1], ["2025-12", 381, 8, null, -26859.29, 1], ["2025-12", 378, 8, null, 0.0, 2], ["2024-12", 381, 8, null, 5197.75, 1], ["2024-12", 50, 8, null, -2607.43, 1], ["2025-12", 379, 8, null, 803.27, 2], ["2024-12", 387, 8, null, -3909.75, 1], ["2024-12", 12, 8, null, 100.0, 1], ["2024-12", 100, 8, null, -30120.4, 2],
["2024-12", 36, 8, null, -6293.96, 1], ["2025-01", 377, 8, null, -5.79, 2], ["2024-12", 110, 8, null, -618794.41, 1], ["2025-12", 375, 8, null, -31235.57, 2], ["2024-12", 364, 8, null, 3679.76, 1], ["2024-01", 130, 8, null, 2.57, 1], ["2024-12", 363, 8, null, 964.91, 1], ["2024-01", 376, 8, null, -37764.75, 1], ["2024-12", 62, 8, null, -340.59, 1], ["2024-01", 173, 8, null, -34.0, 1],
["2024-12", 306, 8, null, 37963.45, 19], ["2025-01", 306, 8, null, -167335.95, 21], ["2025-12", 306, 8, null, 69223.15, 19], ["2024-01", 306, 8, null, -127848.56, 21], ["2025-01", 427, 8, null, -16372.93, 21], ["2024-01", 427, 8, null, -207573.71, 14], ["2024-12", 427, 8, null, -548.59, 15], ["2025-01", 5, 8, null, 115196.16, 21], ["2025-12", 5, 8, null, -106732.31, 20], ["2024-01", 5, 8, null, -33637.11, 14],
["2024-12", 5, 8, null, -131592.95, 19], ["2025-12", 369, 8, null, 0.0, 20], ["2024-01", 369, 8, null, 0.0, 9], ["2025-01", 369, 8, null, -0.0, 21], ["2024-12", 369, 8, null, 0.0, 19], ["2024-12", 66, 8, null, 105492.03, 24], ["2024-01", 66, 8, null, 100550.35, 14], ["2025-12", 66, 8, null, 9479.54, 25], ["2025-01", 32, 8, null, 13750.41, 21], ["2024-12", 32, 8, null, -28003.0, 17],
["2024-01", 32, 8, null, -6571.82, 17], ["2024-12", 126, 8, 70, -3120.74, 9], ["2025-01", 126, 8, 70, -3161.8, 9], ["2025-12", 126, 8, 70, -2758.78, 9], ["2025-12", 126, 8, 79, -624.34, 4], ["2025-01", 126, 8, 79, -795.35, 8], ["2024-01", 126, 8, 79, -119.45, 1], ["2024-12", 126, 8, 79, -278.84, 3], ["2025-12", 126, 8, 76, -459.79, 7], ["2025-01", 126, 8, 76, -1328.18, 6],
["2024-01", 126, 8, 76, -2843.84, 2], ["2025-01", 126, 8, 72, -197.29, 2], ["2025-01", 126, 8, 66, -10619.5, 2], ["2025-12", 126, 8, 60, -463.66, 3], ["2025-01", 126, 8, 60, -37.09, 9], ["2025-12", 126, 8, 80, -2047.7, 4], ["2024-01", 126, 8, 78, -2512.16, 3], ["2024-12", 126, 8, 80, -530.4, 3], ["2025-12", 126, 8, 55, -3299.01, 2], ["2025-12", 126, 8, 72, -196.0, 4],
["2024-01", 126, 8, 72, -586.84, 3], ["2025-01", 126, 8, 80, -927.01, 3], ["2024-01", 126, 8, 55, -3113.5, 2], ["2025-01", 126, 8, 78, -659.4, 3], ["2024-01", 126, 8, 80, -476.75, 1], ["2024-12", 126, 8, 66, -1189.67, 2], ["2024-12", 126, 8, 55, -1800.96, 2], ["2024-12", 126, 8, 60, -268.92, 3], ["2025-12", 128, 8, 61, -2847.1, 4], ["2025-01", 128, 8, 61, -40512.69, 5],
["2024-12", 128, 8, 61, -136.15, 7], ["2024-01", 128, 8, 61, 148.52, 2], ["2024-12", 128, 8, 76, -7.2, 1], ["2025-12", 128, 8, 74, -426.22, 1], ["2024-12", 128, 8, 62, 11.31, 1], ["2024-12", 128, 8, 57, -167.76, 2], ["2024-12", 128, 8, 75, 168.35, 2], ["2024-12", 128, 8, 60, 0.0, 1], ["2024-12", 128, 8, 83, -67.33, 1], ["2024-12", 128, 8, 56, 75.9, 1],
["2024-12", 128, 8, 84, -55.96, 1], ["2025-01", 126, 8, 74, -1996.65, 13], ["2024-12", 126, 8, 74, 3121.44, 16], ["2025-12", 126, 8, 74, 2637.69, 16], ["2024-01", 126, 8, 74, -660.39, 6], ["2024-01", 126, 8, 75, -8742.5, 7], ["2025-01", 126, 8, 75, -7797.84, 17], ["2024-12", 126, 8, 75, -3119.53, 8], ["2025-12", 126, 8, 75, -7407.17, 12], ["2025-12", 126, 8, 65, -10081.94, 12],
["2025-01", 126, 8, 65, -16253.08, 11], ["2024-12", 126, 8, 65, -19903.48, 8], ["2024-01", 126, 8, 65, -1779.0, 1], ["2024-01", 126, 8, 59, -1380.06, 2], ["2025-01", 126, 8, 59, -27267.67, 13], ["2024-12", 126, 8, 59, -3375.0, 8], ["2025-12", 126, 8, 59, -39.1, 3], ["2025-01", 126, 8, 58, -1187.05, 10], ["2024-12", 126, 8, 58, -1367.8, 7], ["2025-12", 126, 8, 58, -878.3, 6],
["2024-01", 126, 8, 58, -387.32, 2], ["2025-12", 126, 8, 62, -480.45, 8], ["2025-01", 126, 8, 62, -182.62, 10], ["2024-01", 126, 8, 62, -364.07, 3], ["2024-12", 126, 8, 62, -422.62, 11], ["2024-01", 126, 8, 70, -2565.37, 1], ["2025-01", 126, 8, 56, -32832.63, 19], ["2024-01", 126, 8, 56, -56038.48, 15], ["2024-12", 126, 8, 56, -9104.74, 17], ["2025-12", 126, 8, 56, -22640.33, 17],
["2024-01", 126, 8, 57, -13824.27, 12], ["2025-01", 126, 8, 57, -8792.49, 19], ["2024-12", 126, 8, 57, -5437.53, 11], ["2025-12", 126, 8, 57, -16416.17, 14], ["2024-12", 126, 8, 61, -70096.28, 17], ["2024-01", 126, 8, 61, -124174.17, 17], ["2025-12", 126, 8, 61, -71928.62, 18], ["2025-01", 126, 8, 61, -31920.44, 21], ["2024-12", 126, 8, 86, 0.0, 1], ["2025-01", 112, 8, 60, -409.81, 4],
["2025-01", 112, 8, 79, 1049.26, 4], ["2025-01", 112, 8, 66, 11431.82, 1], ["2024-12", 112, 8, 55, 2585.7, 1], ["2025-01", 112, 8, 80, 1207.85, 1], ["2024-12", 112, 8, 79, 354.48, 2], ["2024-12", 112, 8, 66, 1261.34, 1], ["2025-12", 112, 8, 55, 5281.05, 1], ["2025-12", 112, 8, 72, 323.5, 2], ["2025-12", 112, 8, 76, 761.46, 2], ["2025-12", 112, 8, 74, -535.75, 8],
["2024-12", 112, 8, 74, 1663.1, 8], ["2025-01", 112, 8, 74, 2692.68, 7], ["2025-12", 112, 8, 75, 11847.84, 5], ["2025-01", 112, 8, 75, 9714.15, 9], ["2024-12", 112, 8, 75, 1691.88, 3], ["2025-01", 112, 8, 59, 32389.92, 5], ["2025-12", 112, 8, 59, 62.4, 1], ["2024-12", 112, 8, 59, 4284.9, 2], ["2025-12", 112, 8, 62, 691.5, 3], ["2025-01", 112, 8, 62, 232.85, 5],
["2024-12", 112, 8, 62, 641.28, 6], ["2025-12", 112, 8, 65, 11598.49, 4], ["2025-01", 112, 8, 65, 17550.47, 5], ["2024-12", 112, 8, 65, 21452.69, 5], ["2025-01", 112, 8, 58, 1785.6, 3], ["2024-12", 112, 8, 58, 2057.36, 3], ["2025-12", 112, 8, 58, 1416.16, 2], ["2025-12", 112, 8, 70, 3283.59, 3], ["2025-01", 112, 8, 70, 3995.17, 3], ["2024-12", 112, 8, 70, 4136.61, 3],
["2024-12", 112, 8, 80, 759.38, 1], ["2025-12", 112, 8, 79, 892.98, 1], ["2025-01", 112, 8, 76, 1826.43, 3], ["2024-12", 112, 8, 60, 385.25, 1], ["2025-01", 112, 8, 78, 955.8, 1], ["2025-12", 112, 8, 80, 3159.71, 1], ["2025-01", 112, 8, 72, 328.0, 1], ["2025-12", 112, 8, 60, 681.1, 1], ["2025-01", 112, 8, 61, 104764.97, 17], ["2024-01", 112, 8, 61, 580.17, 4],
["2025-12", 112, 8, 61, 107154.49, 11], ["2024-12", 112, 8, 61, 100272.92, 13], ["2025-12", 112, 8, 56, 27405.29, 12], ["2024-12", 112, 8, 56, 17438.07, 11], ["2024-01", 112, 8, 56, 45.0, 1], ["2025-01", 112, 8, 56, 38036.97, 12], ["2025-01", 112, 8, 57, 10085.82, 11], ["2024-12", 112, 8, 57, 6392.76, 7], ["2025-12", 112, 8, 57, 19195.38, 9],
];
const _A = [
["110000", "Kundenguthaben", "Forderungen aus L/L Dritte", 5, "Bilanz", "Forderungen aus L/L Dritte", "Forderungen aus L/L", "Umlaufvermögen", "Aktiven"],
["110901", "Einzelwertberichtigung", "Delkredere", 363, "Bilanz", "Delkredere", "Forderungen aus L/L", "Umlaufvermögen", "Aktiven"],
["114020", "Darlehen Personal kurzfristig", "Übrige kurzfristige Forderungen ggü. Dritten", 12, "Bilanz", "Übrige kurzfristige Forderungen ggü. Dritten", "Übrige kfr. Forderungen", "Umlaufvermögen", "Aktiven"],
["117100", "Guthaben Körperschaftssteuern", "Kf. Forderungen ggü. staatlichen Stellen", 351, "Bilanz", "Kf. Forderungen ggü. staatlichen Stellen", "Übrige kfr. Forderungen", "Umlaufvermögen", "Aktiven"],
["117110", "Vorsteuer 20% (A)", "Kf. Forderungen ggü. staatlichen Stellen", 353, "Bilanz", "Kf. Forderungen ggü. staatlichen Stellen", "Übrige kfr. Forderungen", "Umlaufvermögen", "Aktiven"],
["117120", "Vorsteuer 10% (A)", "Kf. Forderungen ggü. staatlichen Stellen", 355, "Bilanz", "Kf. Forderungen ggü. staatlichen Stellen", "Übrige kfr. Forderungen", "Umlaufvermögen", "Aktiven"],
["117125", "Vorsteuer 13% (A)", "Kf. Forderungen ggü. staatlichen Stellen", 356, "Bilanz", "Kf. Forderungen ggü. staatlichen Stellen", "Übrige kfr. Forderungen", "Umlaufvermögen", "Aktiven"],
["117130", "Vorsteuer 20% aus Einkauf UStBBKV (A)", "Kf. Forderungen ggü. staatlichen Stellen", 357, "Bilanz", "Kf. Forderungen ggü. staatlichen Stellen", "Übrige kfr. Forderungen", "Umlaufvermögen", "Aktiven"],
["117135", "Vorsteuer aus i.g. Erwerb (EU)", "Kf. Forderungen ggü. staatlichen Stellen", 358, "Bilanz", "Kf. Forderungen ggü. staatlichen Stellen", "Übrige kfr. Forderungen", "Umlaufvermögen", "Aktiven"],
["117160", "Einfuhrumsatzsteuer Finanzamt 20%", "Kf. Forderungen ggü. staatlichen Stellen", 362, "Bilanz", "Kf. Forderungen ggü. staatlichen Stellen", "Übrige kfr. Forderungen", "Umlaufvermögen", "Aktiven"],
["118050", "Rückkaufsanspruch Lebensversicherung", "Kf. Forderrungen ggü. SV und VE", 364, "Bilanz", "Kf. Forderrungen ggü. SV und VE", "Übrige kfr. Forderungen", "Umlaufvermögen", "Aktiven"],
["120000", "Warenbestand", "Vorräte und nicht fakturierte Dienstleistungen", 32, "Bilanz", "Vorräte und nicht fakturierte Dienstleistungen", "Vorräte", "Umlaufvermögen", "Aktiven"],
["120001", "Vorrat Verpackungsmaterial", "Vorräte und nicht fakturierte Dienstleistungen", 472, "Bilanz", "Vorräte und nicht fakturierte Dienstleistungen", "Vorräte", "Umlaufvermögen", "Aktiven"],
["120810", "Wertberichtigung Waren (Warendrittel)", "Vorräte und nicht fakturierte Dienstleistungen", 36, "Bilanz", "Vorräte und nicht fakturierte Dienstleistungen", "Vorräte", "Umlaufvermögen", "Aktiven"],
["130000", "Aktive Abgrenzungen", "Aktive Rechnungsabgrenzung", 40, "Bilanz", "Aktive Rechnungsabgrenzung", "Aktive Rechnungsabgrenzung", "Umlaufvermögen", "Aktiven"],
["130001", "Aktive Abgrenzungen Vormerkungen", "Aktive Rechnungsabgrenzung", 467, "Bilanz", "Aktive Rechnungsabgrenzung", "Aktive Rechnungsabgrenzung", "Umlaufvermögen", "Aktiven"],
["130003", "Aktive Abgrenzungen Zinserträge aus Finanzanlagen", "Aktive Rechnungsabgrenzung", 471, "Bilanz", "Aktive Rechnungsabgrenzung", "Aktive Rechnungsabgrenzung", "Umlaufvermögen", "Aktiven"],
["150000", "Maschinen und maschinelle Anlagen", "Mobile Sachanlagen", 47, "Bilanz", "Mobile Sachanlagen", "Mobile Sachanlagen", "Anlagevermögen", "Aktiven"],
["150030", "Büromobilien und Geräte", "Mobile Sachanlagen", 50, "Bilanz", "Mobile Sachanlagen", "Mobile Sachanlagen", "Anlagevermögen", "Aktiven"],
["150040", "Geringwertige Wirtschaftsgüter", "Mobile Sachanlagen", 367, "Bilanz", "Mobile Sachanlagen", "Mobile Sachanlagen", "Anlagevermögen", "Aktiven"],
["157000", "Mieterausbauten (Gebäude)", "Mobile Sachanlagen", 62, "Bilanz", "Mobile Sachanlagen", "Mobile Sachanlagen", "Anlagevermögen", "Aktiven"],
["159000", "Verkaufskatalog", "Mobile Sachanlagen", 368, "Bilanz", "Mobile Sachanlagen", "Mobile Sachanlagen", "Anlagevermögen", "Aktiven"],
["200300", "Purchase accrual", "Verbindlichkeiten auf L/L Dritte", 427, "Bilanz", "Verbindlichkeiten auf L/L Dritte", "Verbindlichkeiten aus L/L", "Kurzfristiges Fremdkapital", "Passiven"],
["220100", "Umsatzsteuer (A)", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", 369, "Bilanz", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", "Übrige kfr. Verbindlichkeiten", "Kurzfristiges Fremdkapital", "Passiven"],
["220120", "Umsatzsteuer 20% aus Einkauf UStBBKV (A)", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", 371, "Bilanz", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", "Übrige kfr. Verbindlichkeiten", "Kurzfristiges Fremdkapital", "Passiven"],
["220130", "Umsatzsteuer aus i.g. Erwerb (EU)", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", 372, "Bilanz", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", "Übrige kfr. Verbindlichkeiten", "Kurzfristiges Fremdkapital", "Passiven"],
["220150", "Verrechnungskonto UST", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", 374, "Bilanz", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", "Übrige kfr. Verbindlichkeiten", "Kurzfristiges Fremdkapital", "Passiven"],
["220200", "Verbindlichkeit Finanzamt (Mwst)", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", 375, "Bilanz", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", "Übrige kfr. Verbindlichkeiten", "Kurzfristiges Fremdkapital", "Passiven"],
["220210", "Verbindlichkeit Finanzamt Einfuhrumsatzsteuer", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", 376, "Bilanz", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", "Übrige kfr. Verbindlichkeiten", "Kurzfristiges Fremdkapital", "Passiven"],
["220220", "Verbindlichkeit Finanzamt (L, DG, DZ)", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", 377, "Bilanz", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", "Übrige kfr. Verbindlichkeiten", "Kurzfristiges Fremdkapital", "Passiven"],
["220230", "Verbindlichkeit Mitarbeiter (Nettolohn)", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", 378, "Bilanz", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", "Übrige kfr. Verbindlichkeiten", "Kurzfristiges Fremdkapital", "Passiven"],
["220240", "Verbindlichkeit Gemeinde (Kommunalsteuer)", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", 379, "Bilanz", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", "Übrige kfr. Verbindlichkeiten", "Kurzfristiges Fremdkapital", "Passiven"],
["220250", "Verbindlichkeit Krankenkasse (Sozialversicherung)", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", 380, "Bilanz", "Kf. Verbindlichkeiten ggü. staatlichen Stellen", "Übrige kfr. Verbindlichkeiten", "Kurzfristiges Fremdkapital", "Passiven"],
["221010", "Sonstige Verbindlichkeiten", "Übrige kf. Verbindlichkeiten ggü. Dritten", 381, "Bilanz", "Übrige kf. Verbindlichkeiten ggü. Dritten", "Übrige kfr. Verbindlichkeiten", "Kurzfristiges Fremdkapital", "Passiven"],
["230010", "Transitorische Passiven allgemein", "Passive Rechnungsabgrenzung und kf. Rückstellungen", 100, "Bilanz", "Passive Rechnungsabgrenzung und kf. Rückstellungen", "Passive Rechnungsabgrenzung", "Kurzfristiges Fremdkapital", "Passiven"],
["233030", "Rückstellung Resturlaub", "Rückstellungen", 386, "Bilanz", "Rückstellungen", "Rückstellungen", "Langfristiges Fremdkapital", "Passiven"],
["233040", "Rückstellung Jubiläumsgeld", "Rückstellungen", 387, "Bilanz", "Rückstellungen", "Rückstellungen", "Langfristiges Fremdkapital", "Passiven"],
["233050", "Rückstellung Abfertigung", "Rückstellungen", 388, "Bilanz", "Rückstellungen", "Rückstellungen", "Langfristiges Fremdkapital", "Passiven"],
["233060", "Freie Rücklagen", "Rückstellungen", 389, "Bilanz", "Rückstellungen", "Rückstellungen", "Langfristiges Fremdkapital", "Passiven"],
["264500", "Rückstellung Rechts-/Beratungskosten", "Rückstellungen", 456, "Bilanz", "Rückstellungen", "Rückstellungen", "Langfristiges Fremdkapital", "Passiven"],
["297000", "Bilanzgewinnvortrag", "Bilanzgewinnvortrag", 110, "Bilanz", "Bilanzgewinnvortrag", "Bilanzgewinnvortrag", "Eigenkapital", "Passiven"],
["920000", "verbuchter Gewinn / Verlust", "Jahresgewinn", 231, "Gewinn und Verlust", "Jahresgewinn", "Jahresgewinn", "Erfolgsrechnung", ""],
["999000", "Datenmigration (technisch)", "Bilanzgewinnvortrag", 426, "Bilanz", "Bilanzgewinnvortrag", "Bilanzgewinnvortrag", "???", ""],
["297900", "Jahresgewinn", "Jahresgewinn", 111, "Bilanz", "Jahresgewinn", "Jahresgewinn", "Eigenkapital", "Passiven"],
["100000", "Kasse", "Kasse", 2, "Bilanz", "Kasse", "Flüssige Mittel", "Umlaufvermögen", "Aktiven"],
["102610", "BACA, Hard 03843306600", "Bankguthaben", 306, "Bilanz", "Bankguthaben", "Flüssige Mittel", "Umlaufvermögen", "Aktiven"],
["102710", "Raiba, Hard 63776", "Bankguthaben", 308, "Bilanz", "Bankguthaben", "Flüssige Mittel", "Umlaufvermögen", "Aktiven"],
["102711", "Girokonto Hypo Vorarlberg 13499443018-GI", "Bankguthaben", 453, "Bilanz", "Bankguthaben", "Flüssige Mittel", "Umlaufvermögen", "Aktiven"],
["105809", "Festgeld Raiba AT633743191", "Bankguthaben", 454, "Bilanz", "Bankguthaben", "Flüssige Mittel", "Umlaufvermögen", "Aktiven"],
["105812", "Festgeld Hypo Vorarlberg 13499443026", "Bankguthaben", 455, "Bilanz", "Bankguthaben", "Flüssige Mittel", "Umlaufvermögen", "Aktiven"],
["200010", "Warenkreditoren EUR", "Verbindlichkeiten auf L/L Dritte", 66, "Bilanz", "Verbindlichkeiten auf L/L Dritte", "Verbindlichkeiten aus L/L", "Kurzfristiges Fremdkapital", "Passiven"],
["680000", "Abschreibungen", "Abschreibungen", 213, "Gewinn und Verlust", "Abschreibungen", "Abschreibungen", "Erfolgsrechnung", ""],
["320000", "Warenverkauf", "Handelswarenverkauf", 112, "Gewinn und Verlust", "Warenerlöse", "Umsatz", "Erfolgsrechnung", ""],
["340000", "Ertrag Atteste", "Dienstleistungsverkauf", 113, "Gewinn und Verlust", "Ertrag aus Nebenerlösen & Dienstleistungen", "Fakturierte Nebenerlöse", "Erfolgsrechnung", ""],
["340010", "Ertrag andere Dienstleistungen", "Dienstleistungsverkauf", 114, "Gewinn und Verlust", "Ertrag aus Nebenerlösen & Dienstleistungen", "Fakturierte Nebenerlöse", "Erfolgsrechnung", ""],
["340050", "Verrechnete Sägekosten", "Dienstleistungsverkauf", 346, "Gewinn und Verlust", "Fakturierte Schnittkosten", "Fakturierte Nebenerlöse", "Erfolgsrechnung", ""],
["360000", "Verrechnete Verpackung", "Verrechnete Frachten/Verpackung", 118, "Gewinn und Verlust", "Fakturierte Verpackung", "Fakturierte Nebenerlöse", "Erfolgsrechnung", ""],
["360010", "Verrechnete Fracht", "Verrechnete Frachten/Verpackung", 119, "Gewinn und Verlust", "Fakturierte Fracht/ Porti", "Fakturierte Nebenerlöse", "Erfolgsrechnung", ""],
["360020", "Verrechneter Positionszuschlag", "Verrechnete Positionszuschläge und Auftragspauschalen", 120, "Gewinn und Verlust", "Fakturierter Positionszuschlag", "Fakturierte Nebenerlöse", "Erfolgsrechnung", ""],
["360030", "Verrechnete Auftragspauschale", "Verrechnete Positionszuschläge und Auftragspauschalen", 121, "Gewinn und Verlust", "Fakturierter Positionszuschlag", "Fakturierte Nebenerlöse", "Erfolgsrechnung", ""],
["380000", "Frachtkosten Lagerausgang", "Ausgangsfrachten", 122, "Gewinn und Verlust", "Ausgangs- und Retourfrachten", "Erlösminderungen", "Erfolgsrechnung", ""],
["380010", "Kundenskonti", "Kundenskonti", 123, "Gewinn und Verlust", "Skonti & Abzüge", "Erlösminderungen", "Erfolgsrechnung", ""],
["420000", "Warenaufwand", "Handelwarenaufwand", 126, "Gewinn und Verlust", "Warenaufwand", "Warenaufwand", "Erfolgsrechnung", ""],
["427000", "Eingangsfrachten", "Eingangsfrachten", 127, "Gewinn und Verlust", "Eingangsfrachten", "Bezugskosten", "Erfolgsrechnung", ""],
["427010", "Zollabfertigung", "Eingangsfrachten", 391, "Gewinn und Verlust", "Eingangsfrachten", "Bezugskosten", "Erfolgsrechnung", ""],
["427030", "Zoll", "Eingangsfrachten", 393, "Gewinn und Verlust", "Eingangsfrachten", "Bezugskosten", "Erfolgsrechnung", ""],
["427040", "Vorgemerkte Sicherheiten Zoll", "Eingangsfrachten", 394, "Gewinn und Verlust", "Eingangsfrachten", "Bezugskosten", "Erfolgsrechnung", ""],
["428000", "Inventar Zu- und Abnahme", "Bestandesänderungen und Warenverluste", 128, "Gewinn und Verlust", "Warenaufwand", "Warenaufwand", "Erfolgsrechnung", ""],
["429000", "Lieferantenskonti", "Lieferantenskonti", 130, "Gewinn und Verlust", "Lieferantenskonti", "Bezugskosten", "Erfolgsrechnung", ""],
["520000", "Löhne & Gehälter", "Lohnaufwand", 135, "Gewinn und Verlust", "Lohnaufwand", "Personalaufwand", "Erfolgsrechnung", ""],
["520040", "Boni (variabel)", "Lohnaufwand", 139, "Gewinn und Verlust", "Lohnaufwand", "Personalaufwand", "Erfolgsrechnung", ""],
["520070", "Personalaufwand - IC", "Lohnaufwand", 452, "Gewinn und Verlust", "Lohnaufwand", "Personalaufwand", "Erfolgsrechnung", ""],
["520110", "Mitarbeitervorsorgebeiträge (Abfertigung)", "Lohnaufwand", 396, "Gewinn und Verlust", "Lohnaufwand", "Personalaufwand", "Erfolgsrechnung", ""],
["520140", "Urlaubsgeldrückstellung", "Lohnaufwand", 397, "Gewinn und Verlust", "Lohnaufwand", "Personalaufwand", "Erfolgsrechnung", ""],
["520150", "Jubiläumsgeldrückstellung", "Lohnaufwand", 398, "Gewinn und Verlust", "Lohnaufwand", "Personalaufwand", "Erfolgsrechnung", ""],
["520160", "Abfertigungsrückstellung", "Lohnaufwand", 399, "Gewinn und Verlust", "Lohnaufwand", "Personalaufwand", "Erfolgsrechnung", ""],
["520170", "Pensionsrückstellung", "Lohnaufwand", 400, "Gewinn und Verlust", "Lohnaufwand", "Personalaufwand", "Erfolgsrechnung", ""],
["527100", "Beiträge zur Sozialversicherung", "Sozialversicherungsaufwand", 401, "Gewinn und Verlust", "Sozialversicherungsaufwand", "Personalaufwand", "Erfolgsrechnung", ""],
["527110", "Sozialversicherung Dienstnehmer-Anteile", "Sozialversicherungsaufwand", 402, "Gewinn und Verlust", "Sozialversicherungsaufwand", "Personalaufwand", "Erfolgsrechnung", ""],
["527120", "Dienstgeberbeitrag", "Sozialversicherungsaufwand", 403, "Gewinn und Verlust", "Sozialversicherungsaufwand", "Personalaufwand", "Erfolgsrechnung", ""],
["527130", "Zuschlag zum Dienstgeberbeitrag", "Sozialversicherungsaufwand", 404, "Gewinn und Verlust", "Sozialversicherungsaufwand", "Personalaufwand", "Erfolgsrechnung", ""],
["527140", "Lohnsteuer", "Sozialversicherungsaufwand", 405, "Gewinn und Verlust", "Sozialversicherungsaufwand", "Personalaufwand", "Erfolgsrechnung", ""],
["527150", "Kommunalsteuer", "Sozialversicherungsaufwand", 406, "Gewinn und Verlust", "Sozialversicherungsaufwand", "Personalaufwand", "Erfolgsrechnung", ""],
["527160", "Lebensversicherung zu Gunsten Mitarbeiter", "Sozialversicherungsaufwand", 407, "Gewinn und Verlust", "Sozialversicherungsaufwand", "Personalaufwand", "Erfolgsrechnung", ""],
["528000", "Personalschulung", "Übriger Personalaufwand", 148, "Gewinn und Verlust", "Übriger Personalaufwand", "Personalaufwand", "Erfolgsrechnung", ""],
["528050", "anderer Personalaufwand", "Übriger Personalaufwand", 153, "Gewinn und Verlust", "Übriger Personalaufwand", "Personalaufwand", "Erfolgsrechnung", ""],
["600000", "Mietaufwand", "Raumaufwand", 154, "Gewinn und Verlust", "Raumaufwand", "Raumaufwand", "Erfolgsrechnung", ""],
["600020", "Ertrag Untermiete", "Raumaufwand", 439, "Gewinn und Verlust", "Raumaufwand", "Raumaufwand", "Erfolgsrechnung", ""],
["605000", "Gebäudeunterhalt", "Gebäudeunterhalt und Reinigung", 156, "Gewinn und Verlust", "Raumaufwand", "Raumaufwand", "Erfolgsrechnung", ""],
["610000", "Strom", "Energie, Wasser & Entsorgung", 157, "Gewinn und Verlust", "Energieaufwand", "Energieaufwand", "Erfolgsrechnung", ""],
["615000", "Verbrauchsmaterial und Betriebsmittel (< CHF 500)", "Betriebsaufwand Logistik", 158, "Gewinn und Verlust", "Betriebsaufwand Logistik", "Betriebsaufwand Logistik", "Erfolgsrechnung", ""],
["615005", "GWG/ Neuanschaffungen (< 400 EUR)", "Betriebsaufwand Logistik", 408, "Gewinn und Verlust", "Betriebsaufwand Logistik", "Betriebsaufwand Logistik", "Erfolgsrechnung", ""],
["615040", "Arbeitsbekleidung Anschaffung & Unterhalt", "Betriebsaufwand Logistik", 162, "Gewinn und Verlust", "Betriebsaufwand Logistik", "Betriebsaufwand Logistik", "Erfolgsrechnung", ""],
["615060", "Verpackungsmaterial", "Betriebsaufwand Logistik", 164, "Gewinn und Verlust", "Betriebsaufwand Logistik", "Betriebsaufwand Logistik", "Erfolgsrechnung", ""],
["620000", "Büromaterial/Verbrauchsmaterial", "Büromaterial und Drucksachen", 166, "Gewinn und Verlust", "Büroaufwand", "Übriger Betriebsaufwand", "Erfolgsrechnung", ""],
["625000", "Frankaturen", "Frankaturen", 170, "Gewinn und Verlust", "Büroaufwand", "Übriger Betriebsaufwand", "Erfolgsrechnung", ""],
["630000", "Telekommunikation/Mobile", "Kommunikationsaufwand", 171, "Gewinn und Verlust", "IT Aufwand", "IT Aufwand", "Erfolgsrechnung", ""],
["635000", "IT Anschaffungen < CHF 5'000", "Informatik", 172, "Gewinn und Verlust", "IT Aufwand", "IT Aufwand", "Erfolgsrechnung", ""],
["635010", "IT Lizenzen/Wartungen/Abonnemente", "Informatik", 173, "Gewinn und Verlust", "IT Aufwand", "IT Aufwand", "Erfolgsrechnung", ""],
["635020", "IT Dienstleistungssupport", "Informatik", 174, "Gewinn und Verlust", "IT Aufwand", "IT Aufwand", "Erfolgsrechnung", ""],
["640015", "Insertionskosten Fachpresse", "Werbund und Marketing", 178, "Gewinn und Verlust", "Marketing", "Marketing", "Erfolgsrechnung", ""],
["640035", "Werbegeschenke", "Werbund und Marketing", 182, "Gewinn und Verlust", "Marketing", "Marketing", "Erfolgsrechnung", ""],
["640060", "Einladungen/Geschenke extern", "Werbund und Marketing", 187, "Gewinn und Verlust", "Marketing", "Marketing", "Erfolgsrechnung", ""],
["640070", "Werbung übrige", "Werbund und Marketing", 410, "Gewinn und Verlust", "Marketing", "Marketing", "Erfolgsrechnung", ""],
["645020", "Spesen Chauffeur", "Reisen", 449, "Gewinn und Verlust", "Reise- und Fahrzeugaufwand", "Übriger Betriebsaufwand", "Erfolgsrechnung", ""],
["650000", "Fahrzeugkosten allgemein", "Fahrzeugaufwand", 191, "Gewinn und Verlust", "Reise- und Fahrzeugaufwand", "Übriger Betriebsaufwand", "Erfolgsrechnung", ""],
["650020", "Autokosten LKW 0 % MWST", "Fahrzeugaufwand", 411, "Gewinn und Verlust", "Reise- und Fahrzeugaufwand", "Übriger Betriebsaufwand", "Erfolgsrechnung", ""],
["650030", "Autokosten LKW 20 % MWST", "Fahrzeugaufwand", 412, "Gewinn und Verlust", "Reise- und Fahrzeugaufwand", "Übriger Betriebsaufwand", "Erfolgsrechnung", ""],
["650040", "PKW-Leasing", "Fahrzeugaufwand", 440, "Gewinn und Verlust", "Reise- und Fahrzeugaufwand", "Übriger Betriebsaufwand", "Erfolgsrechnung", ""],
["655000", "Sachversicherung", "Sachversicherungen", 193, "Gewinn und Verlust", "Verwaltungsaufwand", "Übriger Betriebsaufwand", "Erfolgsrechnung", ""],
["660000", "Revision & Steuerberatung", "Fremde Dienstleistungen", 197, "Gewinn und Verlust", "Verwaltungsaufwand", "Übriger Betriebsaufwand", "Erfolgsrechnung", ""],
["665000", "Bank- & Postcheckspesen", "Übriger Verwaltungsaufwand", 236, "Gewinn und Verlust", "Verwaltungsaufwand", "Übriger Betriebsaufwand", "Erfolgsrechnung", ""],
["665010", "Zeitungen / Zeitschriften / Fachbücher", "Übriger Verwaltungsaufwand", 202, "Gewinn und Verlust", "Verwaltungsaufwand", "Übriger Betriebsaufwand", "Erfolgsrechnung", ""],
["665020", "Verbandsbeiträge", "Übriger Verwaltungsaufwand", 203, "Gewinn und Verlust", "Verwaltungsaufwand", "Übriger Betriebsaufwand", "Erfolgsrechnung", ""],
["665040", "Verwaltungskosten andere", "Übriger Verwaltungsaufwand", 235, "Gewinn und Verlust", "Verwaltungsaufwand", "Übriger Betriebsaufwand", "Erfolgsrechnung", ""],
["670040", "Pauschale Einzelwertberichtigung", "Debitorenverluste", 415, "Gewinn und Verlust", "Debitorenverluste", "Übriger Betriebsaufwand", "Erfolgsrechnung", ""],
["675000", "Dienstleistungsertrag allgemein", "Dienstleistungsertrag", 207, "Gewinn und Verlust", "Verrechnete Dienstleistungen Gruppe", "Übriger Betriebsaufwand", "Erfolgsrechnung", ""],
["690040", "Kleindifferenzen", "Finanzaufwand", 237, "Gewinn und Verlust", "Finanzaufwand", "Finanzerfolg", "Erfolgsrechnung", ""],
["695000", "Zinsertrag auf Bank-und Postguthaben", "Finanzertrag", 217, "Gewinn und Verlust", "Finanzertrag", "Finanzerfolg", "Erfolgsrechnung", ""],
["695001", "Zinsertrag aus Finanzanlagen", "Finanzertrag", 465, "Gewinn und Verlust", "Finanzertrag", "Finanzerfolg", "Erfolgsrechnung", ""],
["850040", "ausserordentlicher Aufwand", "Ausserordentlicher Erfolg", 226, "Gewinn und Verlust", "Ausserordentlicher Erfolg", "Ausserordentlicher Erfolg", "Erfolgsrechnung", ""],
["850045", "ausserdordentlicher Ertrag", "Ausserordentlicher Erfolg", 227, "Gewinn und Verlust", "Ausserordentlicher Erfolg", "Ausserordentlicher Erfolg", "Erfolgsrechnung", ""],
["890020", "Körperschaftssteuer", "Direkte Steuern", 417, "Gewinn und Verlust", "Steueraufwand", "Steueraufwand", "Erfolgsrechnung", ""],
["890040", "Kammerumlage", "Direkte Steuern", 419, "Gewinn und Verlust", "Steueraufwand", "Steueraufwand", "Erfolgsrechnung", ""],
];
const _C = [["DAT", "Company accounts data", 1, ""], ["DYNA", "Dynaflex Steel AG", 2, "CHF"], ["FUER", "Fürsorgestiftung der Hans Kohler AG", 3, "CHF"], ["HKAG", "Hans Kohler AG", 4, "CHF"], ["INDR", "Indrohag \"Industrie-Rohmaterial und Handels AG \"", 5, "CHF"], ["KIMO", "Hans Kohler Immobilien AG", 6, "CHF"], ["PLAS", "Plastolit AG", 7, "CHF"], ["STAD", "Stadelmann GmbH", 8, "EUR"], ["Unknown", "", -9999999, ""]];
const _I = [
["STAD", "10", "Kund.spez. Rohr/Fitting", 55, "10 Kund.spez. Rohr/Fitting"],
["STAD", "11", "Geschw. Rundrohre", 56, "11 Geschw. Rundrohre"],
["STAD", "12", "Formrohre", 57, "12 Formrohre"],
["STAD", "13", "Nahtlose Rohre", 58, "13 Nahtlose Rohre"],
["STAD", "14", "Press-System", 59, "14 Press-System"],
["STAD", "16", "Steriltechnik", 60, "16 Steriltechnik"],
["STAD", "17", "Fittings", 61, "17 Fittings"],
["STAD", "18", "Geländerbauteile", 62, "18 Geländerbauteile"],
["STAD", "21", "Bleche kaltgewalzt", 65, "21 Bleche kaltgewalzt"],
["STAD", "22", "Bleche warmgewalzt", 66, "22 Bleche warmgewalzt"],
["STAD", "26", "Blech added-value", 70, "26 Blech added-value"],
["STAD", "29", "andere Prod. Blech", 72, "29 andere Prod. Blech"],
["STAD", "31", "Blank-/Automatenstahl", 74, "31 Blank-/Automatenstahl"],
["STAD", "32", "Stabstahl gewalzt", 75, "32 Stabstahl gewalzt"],
["STAD", "33", "Stabstahl geschnitten", 76, "33 Stabstahl geschnitten"],
["STAD", "35", "Hohlstahl", 78, "35 Hohlstahl"],
["STAD", "36", "Profile deko-geschliffen", 79, "36 Profile deko-geschliffen"],
["STAD", "38", "Sonderprofile", 80, "38 Sonderprofile"],
["STAD", "51", "Schrauben", 83, "51 Schrauben"],
["STAD", "52", "Muttern/Zubehör", 84, "52 Muttern/Zubehör"],
["STAD", "59", "andere Prod. Schrauben", 86, "59 andere Prod. Schrauben"],
["STAD", "990", "Kostenträger Firma", 106, "990 Kostenträger Firma"],
];

function hydrateData() {
  const toObj = (hs, r) => { const o = {}; hs.forEach((h, i) => { o[h] = r[i]; }); return o; };
  const glH = ["period","main_account_id","company_id","item_group_id","amount","entry_count"];
  const acH = ["Hauptkonto-Nr.","Hauptkonto","Hauptkontokategorie","main_account_id","Hauptkontotyp","Reporting H1","Reporting H2","Reporting H3","Reporting H4"];
  const coH = ["Unternehmen Business ID","Unternehmen","company_id","Buchungswährung"];
  const igH = ["Unternehmen Business ID","Warengruppe Business ID","Warengruppename","item_group_id","Warengruppe Nr Name"];
  return {
    gl_entries: { headers: glH, data: _G.map(r => toObj(glH, r)) },
    accounts: { headers: acH, data: _A.map(r => toObj(acH, r)) },
    companies: { headers: coH, data: _C.map(r => toObj(coH, r)) },
    item_groups: { headers: igH, data: _I.map(r => toObj(igH, r)) },
  };
}
const SAMPLE_DATA_RAW = hydrateData();

// ─── THEME ──────────────────────────────────────────────────────
const FONT_URL = "https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:ital,wght@0,300;0,400;0,500;0,600;0,700;0,800;1,400&family=IBM+Plex+Mono:wght@400;500&display=swap";
const C = {
  brand: "#6abbd9",
  brandLight: "#6abbd915",
  brandMid: "#6abbd930",
  brandDark: "#3a8aa8",
  bg: "#fafbfc",
  white: "#ffffff",
  surface: "#ffffff",
  surfaceHover: "#f4f7f9",
  border: "#e8ecf0",
  borderLight: "#f0f2f5",
  text: "#1a2b3c",
  textSec: "#5a6b7c",
  textMuted: "#8a96a3",
  green: "#22a06b",
  greenBg: "#22a06b12",
  red: "#cf1322",
  redBg: "#cf132212",
  amber: "#d97706",
  amberBg: "#d9770612",
  purple: "#7c4dff",
  purpleBg: "#7c4dff12",
};
const SC_COLORS = ["#6abbd9", "#7c4dff", "#f97316", "#ec4899", "#22a06b", "#eab308"];
const ROLE_COLORS = { key: C.amber, measure: C.brand, attribute: C.textMuted, time: C.green, ignore: C.border };

const fmt = n => n == null ? "—" : new Intl.NumberFormat("de-DE", { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(n);
const fmtS = n => { if (n == null) return "—"; const a = Math.abs(n); if (a >= 1e6) return (n / 1e6).toFixed(1) + "M"; if (a >= 1e3) return (n / 1e3).toFixed(1) + "K"; return n.toFixed(0); };

// ─── STYLES ─────────────────────────────────────────────────────
const S = {
  card: {
    background: C.white, border: `1px solid ${C.border}`, borderRadius: 12,
    padding: 20, marginBottom: 14, boxShadow: "0 1px 3px rgba(0,0,0,0.04)"
  },
  cardT: {
    fontSize: 11, fontWeight: 700, color: C.textMuted, textTransform: "uppercase",
    letterSpacing: "0.6px", marginBottom: 12
  },
  th: {
    textAlign: "left", padding: "8px 10px", borderBottom: `2px solid ${C.border}`,
    color: C.textMuted, fontWeight: 600, fontSize: 10, textTransform: "uppercase",
    letterSpacing: "0.5px", position: "sticky", top: 0, background: C.white, zIndex: 1
  },
  td: { padding: "6px 10px", borderBottom: `1px solid ${C.borderLight}`, fontSize: 12, color: C.text },
  mono: { fontFamily: "'IBM Plex Mono', monospace", fontSize: 11, fontWeight: 500 },
  badge: (color) => ({
    display: "inline-flex", alignItems: "center", gap: 4, padding: "2px 8px",
    borderRadius: 20, fontSize: 10, fontWeight: 600, background: color + "14",
    color: color, border: `1px solid ${color}25`
  }),
  tag: (color = C.brand) => ({
    display: "inline-flex", alignItems: "center", gap: 5, padding: "4px 10px",
    borderRadius: 6, fontSize: 11, fontWeight: 500, background: color + "12",
    color: color, border: `1px solid ${color}22`, cursor: "pointer", userSelect: "none"
  }),
  btn: (variant = "primary", small = false) => ({
    padding: small ? "5px 12px" : "8px 16px", borderRadius: 8, border: "none",
    cursor: "pointer", fontSize: small ? 11 : 13, fontWeight: 600,
    fontFamily: "'Plus Jakarta Sans', sans-serif", transition: "all .15s",
    background: variant === "primary" ? C.brand : variant === "danger" ? C.red
      : variant === "active" ? C.brandLight : C.white,
    color: variant === "primary" ? "#fff" : variant === "danger" ? "#fff"
      : variant === "active" ? C.brand : C.textSec,
    border: variant === "primary" || variant === "danger" ? "none" : `1px solid ${C.border}`,
  }),
  input: {
    background: C.white, border: `1px solid ${C.border}`, borderRadius: 8,
    padding: "7px 12px", color: C.text, fontSize: 12,
    fontFamily: "'Plus Jakarta Sans', sans-serif", outline: "none", width: "100%",
    transition: "border-color .15s",
  },
  select: {
    background: C.white, border: `1px solid ${C.border}`, borderRadius: 8,
    padding: "7px 12px", color: C.text, fontSize: 12,
    fontFamily: "'Plus Jakarta Sans', sans-serif", outline: "none",
  },
  dropdown: {
    position: "absolute", top: "100%", left: 0, zIndex: 60, background: C.white,
    border: `1px solid ${C.border}`, borderRadius: 10, padding: 8, marginTop: 4,
    minWidth: 220, maxHeight: 240, overflow: "auto",
    boxShadow: "0 12px 32px rgba(0,0,0,0.12), 0 2px 6px rgba(0,0,0,0.06)"
  },
};

// ─── DATA ENGINE ────────────────────────────────────────────────
const ROLE_OPTIONS = ["key", "measure", "attribute", "time", "ignore"];

function autoRole(name, vals) {
  const c = vals.filter(v => v != null);
  if (!c.length) return "ignore";
  const nr = c.filter(v => typeof v === "number").length / c.length;
  const isId = name.toLowerCase().includes("_id") || name.toLowerCase().endsWith("nr");
  const isDate = c.some(v => typeof v === "string" && /^\d{4}-\d{2}/.test(v));
  if (isDate) return "time";
  if (isId) return "key";
  if (nr > 0.8 && !isId && name !== "entry_count") return "measure";
  return "attribute";
}

function discoverSchema(tables) {
  const schema = {};
  for (const [name, table] of Object.entries(tables)) {
    const { headers, data } = table;
    const columns = headers.map(h => {
      const vals = data.slice(0, 200).map(r => r[h]);
      return { name: h, role: autoRole(h, vals), uniqueCount: new Set(vals.filter(v => v != null).map(String)).size };
    });
    schema[name] = {
      columns,
      isFact: columns.some(c => c.role === "measure") && columns.filter(c => c.role === "key").length >= 2,
      rowCount: data.length
    };
  }
  return schema;
}

function autoDiscoverRelationships(tables, schema) {
  const rels = [];
  const ns = Object.keys(schema);
  for (let i = 0; i < ns.length; i++) {
    for (let j = i + 1; j < ns.length; j++) {
      const shared = schema[ns[i]].columns.map(c => c.name)
        .filter(c => schema[ns[j]].columns.some(d => d.name === c) && c.includes("_id"));
      for (const col of shared) {
        const v1 = new Set(tables[ns[i]].data.map(r => r[col]).filter(v => v != null).map(String));
        const v2 = new Set(tables[ns[j]].data.map(r => r[col]).filter(v => v != null).map(String));
        const ov = [...v1].filter(v => v2.has(v)).length;
        rels.push({
          id: `${ns[i]}-${ns[j]}-${col}`,
          from: ns[i], to: ns[j], fromCol: col, toCol: col,
          coverage: Math.round(ov / Math.min(v1.size || 1, v2.size || 1) * 100),
          overlapCount: ov
        });
      }
    }
  }
  return rels;
}

function buildBaseline(tables, schema, relationships) {
  const fn = Object.entries(schema).find(([, s]) => s.isFact)?.[0];
  if (!fn) return [];
  const lk = {};
  for (const rel of relationships) {
    const dimName = rel.from === fn ? rel.to : rel.to === fn ? rel.from : null;
    if (!dimName || !tables[dimName]) continue;
    const dimKeyCol = rel.from === fn ? rel.toCol : rel.fromCol;
    const factKeyCol = rel.from === fn ? rel.fromCol : rel.toCol;
    if (lk[dimName]) continue;
    const map = {};
    for (const row of tables[dimName].data) map[row[dimKeyCol]] = row;
    const dimCols = schema[dimName]?.columns.filter(c => c.role === "attribute" || c.role === "time") || [];
    lk[dimName] = { map, factKeyCol, dimCols };
  }
  return tables[fn].data.map(row => {
    const e = { ...row };
    if (row.period) { e._year = row.period.slice(0, 4); e._month = row.period.slice(5, 7); e._period = row.period; }
    for (const [, l] of Object.entries(lk)) {
      const dr = l.map[row[l.factKeyCol]];
      if (dr) for (const dc of l.dimCols) e[dc.name] = dr[dc.name];
    }
    return e;
  });
}

function getDimFields(bl) {
  if (!bl.length) return [];
  const nums = new Set(["amount", "entry_count"]);
  const skip = new Set(["company_id"]);
  return Object.keys(bl[0]).filter(k => !nums.has(k) && !skip.has(k) && typeof bl[0][k] !== "number").sort();
}
function getMeasureFields(bl) {
  if (!bl.length) return [];
  return Object.keys(bl[0]).filter(k => typeof bl[0][k] === "number" && k !== "entry_count" && !k.endsWith("_id"));
}
function getUniq(bl, f) { return [...new Set(bl.map(r => r[f]).filter(v => v != null))].sort(); }
function applyFilters(data, filters) {
  return data.filter(r => {
    for (const [f, vs] of Object.entries(filters)) { if (vs.length && !vs.includes(r[f])) return false; }
    return true;
  });
}

function computePivot(data, rowFs, colF, valF, sortMode = "value_desc") {
  if (!rowFs.length || !valF) return { rows: [], colKeys: [] };
  const groups = {};
  const colKeysSet = new Set();
  for (const r of data) {
    const rk = rowFs.map(f => r[f] ?? "—").join(" | ");
    const ck = colF ? (r[colF] ?? "—") : null;
    if (ck) colKeysSet.add(ck);
    if (!groups[rk]) { groups[rk] = { _key: rk, _total: 0 }; rowFs.forEach(f => groups[rk][f] = r[f] ?? "—"); }
    const v = r[valF] || 0;
    groups[rk]._total += v;
    if (ck) { groups[rk][ck] = (groups[rk][ck] || 0) + v; }
  }
  let rows = Object.values(groups);
  const firstF = rowFs[0];
  const isTime = firstF && (firstF.includes("period") || firstF.includes("_year") || firstF.includes("_month") || firstF.includes("date") || firstF === "period");
  if (sortMode === "none") {
    // no sort - caller handles it
  } else if (sortMode === "time_asc" || (sortMode === "auto" && isTime)) {
    rows.sort((a, b) => String(a[firstF] ?? "").localeCompare(String(b[firstF] ?? "")));
  } else if (sortMode === "time_desc") {
    rows.sort((a, b) => String(b[firstF] ?? "").localeCompare(String(a[firstF] ?? "")));
  } else {
    rows.sort((a, b) => b._total - a._total);
  }
  return { rows, colKeys: [...colKeysSet].sort() };
}

function applyRules(data, rules) {
  let res = data.map(r => ({ ...r }));
  for (const rule of rules) {
    const matchIdx = [];
    res.forEach((r, i) => {
      let m = true;
      for (const [k, v] of Object.entries(rule.filters || {})) {
        if (!v || (Array.isArray(v) && v.length === 0)) continue;
        if (Array.isArray(v)) { if (!v.includes(r[k])) m = false; }
        else { if (r[k] !== v) m = false; }
      }
      if (rule.periodFrom && (r._period || r.period) < rule.periodFrom) m = false;
      if (rule.periodTo && (r._period || r.period) > rule.periodTo) m = false;
      if (m) matchIdx.push(i);
    });
    if (rule.type === "multiplier") {
      for (const i of matchIdx) res[i] = { ...res[i], amount: Math.round(res[i].amount * rule.factor * 100) / 100 };
    } else if (rule.type === "offset" && matchIdx.length > 0) {
      // Count distinct periods among matching rows
      const periodCounts = {};
      for (const i of matchIdx) {
        const p = res[i]._period || res[i].period || "all";
        periodCounts[p] = (periodCounts[p] || 0) + 1;
      }
      const numPeriods = Object.keys(periodCounts).length;
      const perPeriod = rule.offset / (numPeriods || 1);
      // Within each period, split evenly across matching rows
      for (const i of matchIdx) {
        const p = res[i]._period || res[i].period || "all";
        const rowsInPeriod = periodCounts[p] || 1;
        const share = perPeriod / rowsInPeriod;
        res[i] = { ...res[i], amount: Math.round((res[i].amount + share) * 100) / 100 };
      }
    }
  }
  return res;
}

// ─── FIELD MANAGER ──────────────────────────────────────────────
function FieldManager({ label, allFields, selected, onChange, color = C.brand, single = false }) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const ref = useRef(null);
  useEffect(() => {
    const h = e => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, []);
  const available = allFields.filter(f => single ? true : !selected.includes(f))
    .filter(f => !search || f.toLowerCase().includes(search.toLowerCase()));

  return (
    <div style={{ marginBottom: 4 }}>
      <div style={{ fontSize: 10, color: C.textMuted, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 5, fontWeight: 600 }}>{label}</div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 4, alignItems: "center" }}>
        {(single ? [selected].filter(Boolean) : selected).map(f => (
          <span key={f} style={S.tag(color)}>
            {f.replace(/_/g, " ")}
            <span onClick={() => onChange(single ? "" : selected.filter(x => x !== f))} style={{ cursor: "pointer", opacity: 0.5, fontSize: 13, lineHeight: 1 }}>×</span>
          </span>
        ))}
        <div ref={ref} style={{ position: "relative" }}>
          <button onClick={() => { setOpen(!open); setSearch(""); }} style={{ ...S.btn("ghost", true), padding: "4px 8px", fontSize: 11, color: color, border: `1px dashed ${color}44` }}>+</button>
          {open && (
            <div style={S.dropdown}>
              <input autoFocus style={{ ...S.input, marginBottom: 6, fontSize: 11 }} placeholder="Search fields..." value={search} onChange={e => setSearch(e.target.value)} />
              {available.length === 0 && <div style={{ fontSize: 11, color: C.textMuted, padding: 6 }}>No fields</div>}
              {available.map(f => (
                <div key={f} onClick={() => { onChange(single ? f : [...selected, f]); if (single) setOpen(false); }}
                  style={{ padding: "6px 8px", borderRadius: 6, cursor: "pointer", fontSize: 12, color: C.text }}
                  onMouseEnter={e => e.target.style.background = C.surfaceHover}
                  onMouseLeave={e => e.target.style.background = ""}>
                  {f.replace(/_/g, " ")}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ─── FILTER MANAGER ─────────────────────────────────────────────
function FilterManager({ baseline, allFields, filters, setFilters }) {
  const [addOpen, setAddOpen] = useState(false);
  const [addSearch, setAddSearch] = useState("");
  const [expandedF, setExpandedF] = useState(null);
  const [valSearch, setValSearch] = useState("");
  const activeFilterFields = Object.keys(filters);
  const availableFs = allFields.filter(f => !activeFilterFields.includes(f))
    .filter(f => !addSearch || f.toLowerCase().includes(addSearch.toLowerCase()));
  const addRef = useRef(null);
  useEffect(() => {
    const h = e => { if (addRef.current && !addRef.current.contains(e.target)) setAddOpen(false); };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, []);

  return (
    <div style={{ marginBottom: 4 }}>
      <div style={{ fontSize: 10, color: C.textMuted, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 5, fontWeight: 600 }}>Filters</div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 4, alignItems: "center" }}>
        {activeFilterFields.map(f => {
          const vals = filters[f] || [];
          const expanded = expandedF === f;
          const allVals = getUniq(baseline, f);
          const fVals = valSearch ? allVals.filter(v => String(v).toLowerCase().includes(valSearch.toLowerCase())) : allVals;
          return (
            <div key={f} style={{ position: "relative" }}>
              <span onClick={() => { setExpandedF(expanded ? null : f); setValSearch(""); }}
                style={{ ...S.tag(vals.length ? C.amber : C.textMuted) }}>
                {f.replace(/_/g, " ")}{vals.length ? ` (${vals.length})` : ""}
                <span onClick={e => { e.stopPropagation(); const nf = { ...filters }; delete nf[f]; setFilters(nf); setExpandedF(null); }}
                  style={{ cursor: "pointer", opacity: .5, fontSize: 13 }}>×</span>
              </span>
              {expanded && (
                <div style={S.dropdown}>
                  <input autoFocus style={{ ...S.input, marginBottom: 6, fontSize: 11 }} placeholder="Search values..." value={valSearch} onChange={e => setValSearch(e.target.value)} />
                  {fVals.slice(0, 80).map(v => {
                    const ch = vals.includes(v);
                    return (
                      <label key={String(v)} style={{ display: "flex", alignItems: "center", gap: 6, padding: "4px 4px", fontSize: 12, color: C.text, cursor: "pointer", borderRadius: 4 }}>
                        <input type="checkbox" checked={ch} onChange={() => setFilters({ ...filters, [f]: ch ? vals.filter(x => x !== v) : [...vals, v] })}
                          style={{ accentColor: C.brand }} />
                        {String(v)}
                      </label>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
        <div ref={addRef} style={{ position: "relative" }}>
          <button onClick={() => { setAddOpen(!addOpen); setAddSearch(""); }}
            style={{ ...S.btn("ghost", true), padding: "4px 8px", fontSize: 11, color: C.amber, border: `1px dashed ${C.amber}44` }}>+ filter</button>
          {addOpen && (
            <div style={S.dropdown}>
              <input autoFocus style={{ ...S.input, marginBottom: 6, fontSize: 11 }} placeholder="Search fields..." value={addSearch} onChange={e => setAddSearch(e.target.value)} />
              {availableFs.map(f => (
                <div key={f} onClick={() => { setFilters({ ...filters, [f]: [] }); setAddOpen(false); }}
                  style={{ padding: "6px 8px", borderRadius: 6, cursor: "pointer", fontSize: 12, color: C.text }}
                  onMouseEnter={e => e.target.style.background = C.surfaceHover}
                  onMouseLeave={e => e.target.style.background = ""}>
                  {f.replace(/_/g, " ")}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ─── PIVOT TABLE ────────────────────────────────────────────────
function PivotTableView({ data, rowFs, colF, valF, colorFn }) {
  const [sortCol, setSortCol] = useState(null);
  const [sortDir, setSortDir] = useState("asc");
  const { rows: rawRows, colKeys } = useMemo(() => computePivot(data, rowFs, colF, valF, "none"), [data, rowFs, colF, valF]);

  // Default: auto sort by first field if time-like, else by _total desc
  const rows = useMemo(() => {
    const arr = [...rawRows];
    const col = sortCol;
    const dir = sortDir;
    if (!col) {
      const f0 = rowFs[0];
      const isTime = f0 && (f0.includes("period") || f0.includes("_year") || f0.includes("_month") || f0 === "period");
      if (isTime) arr.sort((a, b) => String(a[f0] ?? "").localeCompare(String(b[f0] ?? "")));
      else arr.sort((a, b) => b._total - a._total);
      return arr;
    }
    arr.sort((a, b) => {
      const av = a[col] ?? a._total ?? 0;
      const bv = b[col] ?? b._total ?? 0;
      if (typeof av === "number" && typeof bv === "number") return dir === "asc" ? av - bv : bv - av;
      return dir === "asc" ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
    });
    return arr;
  }, [rawRows, sortCol, sortDir, rowFs]);

  function toggleSort(col) {
    if (sortCol === col) setSortDir(d => d === "asc" ? "desc" : "asc");
    else { setSortCol(col); setSortDir(typeof (rawRows[0]?.[col]) === "number" ? "desc" : "asc"); }
  }

  const arrow = col => sortCol === col ? (sortDir === "asc" ? " ↑" : " ↓") : "";
  const thClick = (col, align = "left") => ({
    ...S.th, textAlign: align, cursor: "pointer", userSelect: "none",
    color: sortCol === col ? C.brand : S.th.color,
  });

  if (!rowFs.length || !valF) return <div style={{ color: C.textMuted, fontSize: 12, padding: 20, textAlign: "center" }}>Add row fields and a measure.</div>;
  const hasCols = colF && colKeys.length > 0;
  return (
    <div style={{ maxHeight: 420, overflow: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead><tr>
          {rowFs.map(f => <th key={f} style={thClick(f)} onClick={() => toggleSort(f)}>{f.replace(/_/g, " ")}{arrow(f)}</th>)}
          {hasCols ? colKeys.map(ck => <th key={ck} style={thClick(ck, "right")} onClick={() => toggleSort(ck)}>{String(ck)}{arrow(ck)}</th>)
            : <th style={thClick("_total", "right")} onClick={() => toggleSort("_total")}>{valF}{arrow("_total")}</th>}
          {hasCols && <th style={thClick("_total", "right")} onClick={() => toggleSort("_total")}>Total{arrow("_total")}</th>}
        </tr></thead>
        <tbody>
          {rows.slice(0, 120).map((r, i) => (
            <tr key={i} style={{ background: i % 2 === 0 ? "" : C.bg }}>
              {rowFs.map(f => <td key={f} style={S.td}>{String(r[f] ?? "—")}</td>)}
              {hasCols ? colKeys.map(ck => {
                const v = r[ck] || 0;
                return <td key={ck} style={{ ...S.td, ...S.mono, textAlign: "right", color: colorFn ? colorFn(v) : v >= 0 ? C.green : C.red }}>{fmt(v)}</td>;
              }) : <td style={{ ...S.td, ...S.mono, textAlign: "right", color: colorFn ? colorFn(r._total) : r._total >= 0 ? C.green : C.red }}>{fmt(r._total)}</td>}
              {hasCols && <td style={{ ...S.td, ...S.mono, textAlign: "right", fontWeight: 600, color: colorFn ? colorFn(r._total) : r._total >= 0 ? C.green : C.red }}>{fmt(r._total)}</td>}
            </tr>
          ))}
        </tbody>
        <tfoot><tr style={{ background: C.bg }}>
          <td colSpan={rowFs.length} style={{ ...S.th, fontWeight: 700, color: C.text, borderTop: `2px solid ${C.border}` }}>Total</td>
          {hasCols ? colKeys.map(ck => {
            const t = rows.reduce((s, r) => s + (r[ck] || 0), 0);
            return <td key={ck} style={{ ...S.th, ...S.mono, textAlign: "right", fontWeight: 700, color: C.text, borderTop: `2px solid ${C.border}` }}>{fmt(t)}</td>;
          }) : <td style={{ ...S.th, ...S.mono, textAlign: "right", fontWeight: 700, color: C.text, borderTop: `2px solid ${C.border}` }}>{fmt(rows.reduce((s, r) => s + r._total, 0))}</td>}
          {hasCols && <td style={{ ...S.th, ...S.mono, textAlign: "right", fontWeight: 700, color: C.text, borderTop: `2px solid ${C.border}` }}>{fmt(rows.reduce((s, r) => s + r._total, 0))}</td>}
        </tr></tfoot>
      </table>
    </div>
  );
}

// ─── PIVOT CHART ────────────────────────────────────────────────
function PivotChartView({ data, rowFs, colF, valF, scenarioData }) {
  const chartData = useMemo(() => {
    if (!rowFs.length || !valF) return [];
    const { rows } = computePivot(data, rowFs, null, valF, "auto");
    const main = rows.slice(0, 25).map(r => ({ ...r, label: rowFs.map(f => r[f]).join(" | ") }));
    if (!scenarioData || !Object.keys(scenarioData).length) return main.map(r => ({ ...r, Actuals: r._total }));
    const scPivots = {};
    for (const [name, sd] of Object.entries(scenarioData)) {
      const { rows: sr } = computePivot(sd, rowFs, null, valF, "auto");
      const map = {}; for (const r of sr) map[r._key] = r._total;
      scPivots[name] = map;
    }
    return main.map(r => {
      const out = { ...r, Actuals: r._total };
      for (const [name, map] of Object.entries(scPivots)) out[name] = map[r._key] || 0;
      return out;
    });
  }, [data, rowFs, colF, valF, scenarioData]);
  if (!chartData.length) return null;
  const hasScen = scenarioData && Object.keys(scenarioData).length > 0;
  const labelF = rowFs[rowFs.length - 1];
  return (
    <ResponsiveContainer width="100%" height={280}>
      <BarChart data={chartData} margin={{ top: 8, right: 12, left: 12, bottom: 60 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
        <XAxis dataKey={labelF} tick={{ fill: C.textMuted, fontSize: 10 }} angle={-30} textAnchor="end" interval={0} height={65} />
        <YAxis tick={{ fill: C.textMuted, fontSize: 10 }} tickFormatter={fmtS} />
        <Tooltip contentStyle={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 11, boxShadow: "0 4px 12px rgba(0,0,0,0.08)" }} formatter={v => [fmt(v), ""]} />
        {hasScen ? (
          <>
            <Legend wrapperStyle={{ fontSize: 11 }} />
            <Bar dataKey="Actuals" fill={C.textMuted} radius={[4, 4, 0, 0]} opacity={0.35} />
            {Object.keys(scenarioData).map((name, i) => (
              <Bar key={name} dataKey={name} fill={SC_COLORS[i % SC_COLORS.length]} radius={[4, 4, 0, 0]} />
            ))}
          </>
        ) : (
          <Bar dataKey="Actuals" radius={[4, 4, 0, 0]}>
            {chartData.map((r, i) => <Cell key={i} fill={r.Actuals >= 0 ? C.green : C.red} opacity={0.75} />)}
          </Bar>
        )}
      </BarChart>
    </ResponsiveContainer>
  );
}

// ─── WATERFALL CHART ────────────────────────────────────────────
function WaterfallChart({ baseline, scenarioData, scenarioName, scenarioColor, rowFs, valF, waterfallField }) {
  const data = useMemo(() => {
    if (!waterfallField || !scenarioData) return [];
    // Group baseline and scenario by waterfallField
    const groupBy = (arr) => {
      const g = {};
      for (const r of arr) {
        const k = String(r[waterfallField] ?? "Other");
        g[k] = (g[k] || 0) + (r[valF] || 0);
      }
      return g;
    };
    const baseG = groupBy(baseline);
    const scG = groupBy(scenarioData);
    const allKeys = [...new Set([...Object.keys(baseG), ...Object.keys(scG)])];

    // Build waterfall items: only where there's a change
    const items = [];
    for (const k of allKeys) {
      const bv = baseG[k] || 0;
      const sv = scG[k] || 0;
      const delta = sv - bv;
      if (Math.abs(delta) > 0.01) items.push({ key: k, delta });
    }
    // Sort by absolute delta descending
    items.sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta));

    const baseTotal = baseline.reduce((s, r) => s + (r[valF] || 0), 0);
    const scTotal = scenarioData.reduce((s, r) => s + (r[valF] || 0), 0);

    // Build waterfall bars
    const bars = [];
    bars.push({ name: "Actuals", value: baseTotal, isTotal: true, delta: 0, bottom: 0 });
    let running = baseTotal;
    for (const item of items.slice(0, 15)) {
      const bottom = item.delta >= 0 ? running : running + item.delta;
      bars.push({ name: item.key.length > 20 ? item.key.slice(0, 18) + "…" : item.key, value: item.delta, isTotal: false, delta: item.delta, bottom });
      running += item.delta;
    }
    // If there are more items, aggregate as "Other"
    if (items.length > 15) {
      const rest = items.slice(15).reduce((s, i) => s + i.delta, 0);
      if (Math.abs(rest) > 0.01) {
        const bottom = rest >= 0 ? running : running + rest;
        bars.push({ name: "Other", value: rest, isTotal: false, delta: rest, bottom });
        running += rest;
      }
    }
    bars.push({ name: scenarioName, value: scTotal, isTotal: true, delta: 0, bottom: 0 });
    return bars;
  }, [baseline, scenarioData, waterfallField, valF, scenarioName]);

  if (!data.length) return null;

  return (
    <ResponsiveContainer width="100%" height={300}>
      <BarChart data={data} margin={{ top: 8, right: 12, left: 12, bottom: 80 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
        <XAxis dataKey="name" tick={{ fill: C.textMuted, fontSize: 10 }} angle={-35} textAnchor="end" interval={0} height={80} />
        <YAxis tick={{ fill: C.textMuted, fontSize: 10 }} tickFormatter={fmtS} />
        <Tooltip
          contentStyle={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 11, boxShadow: "0 4px 12px rgba(0,0,0,0.08)" }}
          formatter={(v, name) => {
            if (name === "bottom") return [null, null];
            return [fmt(v), ""];
          }}
        />
        <Bar dataKey="bottom" stackId="a" fill="transparent" />
        <Bar dataKey="value" stackId="a" radius={[3, 3, 0, 0]}>
          {data.map((d, i) => (
            <Cell key={i} fill={d.isTotal ? scenarioColor || C.brand : d.delta >= 0 ? C.green : C.red} opacity={d.isTotal ? 0.7 : 0.85} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}

// ─── COMPARISON TABLE ───────────────────────────────────────────
function ComparisonTable({ baseline, scenarioOutputs, rowFs, colF, valF, scenarios }) {
  const [sortCol, setSortCol] = useState(null);
  const [sortDir, setSortDir] = useState("asc");
  const hasCol = colF && colF.length > 0;

  // Compute column keys
  const colKeys = useMemo(() => {
    if (!hasCol) return [];
    const keys = new Set();
    baseline.forEach(r => { if (r[colF] != null) keys.add(r[colF]); });
    return [...keys].sort();
  }, [baseline, colF, hasCol]);

  // Build raw data with column-pivoted values
  const rawData = useMemo(() => {
    if (!hasCol) {
      // Simple mode: no column pivot
      const { rows: actRows } = computePivot(baseline, rowFs, null, valF, "none");
      const scPivots = {};
      for (const sc of scenarios) {
        const { rows: sr } = computePivot(scenarioOutputs[sc.name] || [], rowFs, null, valF, "none");
        const map = {}; for (const r of sr) map[r._key] = r._total;
        scPivots[sc.name] = { map };
      }
      return actRows.map(r => {
        const out = { ...r };
        for (const sc of scenarios) {
          out["sc_" + sc.name] = scPivots[sc.name]?.map[r._key] || 0;
          out["var_" + sc.name] = (scPivots[sc.name]?.map[r._key] || 0) - r._total;
        }
        return out;
      });
    }
    // Column pivot mode: group by rowFs, then for each colKey compute actuals + scenarios
    const groupData = (data) => {
      const groups = {};
      for (const r of data) {
        const rk = rowFs.map(f => r[f] ?? "—").join(" | ");
        const ck = r[colF] ?? "—";
        if (!groups[rk]) { groups[rk] = { _key: rk }; rowFs.forEach(f => groups[rk][f] = r[f] ?? "—"); }
        groups[rk]["col_" + ck] = (groups[rk]["col_" + ck] || 0) + (r[valF] || 0);
        groups[rk]._total = (groups[rk]._total || 0) + (r[valF] || 0);
      }
      return groups;
    };
    const actG = groupData(baseline);
    const scGroups = {};
    for (const sc of scenarios) scGroups[sc.name] = groupData(scenarioOutputs[sc.name] || []);

    return Object.values(actG).map(r => {
      const out = { ...r };
      // For each colKey and scenario, add values
      for (const ck of colKeys) {
        out["act_" + ck] = r["col_" + ck] || 0;
        for (const sc of scenarios) {
          const sr = scGroups[sc.name]?.[r._key];
          out["sc_" + sc.name + "_" + ck] = sr?.["col_" + ck] || 0;
          out["var_" + sc.name + "_" + ck] = (sr?.["col_" + ck] || 0) - (r["col_" + ck] || 0);
        }
      }
      // Totals per scenario
      for (const sc of scenarios) {
        const sr = scGroups[sc.name]?.[r._key];
        out["sc_" + sc.name] = sr?._total || 0;
        out["var_" + sc.name] = (sr?._total || 0) - (r._total || 0);
      }
      return out;
    });
  }, [baseline, scenarioOutputs, rowFs, colF, valF, scenarios, hasCol, colKeys]);

  const data = useMemo(() => {
    const arr = [...rawData];
    if (!sortCol) {
      const f0 = rowFs[0];
      const isTime = f0 && (f0.includes("period") || f0.includes("_year") || f0.includes("_month") || f0 === "period");
      if (isTime) arr.sort((a, b) => String(a[f0] ?? "").localeCompare(String(b[f0] ?? "")));
      else arr.sort((a, b) => (b._total || 0) - (a._total || 0));
      return arr;
    }
    arr.sort((a, b) => {
      const av = a[sortCol] ?? 0; const bv = b[sortCol] ?? 0;
      if (typeof av === "number" && typeof bv === "number") return sortDir === "asc" ? av - bv : bv - av;
      return sortDir === "asc" ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
    });
    return arr;
  }, [rawData, sortCol, sortDir, rowFs]);

  function toggleSort(col) {
    if (sortCol === col) setSortDir(d => d === "asc" ? "desc" : "asc");
    else { setSortCol(col); setSortDir(typeof (rawData[0]?.[col]) === "number" ? "desc" : "asc"); }
  }
  const arrow = col => sortCol === col ? (sortDir === "asc" ? " ↑" : " ↓") : "";
  const thClick = (col, align = "left", extra = {}) => ({
    ...S.th, textAlign: align, cursor: "pointer", userSelect: "none",
    color: sortCol === col ? C.brand : S.th.color, ...extra,
  });

  if (!data.length) return null;

  // Column pivot mode
  if (hasCol && colKeys.length > 0) {
    return (
      <div style={{ maxHeight: 480, overflow: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            {/* Top header row: column field values as groups */}
            <tr>
              <th colSpan={rowFs.length} style={{ ...S.th, borderBottom: "none" }}></th>
              {colKeys.map(ck => (
                <th key={ck} colSpan={1 + scenarios.length * 2} style={{ ...S.th, textAlign: "center", borderBottom: "none", color: C.brand, fontSize: 11, fontWeight: 700 }}>{String(ck)}</th>
              ))}
              <th colSpan={1 + scenarios.length * 2} style={{ ...S.th, textAlign: "center", borderBottom: "none", color: C.text, fontWeight: 700, fontSize: 11 }}>Total</th>
            </tr>
            {/* Sub header row: Actuals, Scenarios, Deltas */}
            <tr>
              {rowFs.map(f => <th key={f} style={thClick(f)} onClick={() => toggleSort(f)}>{f.replace(/_/g, " ")}{arrow(f)}</th>)}
              {colKeys.map(ck => (
                <React.Fragment key={ck}>
                  <th style={thClick("act_" + ck, "right")} onClick={() => toggleSort("act_" + ck)}>Act{arrow("act_" + ck)}</th>
                  {scenarios.map(sc => (
                    <React.Fragment key={sc.id}>
                      <th style={thClick("sc_" + sc.name + "_" + ck, "right", { color: sortCol === "sc_" + sc.name + "_" + ck ? C.brand : sc.color })} onClick={() => toggleSort("sc_" + sc.name + "_" + ck)}>{sc.name.slice(0, 6)}{arrow("sc_" + sc.name + "_" + ck)}</th>
                      <th style={thClick("var_" + sc.name + "_" + ck, "right", { color: sortCol === "var_" + sc.name + "_" + ck ? C.brand : sc.color })} onClick={() => toggleSort("var_" + sc.name + "_" + ck)}>Δ{arrow("var_" + sc.name + "_" + ck)}</th>
                    </React.Fragment>
                  ))}
                </React.Fragment>
              ))}
              <th style={thClick("_total", "right")} onClick={() => toggleSort("_total")}>Act{arrow("_total")}</th>
              {scenarios.map(sc => (
                <React.Fragment key={sc.id}>
                  <th style={thClick("sc_" + sc.name, "right", { color: sortCol === "sc_" + sc.name ? C.brand : sc.color })} onClick={() => toggleSort("sc_" + sc.name)}>{sc.name.slice(0, 6)}{arrow("sc_" + sc.name)}</th>
                  <th style={thClick("var_" + sc.name, "right", { color: sortCol === "var_" + sc.name ? C.brand : sc.color })} onClick={() => toggleSort("var_" + sc.name)}>Δ{arrow("var_" + sc.name)}</th>
                </React.Fragment>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.slice(0, 80).map((r, i) => (
              <tr key={i} style={{ background: i % 2 === 0 ? "" : C.bg }}>
                {rowFs.map(f => <td key={f} style={S.td}>{String(r[f] ?? "—")}</td>)}
                {colKeys.map(ck => (
                  <React.Fragment key={ck}>
                    <td style={{ ...S.td, ...S.mono, textAlign: "right", color: (r["act_" + ck] || 0) >= 0 ? C.green : C.red }}>{fmt(r["act_" + ck])}</td>
                    {scenarios.map(sc => {
                      const sv = r["sc_" + sc.name + "_" + ck] || 0;
                      const dv = r["var_" + sc.name + "_" + ck] || 0;
                      return (
                        <React.Fragment key={sc.id}>
                          <td style={{ ...S.td, ...S.mono, textAlign: "right", color: sc.color }}>{fmt(sv)}</td>
                          <td style={{ ...S.td, ...S.mono, textAlign: "right", color: dv >= 0 ? C.green : C.red }}>{dv >= 0 ? "+" : ""}{fmt(dv)}</td>
                        </React.Fragment>
                      );
                    })}
                  </React.Fragment>
                ))}
                <td style={{ ...S.td, ...S.mono, textAlign: "right", fontWeight: 600, color: (r._total || 0) >= 0 ? C.green : C.red }}>{fmt(r._total)}</td>
                {scenarios.map(sc => {
                  const sv = r["sc_" + sc.name] || 0;
                  const dv = r["var_" + sc.name] || 0;
                  return (
                    <React.Fragment key={sc.id}>
                      <td style={{ ...S.td, ...S.mono, textAlign: "right", fontWeight: 600, color: sc.color }}>{fmt(sv)}</td>
                      <td style={{ ...S.td, ...S.mono, textAlign: "right", fontWeight: 600, color: dv >= 0 ? C.green : C.red }}>{dv >= 0 ? "+" : ""}{fmt(dv)}</td>
                    </React.Fragment>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }

  // Simple mode (no column pivot)
  return (
    <div style={{ maxHeight: 450, overflow: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead><tr>
          {rowFs.map(f => <th key={f} style={thClick(f)} onClick={() => toggleSort(f)}>{f.replace(/_/g, " ")}{arrow(f)}</th>)}
          <th style={thClick("_total", "right")} onClick={() => toggleSort("_total")}>Actuals{arrow("_total")}</th>
          {scenarios.map(sc => <th key={sc.id} style={thClick("sc_" + sc.name, "right", { color: sortCol === "sc_" + sc.name ? C.brand : sc.color })} onClick={() => toggleSort("sc_" + sc.name)}>{sc.name}{arrow("sc_" + sc.name)}</th>)}
          {scenarios.map(sc => <th key={"v" + sc.id} style={thClick("var_" + sc.name, "right", { color: sortCol === "var_" + sc.name ? C.brand : sc.color })} onClick={() => toggleSort("var_" + sc.name)}>Δ {sc.name}{arrow("var_" + sc.name)}</th>)}
        </tr></thead>
        <tbody>
          {data.slice(0, 120).map((r, i) => (
            <tr key={i} style={{ background: i % 2 === 0 ? "" : C.bg }}>
              {rowFs.map(f => <td key={f} style={S.td}>{String(r[f] ?? "—")}</td>)}
              <td style={{ ...S.td, ...S.mono, textAlign: "right", color: r._total >= 0 ? C.green : C.red }}>{fmt(r._total)}</td>
              {scenarios.map(sc => <td key={sc.id} style={{ ...S.td, ...S.mono, textAlign: "right", color: sc.color }}>{fmt(r["sc_" + sc.name])}</td>)}
              {scenarios.map(sc => {
                const v = r["var_" + sc.name];
                return <td key={"v" + sc.id} style={{ ...S.td, ...S.mono, textAlign: "right", color: v >= 0 ? C.green : C.red }}>{v >= 0 ? "+" : ""}{fmt(v)}</td>;
              })}
            </tr>
          ))}
        </tbody>
        <tfoot><tr style={{ background: C.bg }}>
          <td colSpan={rowFs.length} style={{ ...S.th, fontWeight: 700, color: C.text, borderTop: `2px solid ${C.border}` }}>Total</td>
          <td style={{ ...S.th, ...S.mono, textAlign: "right", fontWeight: 700, color: C.text, borderTop: `2px solid ${C.border}` }}>{fmt(data.reduce((s, r) => s + (r._total || 0), 0))}</td>
          {scenarios.map(sc => <td key={sc.id} style={{ ...S.th, ...S.mono, textAlign: "right", fontWeight: 700, color: sc.color, borderTop: `2px solid ${C.border}` }}>{fmt(data.reduce((s, r) => s + (r["sc_" + sc.name] || 0), 0))}</td>)}
          {scenarios.map(sc => {
            const v = data.reduce((s, r) => s + (r["var_" + sc.name] || 0), 0);
            return <td key={"v" + sc.id} style={{ ...S.th, ...S.mono, textAlign: "right", fontWeight: 700, color: v >= 0 ? C.green : C.red, borderTop: `2px solid ${C.border}` }}>{v >= 0 ? "+" : ""}{fmt(v)}</td>;
          })}
        </tr></tfoot>
      </table>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// SCHEMA VIEW (with editable roles + editable relationships)
// ═══════════════════════════════════════════════════════════════
function SchemaView({ tables, schema, setSchema, relationships, setRelationships }) {
  const [addRelOpen, setAddRelOpen] = useState(false);
  const [newRel, setNewRel] = useState({ from: "", to: "", fromCol: "", toCol: "" });
  const tableNames = Object.keys(schema);

  function changeRole(tn, cn, nr) {
    setSchema(p => {
      const n = { ...p };
      n[tn] = { ...n[tn], columns: n[tn].columns.map(c => c.name === cn ? { ...c, role: nr } : c) };
      n[tn].isFact = n[tn].columns.some(c => c.role === "measure") && n[tn].columns.filter(c => c.role === "key").length >= 2;
      return n;
    });
  }

  function addRelationship() {
    if (!newRel.from || !newRel.to || !newRel.fromCol || !newRel.toCol) return;
    const v1 = new Set(tables[newRel.from].data.map(r => r[newRel.fromCol]).filter(v => v != null).map(String));
    const v2 = new Set(tables[newRel.to].data.map(r => r[newRel.toCol]).filter(v => v != null).map(String));
    const ov = [...v1].filter(v => v2.has(v)).length;
    setRelationships(p => [...p, {
      id: `${newRel.from}-${newRel.to}-${newRel.fromCol}-${Date.now()}`,
      ...newRel,
      coverage: Math.round(ov / Math.min(v1.size || 1, v2.size || 1) * 100),
      overlapCount: ov
    }]);
    setNewRel({ from: "", to: "", fromCol: "", toCol: "" });
    setAddRelOpen(false);
  }

  function removeRel(id) { setRelationships(p => p.filter(r => r.id !== id)); }

  function updateRelCol(id, side, col) {
    setRelationships(p => p.map(r => {
      if (r.id !== id) return r;
      const updated = { ...r, [side]: col };
      const v1 = new Set(tables[updated.from].data.map(row => row[updated.fromCol]).filter(v => v != null).map(String));
      const v2 = new Set(tables[updated.to].data.map(row => row[updated.toCol]).filter(v => v != null).map(String));
      const ov = [...v1].filter(v => v2.has(v)).length;
      return { ...updated, coverage: Math.round(ov / Math.min(v1.size || 1, v2.size || 1) * 100), overlapCount: ov };
    }));
  }

  return (
    <div>
      <div style={{ marginBottom: 20 }}>
        <h2 style={{ fontSize: 20, fontWeight: 700, color: C.text, marginBottom: 4 }}>Data Model</h2>
        <p style={{ color: C.textSec, fontSize: 13 }}>Auto-discovered schema. Edit roles and relationships below.</p>
      </div>

      {/* Relationships */}
      <div style={S.card}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
          <div style={S.cardT}>Relationships</div>
          <button onClick={() => setAddRelOpen(!addRelOpen)} style={S.btn("primary", true)}>+ Add Relationship</button>
        </div>

        {addRelOpen && (
          <div style={{ background: C.bg, borderRadius: 8, padding: 14, border: `1px solid ${C.border}`, marginBottom: 12 }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr auto", gap: 8, alignItems: "end" }}>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>From Table</label>
                <select style={{ ...S.select, width: "100%" }} value={newRel.from} onChange={e => setNewRel(p => ({ ...p, from: e.target.value, fromCol: "" }))}>
                  <option value="">Select...</option>
                  {tableNames.map(t => <option key={t} value={t}>{t}</option>)}
                </select>
              </div>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>From Column</label>
                <select style={{ ...S.select, width: "100%" }} value={newRel.fromCol} onChange={e => setNewRel(p => ({ ...p, fromCol: e.target.value }))}>
                  <option value="">Select...</option>
                  {newRel.from && schema[newRel.from]?.columns.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
                </select>
              </div>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>To Table</label>
                <select style={{ ...S.select, width: "100%" }} value={newRel.to} onChange={e => setNewRel(p => ({ ...p, to: e.target.value, toCol: "" }))}>
                  <option value="">Select...</option>
                  {tableNames.filter(t => t !== newRel.from).map(t => <option key={t} value={t}>{t}</option>)}
                </select>
              </div>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>To Column</label>
                <select style={{ ...S.select, width: "100%" }} value={newRel.toCol} onChange={e => setNewRel(p => ({ ...p, toCol: e.target.value }))}>
                  <option value="">Select...</option>
                  {newRel.to && schema[newRel.to]?.columns.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
                </select>
              </div>
              <button onClick={addRelationship} style={S.btn("primary", true)}>Add</button>
            </div>
          </div>
        )}

        <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
          {relationships.map(rel => (
            <div key={rel.id} style={{ background: C.bg, borderRadius: 8, padding: "10px 14px", border: `1px solid ${C.border}`, display: "flex", alignItems: "center", gap: 10 }}>
              <span style={{ ...S.badge(C.brand), minWidth: 80, justifyContent: "center" }}>{rel.from}</span>
              <select style={{ ...S.select, fontSize: 11, padding: "3px 6px" }} value={rel.fromCol} onChange={e => updateRelCol(rel.id, "fromCol", e.target.value)}>
                {schema[rel.from]?.columns.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
              </select>
              <span style={{ color: C.textMuted, fontSize: 18, fontWeight: 300 }}>=</span>
              <span style={{ ...S.badge(C.purple), minWidth: 80, justifyContent: "center" }}>{rel.to}</span>
              <select style={{ ...S.select, fontSize: 11, padding: "3px 6px" }} value={rel.toCol} onChange={e => updateRelCol(rel.id, "toCol", e.target.value)}>
                {schema[rel.to]?.columns.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
              </select>
              <div style={{ flex: 1 }} />
              <span style={{ ...S.mono, fontSize: 10, color: rel.coverage > 80 ? C.green : rel.coverage > 50 ? C.amber : C.red }}>{rel.coverage}%</span>
              <span style={{ fontSize: 10, color: C.textMuted }}>{rel.overlapCount} matches</span>
              <span onClick={() => removeRel(rel.id)} style={{ cursor: "pointer", color: C.textMuted, fontSize: 14, padding: "2px 4px" }}>×</span>
            </div>
          ))}
        </div>
      </div>

      {/* Tables */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(300px, 1fr))", gap: 14 }}>
        {Object.entries(schema).map(([name, info]) => (
          <div key={name} style={{ ...S.card, borderColor: info.isFact ? C.brand + "44" : C.border }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
              <span style={{ fontSize: 15, fontWeight: 700, color: C.text }}>{name}</span>
              <span style={S.badge(info.isFact ? C.brand : C.purple)}>{info.isFact ? "FACT" : "DIMENSION"}</span>
            </div>
            <div style={{ fontSize: 12, color: C.textMuted, marginBottom: 10 }}>{info.rowCount} rows</div>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead><tr>
                <th style={S.th}>Column</th>
                <th style={S.th}>Role</th>
                <th style={S.th}>Unique</th>
              </tr></thead>
              <tbody>{info.columns.map(col => (
                <tr key={col.name}>
                  <td style={{ ...S.td, ...S.mono, fontSize: 11 }}>{col.name}</td>
                  <td style={S.td}>
                    <select value={col.role} onChange={e => changeRole(name, col.name, e.target.value)}
                      style={{ ...S.select, padding: "2px 8px", fontSize: 10, background: ROLE_COLORS[col.role] + "12", color: ROLE_COLORS[col.role], border: `1px solid ${ROLE_COLORS[col.role]}30`, borderRadius: 20, fontWeight: 600 }}>
                      {ROLE_OPTIONS.map(r => <option key={r} value={r}>{r.toUpperCase()}</option>)}
                    </select>
                  </td>
                  <td style={{ ...S.td, color: C.textMuted, fontSize: 11 }}>{col.uniqueCount}</td>
                </tr>
              ))}</tbody>
            </table>
          </div>
        ))}
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// ACTUALS VIEW
// ═══════════════════════════════════════════════════════════════
function ActualsView({ baseline }) {
  const dims = useMemo(() => getDimFields(baseline), [baseline]);
  const measures = useMemo(() => getMeasureFields(baseline), [baseline]);
  const [rowFs, setRowFs] = useState(["_period"]);
  const [colF, setColF] = useState("");
  const [valF, setValF] = useState("amount");
  const [filters, setFilters] = useState({});
  const filtered = useMemo(() => applyFilters(baseline, filters), [baseline, filters]);

  return (
    <div>
      <div style={{ marginBottom: 20 }}>
        <h2 style={{ fontSize: 20, fontWeight: 700, color: C.text, marginBottom: 4 }}>Actuals</h2>
        <p style={{ color: C.textSec, fontSize: 13 }}>{filtered.length} entries after filters</p>
      </div>
      <div style={S.card}>
        <div style={{ display: "grid", gridTemplateColumns: "2fr 2fr 1fr", gap: 14, marginBottom: 10 }}>
          <FieldManager label="Row Fields" allFields={dims} selected={rowFs} onChange={setRowFs} color={C.brand} />
          <FieldManager label="Column Field" allFields={dims.filter(f => !rowFs.includes(f))} selected={colF} onChange={setColF} color={C.purple} single />
          <FieldManager label="Value" allFields={measures} selected={valF} onChange={setValF} color={C.green} single />
        </div>
        <FilterManager baseline={baseline} allFields={dims} filters={filters} setFilters={setFilters} />
      </div>
      <div style={S.card}>
        <div style={S.cardT}>Pivot Table</div>
        <PivotTableView data={filtered} rowFs={rowFs} colF={colF} valF={valF} />
      </div>
      <div style={S.card}>
        <div style={S.cardT}>Chart</div>
        <PivotChartView data={filtered} rowFs={rowFs} colF={colF} valF={valF} />
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// SCENARIOS VIEW
// ═══════════════════════════════════════════════════════════════
// ─── RULE FILTER HELPERS (for inline rule editing) ──────────────
function RuleFilterTag({ dim, activeVals, baseline, onChange, onRemove }) {
  const [expanded, setExpanded] = useState(false);
  const [search, setSearch] = useState("");
  const ref = useRef(null);
  useEffect(() => {
    const h = e => { if (ref.current && !ref.current.contains(e.target)) setExpanded(false); };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, []);
  const allVals = getUniq(baseline, dim);
  const filtered = search ? allVals.filter(v => String(v).toLowerCase().includes(search.toLowerCase())) : allVals;
  return (
    <div ref={ref} style={{ position: "relative" }}>
      <span onClick={() => { setExpanded(!expanded); setSearch(""); }}
        style={{ ...S.tag(activeVals.length ? C.amber : C.textMuted) }}>
        {dim.replace(/_/g, " ")}{activeVals.length ? ` (${activeVals.length})` : ""}
        <span onClick={e => { e.stopPropagation(); onRemove(); }}
          style={{ cursor: "pointer", opacity: .5, fontSize: 13 }}>×</span>
      </span>
      {expanded && (
        <div style={S.dropdown}>
          <input autoFocus style={{ ...S.input, marginBottom: 6, fontSize: 11 }}
            placeholder="Search values..." value={search}
            onChange={e => setSearch(e.target.value)} />
          {filtered.slice(0, 80).map(v => {
            const ch = activeVals.includes(v);
            return (
              <label key={String(v)} style={{ display: "flex", alignItems: "center", gap: 6, padding: "4px 4px", fontSize: 12, color: C.text, cursor: "pointer" }}>
                <input type="checkbox" checked={ch} style={{ accentColor: C.brand }}
                  onChange={() => onChange(ch ? activeVals.filter(x => x !== v) : [...activeVals, v])} />
                {String(v)}
              </label>
            );
          })}
        </div>
      )}
    </div>
  );
}

function RuleFilterAdd({ dims, existingFilters, onAdd }) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const ref = useRef(null);
  useEffect(() => {
    const h = e => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, []);
  const available = dims.filter(f => !existingFilters.includes(f))
    .filter(f => !search || f.toLowerCase().includes(search.toLowerCase()));
  return (
    <div ref={ref} style={{ position: "relative" }}>
      <button onClick={() => { setOpen(!open); setSearch(""); }}
        style={{ ...S.btn("ghost", true), padding: "4px 8px", fontSize: 11, color: C.amber, border: `1px dashed ${C.amber}44` }}>+ filter</button>
      {open && (
        <div style={S.dropdown}>
          <input autoFocus style={{ ...S.input, marginBottom: 6, fontSize: 11 }}
            placeholder="Search fields..." value={search}
            onChange={e => setSearch(e.target.value)} />
          {available.map(f => (
            <div key={f} onClick={() => { onAdd(f); setOpen(false); }}
              style={{ padding: "6px 8px", borderRadius: 6, cursor: "pointer", fontSize: 12, color: C.text }}
              onMouseEnter={e => e.target.style.background = C.surfaceHover}
              onMouseLeave={e => e.target.style.background = ""}>
              {f.replace(/_/g, " ")}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function ScenariosView({ baseline, scenarios, setScenarios }) {
  const dims = useMemo(() => getDimFields(baseline), [baseline]);
  const measures = useMemo(() => getMeasureFields(baseline), [baseline]);
  const periods = useMemo(() => getUniq(baseline, "_period"), [baseline]);

  const [active, setActive] = useState(new Set());
  const [editId, setEditId] = useState(null);
  const [rowFs, setRowFs] = useState(["_period"]);
  const [colF, setColF] = useState("");
  const [valF, setValF] = useState("amount");
  const [filters, setFilters] = useState({});
  const [newRule, setNewRule] = useState({ name: "", type: "multiplier", factor: 1.05, offset: 0, filters: {}, periodFrom: "", periodTo: "" });
  const [ruleFilterFields, setRuleFilterFields] = useState([]);
  const [ruleFilterSearch, setRuleFilterSearch] = useState("");
  const [ruleFilterOpen, setRuleFilterOpen] = useState(false);
  const [ruleFilterExpanded, setRuleFilterExpanded] = useState(null);
  const [ruleValSearch, setRuleValSearch] = useState("");
  const ruleFilterRef = useRef(null);
  const [waterfallField, setWaterfallField] = useState("");

  const toggle = id => setActive(p => { const n = new Set(p); n.has(id) ? n.delete(id) : n.add(id); return n; });
  const filtered = useMemo(() => applyFilters(baseline, filters), [baseline, filters]);
  const scOutputs = useMemo(() => {
    const o = {};
    for (const sc of scenarios) if (active.has(sc.id)) o[sc.name] = applyFilters(applyRules(baseline, sc.rules), filters);
    return o;
  }, [scenarios, active, baseline, filters]);
  const editSc = scenarios.find(s => s.id === editId);

  function addScenario() {
    const id = Date.now();
    setScenarios(p => [...p, { id, name: `Scenario ${p.length + 1}`, rules: [], color: SC_COLORS[p.length % SC_COLORS.length] }]);
    setEditId(id); setActive(p => new Set([...p, id]));
  }
  function delScenario(id) { setScenarios(p => p.filter(s => s.id !== id)); setActive(p => { const n = new Set(p); n.delete(id); return n; }); if (editId === id) setEditId(null); }
  function renameScenario(id, newName) { if (newName.trim()) setScenarios(p => p.map(s => s.id === id ? { ...s, name: newName.trim() } : s)); }
  function addRule() {
    if (!editId || !newRule.name) return;
    setScenarios(p => p.map(s => s.id !== editId ? s : { ...s, rules: [...s.rules, { ...newRule, id: Date.now() }] }));
    setNewRule({ name: "", type: "multiplier", factor: 1.05, offset: 0, filters: {}, periodFrom: "", periodTo: "" });
    setRuleFilterFields([]);
  }
  function rmRule(rid) { setScenarios(p => p.map(s => s.id !== editId ? s : { ...s, rules: s.rules.filter(r => r.id !== rid) })); }
  function updateRule(rid, updates) { setScenarios(p => p.map(s => s.id !== editId ? s : { ...s, rules: s.rules.map(r => r.id === rid ? { ...r, ...updates } : r) })); }
  const [editingRuleId, setEditingRuleId] = useState(null);

  const variance = useMemo(() => {
    if (!active.size) return [];
    const at = filtered.reduce((s, r) => s + (r[valF] || 0), 0);
    return scenarios.filter(sc => active.has(sc.id)).map(sc => {
      const sd = scOutputs[sc.name] || [];
      const st = sd.reduce((s, r) => s + (r[valF] || 0), 0);
      return { name: sc.name, color: sc.color, total: st, variance: st - at, pct: at ? ((st - at) / Math.abs(at)) * 100 : 0 };
    });
  }, [active, scenarios, scOutputs, filtered, valF]);

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div>
          <h2 style={{ fontSize: 20, fontWeight: 700, color: C.text, marginBottom: 4 }}>Scenarios</h2>
          <p style={{ color: C.textSec, fontSize: 13 }}>{active.size} active · {scenarios.length} total</p>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <button style={S.btn("primary")} onClick={addScenario}>+ New Scenario</button>
        </div>
      </div>

      {scenarios.length > 0 && (
        <div style={{ display: "flex", gap: 8, marginBottom: 16, flexWrap: "wrap" }}>
          {scenarios.map(sc => (
            <div key={sc.id} style={{ display: "flex", alignItems: "center", gap: 2 }}>
              <button onClick={() => toggle(sc.id)}
                style={{ display: "inline-flex", alignItems: "center", gap: 8, padding: "10px 20px", borderRadius: 10, cursor: "pointer", fontSize: 14, fontWeight: 600, fontFamily: "'Plus Jakarta Sans', sans-serif", background: active.has(sc.id) ? sc.color + "15" : C.white, border: `2px solid ${active.has(sc.id) ? sc.color : C.border}`, color: active.has(sc.id) ? sc.color : C.textMuted, transition: "all .15s" }}>
                <span style={{ width: 10, height: 10, borderRadius: "50%", background: sc.color, flexShrink: 0 }} />
                {sc.name}
                <span style={{ fontSize: 11, opacity: 0.6 }}>({sc.rules.length})</span>
              </button>
              <span onClick={() => setEditId(editId === sc.id ? null : sc.id)} style={{ padding: "6px 8px", cursor: "pointer", color: editId === sc.id ? C.brand : C.textMuted, fontSize: 15 }}>✎</span>
              <span onClick={() => delScenario(sc.id)} style={{ padding: "6px 6px", cursor: "pointer", color: C.textMuted, fontSize: 15 }}>×</span>
            </div>
          ))}
        </div>
      )}

      <div style={S.card}>
        <div style={{ display: "grid", gridTemplateColumns: "2fr 2fr 1fr", gap: 14, marginBottom: 10 }}>
          <FieldManager label="Row Fields" allFields={dims} selected={rowFs} onChange={setRowFs} color={C.brand} />
          <FieldManager label="Column Field" allFields={dims.filter(f => !rowFs.includes(f))} selected={colF} onChange={setColF} color={C.purple} single />
          <FieldManager label="Value" allFields={measures} selected={valF} onChange={setValF} color={C.green} single />
        </div>
        <FilterManager baseline={baseline} allFields={dims} filters={filters} setFilters={setFilters} />
      </div>

      {editSc && (
        <div style={{ ...S.card, borderColor: editSc.color + "44", borderWidth: 2 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 12 }}>
            <span style={{ fontSize: 11, fontWeight: 700, color: editSc.color, textTransform: "uppercase", letterSpacing: "0.6px" }}>Editing:</span>
            <input
              value={editSc.name}
              onChange={e => renameScenario(editSc.id, e.target.value)}
              style={{ ...S.input, fontSize: 14, fontWeight: 700, color: editSc.color, border: `1px solid ${editSc.color}33`, background: editSc.color + "08", padding: "4px 10px", borderRadius: 6, width: "auto", minWidth: 120, maxWidth: 300 }}
            />
          </div>
          {editSc.rules.map(rule => {
            const isEditing = editingRuleId === rule.id;
            return (
              <div key={rule.id} style={{ background: C.bg, borderRadius: 8, border: `1px solid ${isEditing ? editSc.color + "44" : C.border}`, marginBottom: 4, overflow: "hidden" }}>
                {/* Collapsed summary row */}
                <div style={{ padding: "8px 12px", display: "flex", justifyContent: "space-between", alignItems: "center", cursor: "pointer" }}
                  onClick={() => setEditingRuleId(isEditing ? null : rule.id)}>
                  <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: 6 }}>
                    <span style={{ fontSize: 12, color: isEditing ? editSc.color : C.textMuted }}>{isEditing ? "▾" : "▸"}</span>
                    <span style={{ fontWeight: 600, fontSize: 12 }}>{rule.name}</span>
                    <span style={S.badge(rule.type === "multiplier" ? C.brand : C.amber)}>{rule.type === "multiplier" ? `×${rule.factor}` : `+${fmt(rule.offset)}`}</span>
                    {rule.periodFrom && <span style={{ fontSize: 10, color: C.textMuted }}>{rule.periodFrom} → {rule.periodTo || "∞"}</span>}
                    {Object.entries(rule.filters || {}).filter(([, v]) => v && (!Array.isArray(v) || v.length > 0)).map(([k, v]) =>
                      <span key={k} style={S.badge(C.purple)}>{k}: {Array.isArray(v) ? (v.length > 2 ? v.slice(0, 2).join(", ") + ` +${v.length - 2}` : v.join(", ")) : v}</span>
                    )}
                  </div>
                  <button onClick={e => { e.stopPropagation(); rmRule(rule.id); }} style={{ ...S.btn("danger", true), borderRadius: 6 }}>×</button>
                </div>
                {/* Expanded editor */}
                {isEditing && (
                  <div style={{ padding: "8px 12px 12px", borderTop: `1px solid ${C.border}` }}>
                    <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr 1fr", gap: 8 }}>
                      <div>
                        <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Name</label>
                        <input style={S.input} value={rule.name} onChange={e => updateRule(rule.id, { name: e.target.value })} />
                      </div>
                      <div>
                        <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Type</label>
                        <select style={{ ...S.select, width: "100%" }} value={rule.type} onChange={e => updateRule(rule.id, { type: e.target.value })}>
                          <option value="multiplier">Multiplier (×)</option>
                          <option value="offset">Offset (+/-)</option>
                        </select>
                      </div>
                      <div>
                        <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>{rule.type === "multiplier" ? "Factor" : "Total Offset"}</label>
                        {rule.type === "multiplier"
                          ? <input style={S.input} type="number" step="0.01" value={rule.factor} onChange={e => updateRule(rule.id, { factor: parseFloat(e.target.value) || 1 })} />
                          : <input style={S.input} type="number" step="1000" value={rule.offset} onChange={e => updateRule(rule.id, { offset: parseFloat(e.target.value) || 0 })} />}
                      </div>
                    </div>
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginTop: 8 }}>
                      <div>
                        <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Period From</label>
                        <select style={{ ...S.select, width: "100%" }} value={rule.periodFrom || ""} onChange={e => updateRule(rule.id, { periodFrom: e.target.value })}>
                          <option value="">All</option>
                          {periods.map(p => <option key={p} value={p}>{p}</option>)}
                        </select>
                      </div>
                      <div>
                        <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Period To</label>
                        <select style={{ ...S.select, width: "100%" }} value={rule.periodTo || ""} onChange={e => updateRule(rule.id, { periodTo: e.target.value })}>
                          <option value="">All</option>
                          {periods.map(p => <option key={p} value={p}>{p}</option>)}
                        </select>
                      </div>
                    </div>
                    {/* Inline filter editor for existing rule */}
                    <div style={{ marginTop: 8 }}>
                      <div style={{ fontSize: 10, color: C.textMuted, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 4, fontWeight: 600 }}>Filters</div>
                      <div style={{ display: "flex", flexWrap: "wrap", gap: 4, alignItems: "center" }}>
                        {Object.keys(rule.filters || {}).map(dim => {
                          const vals = rule.filters[dim] || [];
                          const activeVals = Array.isArray(vals) ? vals : (vals ? [vals] : []);
                          return (
                            <RuleFilterTag key={dim} dim={dim} activeVals={activeVals} baseline={baseline}
                              onChange={nv => updateRule(rule.id, { filters: { ...rule.filters, [dim]: nv } })}
                              onRemove={() => { const nf = { ...rule.filters }; delete nf[dim]; updateRule(rule.id, { filters: nf }); }}
                            />
                          );
                        })}
                        <RuleFilterAdd dims={dims} existingFilters={Object.keys(rule.filters || {})}
                          onAdd={dim => updateRule(rule.id, { filters: { ...rule.filters, [dim]: [] } })}
                        />
                      </div>
                    </div>
                  </div>
                )}
              </div>
            );
          })}

          <div style={{ background: C.bg, borderRadius: 10, padding: 16, border: `1px solid ${C.border}`, marginTop: 8 }}>
            <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr 1fr", gap: 10 }}>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Rule Name</label>
                <input style={S.input} value={newRule.name} onChange={e => setNewRule(p => ({ ...p, name: e.target.value }))} placeholder="e.g. Revenue +5%" />
              </div>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Type</label>
                <select style={{ ...S.select, width: "100%" }} value={newRule.type} onChange={e => setNewRule(p => ({ ...p, type: e.target.value }))}>
                  <option value="multiplier">Multiplier (×)</option>
                  <option value="offset">Offset (+/-)</option>
                </select>
              </div>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>{newRule.type === "multiplier" ? "Factor" : "Total Offset"}</label>
                {newRule.type === "multiplier"
                  ? <input style={S.input} type="number" step="0.01" value={newRule.factor} onChange={e => setNewRule(p => ({ ...p, factor: parseFloat(e.target.value) || 1 }))} />
                  : <input style={S.input} type="number" step="1000" value={newRule.offset} onChange={e => setNewRule(p => ({ ...p, offset: parseFloat(e.target.value) || 0 }))} />}
              </div>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginTop: 10 }}>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Period From</label>
                <select style={{ ...S.select, width: "100%" }} value={newRule.periodFrom} onChange={e => setNewRule(p => ({ ...p, periodFrom: e.target.value }))}>
                  <option value="">All</option>
                  {periods.map(p => <option key={p} value={p}>{p}</option>)}
                </select>
              </div>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Period To</label>
                <select style={{ ...S.select, width: "100%" }} value={newRule.periodTo} onChange={e => setNewRule(p => ({ ...p, periodTo: e.target.value }))}>
                  <option value="">All</option>
                  {periods.map(p => <option key={p} value={p}>{p}</option>)}
                </select>
              </div>
            </div>

            {/* Rule filters */}
            <div style={{ marginTop: 10 }} ref={ruleFilterRef}>
              <div style={{ fontSize: 10, color: C.textMuted, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 5, fontWeight: 600 }}>Rule Filters</div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 4, alignItems: "center" }}>
                {ruleFilterFields.map(dim => {
                  const vals = newRule.filters[dim] || [];
                  const isArr = Array.isArray(vals);
                  const activeVals = isArr ? vals : (vals ? [vals] : []);
                  const expanded = ruleFilterExpanded === dim;
                  const dimVals = getUniq(baseline, dim);
                  return (
                    <div key={dim} style={{ position: "relative" }}>
                      <span onClick={() => { setRuleFilterExpanded(expanded ? null : dim); setRuleValSearch(""); }}
                        style={{ ...S.tag(activeVals.length ? C.amber : C.textMuted) }}>
                        {dim.replace(/_/g, " ")}{activeVals.length ? ` (${activeVals.length})` : ""}
                        <span onClick={e => { e.stopPropagation(); setRuleFilterFields(p => p.filter(f => f !== dim)); setNewRule(p => { const nf = { ...p.filters }; delete nf[dim]; return { ...p, filters: nf }; }); setRuleFilterExpanded(null); }}
                          style={{ cursor: "pointer", opacity: .5, fontSize: 13 }}>×</span>
                      </span>
                      {expanded && (
                        <div style={S.dropdown}>
                          <input autoFocus style={{ ...S.input, marginBottom: 6, fontSize: 11 }} placeholder="Search values..." value={ruleValSearch} onChange={e => setRuleValSearch(e.target.value)} />
                          {dimVals.filter(v => !ruleValSearch || String(v).toLowerCase().includes(ruleValSearch.toLowerCase())).slice(0, 80).map(v => {
                            const ch = activeVals.includes(v);
                            return (
                              <label key={String(v)} style={{ display: "flex", alignItems: "center", gap: 6, padding: "4px 4px", fontSize: 12, color: C.text, cursor: "pointer" }}>
                                <input type="checkbox" checked={ch} style={{ accentColor: C.brand }} onChange={() => {
                                  const nv = ch ? activeVals.filter(x => x !== v) : [...activeVals, v];
                                  setNewRule(p => ({ ...p, filters: { ...p.filters, [dim]: nv } }));
                                }} />
                                {String(v)}
                              </label>
                            );
                          })}
                        </div>
                      )}
                    </div>
                  );
                })}
                <div style={{ position: "relative" }}>
                  <button onClick={() => { setRuleFilterOpen(!ruleFilterOpen); setRuleFilterSearch(""); }}
                    style={{ ...S.btn("ghost", true), padding: "4px 8px", fontSize: 11, color: C.amber, border: `1px dashed ${C.amber}44` }}>+ filter</button>
                  {ruleFilterOpen && (
                    <div style={S.dropdown}>
                      <input autoFocus style={{ ...S.input, marginBottom: 6, fontSize: 11 }} placeholder="Search fields..." value={ruleFilterSearch} onChange={e => setRuleFilterSearch(e.target.value)} />
                      {dims.filter(f => !ruleFilterFields.includes(f)).filter(f => !ruleFilterSearch || f.toLowerCase().includes(ruleFilterSearch.toLowerCase())).map(f => (
                        <div key={f} onClick={() => { setRuleFilterFields(p => [...p, f]); setRuleFilterOpen(false); }}
                          style={{ padding: "6px 8px", borderRadius: 6, cursor: "pointer", fontSize: 12, color: C.text }}
                          onMouseEnter={e => e.target.style.background = C.surfaceHover}
                          onMouseLeave={e => e.target.style.background = ""}>
                          {f.replace(/_/g, " ")}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            </div>

            <button style={{ ...S.btn("primary"), marginTop: 12 }} onClick={addRule} disabled={!newRule.name}>Add Rule</button>
          </div>
        </div>
      )}

      {active.size > 0 && rowFs.length > 0 && (
        <div style={S.card}>
          <div style={S.cardT}>Comparison Chart</div>
          <PivotChartView data={filtered} rowFs={rowFs} colF={colF} valF={valF} scenarioData={scOutputs} />
        </div>
      )}

      {variance.length > 0 && (
        <div style={S.card}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
            <div style={S.cardT}>Waterfall Analysis</div>
            <FieldManager label="" allFields={dims} selected={waterfallField} onChange={setWaterfallField} color={C.purple} single />
          </div>
          {waterfallField ? (
            <div style={{ display: "grid", gridTemplateColumns: `repeat(${Math.min(scenarios.filter(sc => active.has(sc.id)).length, 2)}, 1fr)`, gap: 14 }}>
              {scenarios.filter(sc => active.has(sc.id)).map(sc => (
                <div key={sc.id}>
                  <div style={{ fontSize: 12, fontWeight: 600, color: sc.color, marginBottom: 6, textAlign: "center" }}>{sc.name}</div>
                  <WaterfallChart baseline={filtered} scenarioData={scOutputs[sc.name]} scenarioName={sc.name} scenarioColor={sc.color} rowFs={rowFs} valF={valF} waterfallField={waterfallField} />
                </div>
              ))}
            </div>
          ) : (
            <div style={{ textAlign: "center", padding: 24, color: C.textMuted, fontSize: 12 }}>Select a field above to break down changes by dimension.</div>
          )}
        </div>
      )}

      {variance.length > 0 && (
        <div style={S.card}>
          <div style={S.cardT}>Variance Summary</div>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead><tr>
              <th style={S.th}>Scenario</th>
              <th style={{ ...S.th, textAlign: "right" }}>Total</th>
              <th style={{ ...S.th, textAlign: "right" }}>Δ Actuals</th>
              <th style={{ ...S.th, textAlign: "right" }}>Δ %</th>
            </tr></thead>
            <tbody>
              <tr><td style={S.td}><span style={{ color: C.textSec }}>● Actuals</span></td>
                <td style={{ ...S.td, ...S.mono, textAlign: "right" }}>{fmt(filtered.reduce((s, r) => s + (r[valF] || 0), 0))}</td>
                <td style={{ ...S.td, textAlign: "right" }}>—</td><td style={{ ...S.td, textAlign: "right" }}>—</td></tr>
              {variance.map(v => (
                <tr key={v.name}>
                  <td style={S.td}><span style={{ color: v.color, fontWeight: 600 }}>● {v.name}</span></td>
                  <td style={{ ...S.td, ...S.mono, textAlign: "right" }}>{fmt(v.total)}</td>
                  <td style={{ ...S.td, ...S.mono, textAlign: "right", color: v.variance >= 0 ? C.green : C.red }}>{v.variance >= 0 ? "+" : ""}{fmt(v.variance)}</td>
                  <td style={{ ...S.td, textAlign: "right", color: v.pct >= 0 ? C.green : C.red, fontWeight: 600 }}>{v.pct >= 0 ? "+" : ""}{v.pct.toFixed(1)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {active.size > 0 && rowFs.length > 0 && valF && (
        <div style={S.card}>
          <div style={S.cardT}>Comparison Table</div>
          <ComparisonTable baseline={filtered} scenarioOutputs={scOutputs} rowFs={rowFs} colF={colF} valF={valF} scenarios={scenarios.filter(sc => active.has(sc.id))} />
        </div>
      )}

      {scenarios.length === 0 && (
        <div style={{ ...S.card, textAlign: "center", padding: 48 }}>
          <div style={{ fontSize: 36, marginBottom: 10 }}>📊</div>
          <div style={{ fontSize: 17, fontWeight: 700, color: C.text, marginBottom: 6 }}>No scenarios yet</div>
          <p style={{ color: C.textMuted, fontSize: 13, marginBottom: 16 }}>Create a scenario to model "what-if" plans against your actuals.</p>
          <button style={S.btn("primary")} onClick={addScenario}>Create First Scenario</button>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// CHAT PANEL
// ═══════════════════════════════════════════════════════════════
function ChatPanel({ baseline, scenarios, setScenarios, setActiveTab }) {
  const dims = useMemo(() => getDimFields(baseline), [baseline]);
  const [messages, setMessages] = useState([
    { role: "assistant", content: "Data loaded — **469 GL entries** for Stadelmann GmbH (EUR).\n\nTry:\n• \"What if Warenaufwand increases 10% in 2025?\"\n• \"Model 20% revenue drop for Fittings\"\n• \"Add 100k to Personalaufwand\"" }
  ]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const endRef = useRef(null);
  useEffect(() => { endRef.current?.scrollIntoView({ behavior: "smooth" }); }, [messages]);
  const ctx = useMemo(() => {
    const types = [...new Set(baseline.map(r => r["Hauptkontotyp"]).filter(Boolean))];
    const h2 = [...new Set(baseline.map(r => r["Reporting H2"]).filter(Boolean))];
    const periods = [...new Set(baseline.map(r => r._period).filter(Boolean))].sort();
    return { types, h2, periods, dims: dims.slice(0, 15) };
  }, [baseline, dims]);

  async function send() {
    if (!input.trim() || loading) return;
    const msg = input.trim(); setInput(""); setLoading(true);
    setMessages(p => [...p, { role: "user", content: msg }]);
    try {
      const sys = `You are a CFO assistant. Data: Stadelmann GmbH (EUR), periods: ${ctx.periods.join(",")}, account types: ${ctx.types.join(",")}, H2: ${ctx.h2.join(",")}, dims: ${ctx.dims.join(",")}, scenarios: ${scenarios.map(s => s.name).join(",") || "none"}.
To create a scenario include: <SCENARIO_ACTION>{"action":"create_scenario","name":"Name","rules":[{"name":"rule","type":"multiplier|offset","factor":1.05,"offset":100000,"filters":{"Reporting H2":"Personalaufwand"},"periodFrom":"2025-01","periodTo":"2025-12"}]}</SCENARIO_ACTION>
For offset type: offset is the TOTAL amount to add/subtract. For multiplier: factor is the multiplier. Be concise.`;
      const res = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ model: "claude-sonnet-4-20250514", max_tokens: 1000, system: sys, messages: messages.concat([{ role: "user", content: msg }]).filter(m => m.role !== "system").map(m => ({ role: m.role, content: m.content })) })
      });
      const data = await res.json();
      const text = data.content?.map(c => c.text || "").join("") || "Could you rephrase?";
      const am = text.match(/<SCENARIO_ACTION>([\s\S]*?)<\/SCENARIO_ACTION>/);
      if (am) { try { const a = JSON.parse(am[1]); if (a.action === "create_scenario") { const id = Date.now(); setScenarios(p => [...p, { id, name: a.name, rules: (a.rules || []).map((r, i) => ({ ...r, id: Date.now() + i })), color: SC_COLORS[p.length % SC_COLORS.length] }]); setActiveTab("scenarios"); } } catch { } }
      setMessages(p => [...p, { role: "assistant", content: text.replace(/<SCENARIO_ACTION>[\s\S]*?<\/SCENARIO_ACTION>/g, "").trim() || "Done! Check the Scenarios tab." }]);
    } catch { setMessages(p => [...p, { role: "assistant", content: "Connection issue. Build scenarios manually in the Scenarios tab." }]); }
    setLoading(false);
  }

  return (
    <div style={{ width: 340, borderLeft: `1px solid ${C.border}`, display: "flex", flexDirection: "column", background: C.white, flexShrink: 0 }}>
      <div style={{ padding: "14px 16px", borderBottom: `1px solid ${C.border}`, fontSize: 13, fontWeight: 700, color: C.brand, display: "flex", alignItems: "center", gap: 6 }}>
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><circle cx="8" cy="8" r="7" stroke={C.brand} strokeWidth="2" /><path d="M5 8h6M8 5v6" stroke={C.brand} strokeWidth="1.5" strokeLinecap="round" /></svg>
        AI Assistant
      </div>
      <div style={{ flex: 1, overflow: "auto", padding: 12, display: "flex", flexDirection: "column", gap: 8 }}>
        {messages.map((m, i) => (
          <div key={i} style={{ padding: "10px 14px", borderRadius: 10, fontSize: 12, lineHeight: 1.6, background: m.role === "user" ? C.brandLight : C.bg, border: `1px solid ${m.role === "user" ? C.brandMid : C.border}`, alignSelf: m.role === "user" ? "flex-end" : "flex-start", maxWidth: "92%" }}>
            {m.content.split("\n").map((l, j) => {
              let h = l.replace(/\*\*(.*?)\*\*/g, '<b>$1</b>');
              if (l.startsWith("• ") || l.startsWith("- ")) return <div key={j} style={{ paddingLeft: 10, marginBottom: 2 }}><span style={{ color: C.brand, marginRight: 4 }}>·</span><span dangerouslySetInnerHTML={{ __html: h.slice(2) }} /></div>;
              return <div key={j} style={{ marginBottom: l ? 1 : 6 }} dangerouslySetInnerHTML={{ __html: h }} />;
            })}
          </div>
        ))}
        {loading && <div style={{ padding: "10px 14px", borderRadius: 10, background: C.bg, border: `1px solid ${C.border}`, fontSize: 12, color: C.textMuted }}>Thinking...</div>}
        <div ref={endRef} />
      </div>
      <div style={{ padding: 12, borderTop: `1px solid ${C.border}`, display: "flex", gap: 6 }}>
        <input style={{ ...S.input, flex: 1 }} value={input} onChange={e => setInput(e.target.value)} onKeyDown={e => e.key === "Enter" && send()} placeholder="Ask about your data..." />
        <button style={S.btn("primary", true)} onClick={send} disabled={loading}>→</button>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// MAIN APP
// ═══════════════════════════════════════════════════════════════
export default function App() {
  const [tab, setTab] = useState("schema");
  const [scenarios, setScenarios] = useState([]);
  const tables = useMemo(() => SAMPLE_DATA_RAW, []);
  const discoveredSchema = useMemo(() => discoverSchema(tables), [tables]);
  const [schema, setSchema] = useState(discoveredSchema);
  const discoveredRels = useMemo(() => autoDiscoverRelationships(tables, discoveredSchema), [tables, discoveredSchema]);
  const [relationships, setRelationships] = useState(discoveredRels);
  const baseline = useMemo(() => buildBaseline(tables, schema, relationships), [tables, schema, relationships]);

  return (
    <div style={{ fontFamily: "'Plus Jakarta Sans', sans-serif", background: C.bg, color: C.text, height: "100vh", fontSize: 13, display: "flex", flexDirection: "column" }}>
      <link href={FONT_URL} rel="stylesheet" />
      <style>{`
        ::-webkit-scrollbar { width: 5px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: ${C.border}; border-radius: 3px; }
        select option { background: ${C.white}; color: ${C.text}; }
        * { box-sizing: border-box; }
        input:focus, select:focus { border-color: ${C.brand} !important; }
      `}</style>

      <div style={{ padding: "0 24px", height: 56, borderBottom: `1px solid ${C.border}`, display: "flex", alignItems: "center", justifyContent: "space-between", background: C.white, flexShrink: 0 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{ width: 28, height: 28, borderRadius: 8, background: `linear-gradient(135deg, ${C.brand}, ${C.brandDark})`, display: "flex", alignItems: "center", justifyContent: "center" }}>
            <span style={{ color: "#fff", fontSize: 13, fontWeight: 800 }}>d</span>
          </div>
          <span style={{ fontSize: 17, fontWeight: 800, color: C.text, letterSpacing: "-0.3px" }}>
            data<span style={{ color: C.brand }}>Bob</span>IQ
          </span>
        </div>

        <div style={{ display: "flex", gap: 2, background: C.bg, borderRadius: 10, padding: 3 }}>
          {[{ id: "schema", l: "Data Model" }, { id: "actuals", l: "Actuals" }, { id: "scenarios", l: "Scenarios" }].map(t => (
            <button key={t.id} onClick={() => setTab(t.id)} style={{
              padding: "7px 18px", borderRadius: 8, border: "none", cursor: "pointer", fontSize: 13, fontWeight: 600,
              fontFamily: "'Plus Jakarta Sans', sans-serif", transition: "all .15s",
              background: tab === t.id ? C.white : "transparent",
              color: tab === t.id ? C.brand : C.textMuted,
              boxShadow: tab === t.id ? "0 1px 3px rgba(0,0,0,0.08)" : "none",
            }}>
              {t.l}
              {t.id === "scenarios" && scenarios.length > 0 && (
                <span style={{ marginLeft: 6, background: C.brandLight, color: C.brand, borderRadius: 10, padding: "1px 7px", fontSize: 10, fontWeight: 700 }}>{scenarios.length}</span>
              )}
            </button>
          ))}
        </div>

        <div style={{ fontSize: 12, color: C.textMuted, fontWeight: 500 }}>Stadelmann GmbH · EUR</div>
      </div>

      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
        <div style={{ flex: 1, overflow: "auto", padding: 24 }}>
          {tab === "schema" && <SchemaView tables={tables} schema={schema} setSchema={setSchema} relationships={relationships} setRelationships={setRelationships} />}
          {tab === "actuals" && <ActualsView baseline={baseline} />}
          {tab === "scenarios" && <ScenariosView baseline={baseline} scenarios={scenarios} setScenarios={setScenarios} />}
        </div>
        <ChatPanel baseline={baseline} scenarios={scenarios} setScenarios={setScenarios} setActiveTab={setTab} />
      </div>
    </div>
  );
}
