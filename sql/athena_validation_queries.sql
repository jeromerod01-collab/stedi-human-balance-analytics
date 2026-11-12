-- Run each query in Athena (database: stedi) and screenshot the result.
-- Expected counts are in the comments.

-- ===== LANDING =====
SELECT COUNT(*) FROM customer_landing;                                      -- 956   -> screenshots/customer_landing.png
SELECT * FROM customer_landing WHERE sharewithresearchasofdate IS NULL;     -- 474 rows with blank consent (shows the problem)
SELECT COUNT(*) FROM accelerometer_landing;                                 -- 81273 -> screenshots/accelerometer_landing.png
SELECT COUNT(*) FROM step_trainer_landing;                                  -- 28680 -> screenshots/step_trainer_landing.png

-- ===== TRUSTED =====
SELECT COUNT(*) FROM customer_trusted;                                      -- 482   -> screenshots/customer_trusted.png
SELECT COUNT(*) FROM customer_trusted WHERE sharewithresearchasofdate IS NULL; -- 0
SELECT COUNT(*) FROM accelerometer_trusted;                                 -- 40981 -> screenshots/accelerometer_trusted.png
SELECT COUNT(*) FROM step_trainer_trusted;                                  -- 14460 -> screenshots/step_trainer_trusted.png

-- ===== CURATED =====
SELECT COUNT(*) FROM customer_curated;                                      -- 482   -> screenshots/customer_curated.png
SELECT COUNT(*) FROM machine_learning_curated;                              -- 43681 -> screenshots/machine_learning_curated.png
