WITH 
ab_df AS (
    SELECT 
        *,
        (1 - eur_cost_price / row1_ab) - max_reduction AS row1_ab_tgm_cap,
        (1 - eur_cost_price / row_ab) - max_reduction AS row_ab_tgm_cap,
        (1 - eur_cost_price / au_ab) - max_reduction AS au_ab_tgm_cap,
        (1 - eur_cost_price / kr_ab) - max_reduction AS kr_ab_tgm_cap,
        (1 - eur_cost_price / cn_ab) - max_reduction AS cn_ab_tgm_cap,
        (1 - eur_cost_price*0.86 / gb_ab) - max_reduction AS gb_ab_tgm_cap,
        (1 - eur_cost_price*1.08 / us_ab) - max_reduction AS us_ab_tgm_cap,
        (1 - eur_cost_price*165 / jp_ab) - max_reduction AS jp_ab_tgm_cap,
        (1 - eur_cost_price / hk_ab) - max_reduction AS hk_ab_tgm_cap
    FROM df 
),
p AS (
    SELECT
        *,
        ROUND(
            CASE WHEN row1_ab > 0 
            THEN MIN(row1*0.85, 
                    MAX(
                        ab_df.eur_cost_price / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'ROW1' LIMIT 1))),
                        ab_df.eur_cost_price / (1 - ab_df.row1_ab_tgm_cap)))
            ELSE ab_df.eur_cost_price / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'ROW1' LIMIT 1)))
            END, 0) AS row1_ab_new,
        ROUND(
            CASE WHEN row_ab > 0
            THEN MIN(row_*0.85, 
                    MAX(
                        ab_df.eur_cost_price / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'ROW' LIMIT 1))),
                        ab_df.eur_cost_price / (1 - ab_df.row_ab_tgm_cap)))
            ELSE ab_df.eur_cost_price / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'ROW' LIMIT 1)))
            END, 0) AS row_ab_new,
        ROUND(
            CASE WHEN au_ab > 0 
            THEN MIN(au*0.85, 
                    MAX(
                        ab_df.eur_cost_price / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'AU' LIMIT 1))),
                        ab_df.eur_cost_price / (1 - ab_df.au_ab_tgm_cap)))
            ELSE ab_df.eur_cost_price / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'AU' LIMIT 1)))
            END, 0) AS au_ab_new,
        ROUND(
            CASE WHEN kr_ab > 0
            THEN MIN(kr*0.85, 
                    MAX(
                        ab_df.eur_cost_price / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'KR' LIMIT 1))),
                        ab_df.eur_cost_price / (1 - ab_df.kr_ab_tgm_cap)))
            ELSE ab_df.eur_cost_price / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'KR' LIMIT 1)))
            END, 0) AS kr_ab_new,
        ROUND(
            CASE WHEN cn_ab > 0 
            THEN MIN(cn*0.85, 
                    MAX(
                        ab_df.eur_cost_price / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'CN' LIMIT 1))),
                        ab_df.eur_cost_price / (1 - ab_df.cn_ab_tgm_cap)))
            ELSE ab_df.eur_cost_price / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'CN' LIMIT 1)))
            END, 0) AS cn_ab_new,
        ROUND(
            CASE WHEN gb_ab > 0
            THEN MIN(gb*0.85, 
                    MAX(
                        ab_df.eur_cost_price*0.86 / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'GB' LIMIT 1))),
                        ab_df.eur_cost_price*0.86 / (1 - ab_df.gb_ab_tgm_cap)))
            ELSE ab_df.eur_cost_price*0.86 / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'GB' LIMIT 1)))
            END, 0) AS gb_ab_new,
        ROUND(
            CASE WHEN us_ab > 0
            THEN MIN(us*0.85, 
                    MAX(
                        ab_df.eur_cost_price*1.08 / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'US' LIMIT 1))),
                        ab_df.eur_cost_price*1.08 / (1 - ab_df.us_ab_tgm_cap)))
            ELSE ab_df.eur_cost_price*1.08 / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'US' LIMIT 1)))
            END, 0) AS us_ab_new,
        ROUND(
            CASE WHEN jp_ab > 0
            THEN MIN(jp*0.85, 
                    MAX(
                        ab_df.eur_cost_price*165 / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'JP' LIMIT 1))),
                        ab_df.eur_cost_price*165 / (1 - ab_df.jp_ab_tgm_cap)))
            ELSE ab_df.eur_cost_price*165 / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'JP' LIMIT 1)))
            END, 0) AS jp_ab_new,
        ROUND(
            CASE WHEN hk_ab > 0
            THEN MIN(hk*0.85, 
                    MAX(
                        ab_df.eur_cost_price / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'HKMO' LIMIT 1))),
                        ab_df.eur_cost_price / (1 - ab_df.hk_ab_tgm_cap)))
            ELSE ab_df.eur_cost_price / (1 - (ab_df.target_cm - (SELECT total FROM v_cost WHERE price_column = 'HKMO' LIMIT 1)))
            END, 0) AS hk_ab_new
    FROM ab_df
)
SELECT *,
    ROUND(1 - row1_ab_new / row1_ab, 2) AS row1_ab_diff,
    ROUND(1 - row_ab_new / row_ab, 2) AS row_ab_diff,
    ROUND(1 - au_ab_new / au_ab, 2) AS au_ab_diff,
    ROUND(1 - kr_ab_new / kr_ab, 2) AS kr_ab_diff,
    ROUND(1 - cn_ab_new / cn_ab, 2) AS cn_ab_diff,
    ROUND(1 - gb_ab_new / gb_ab, 2) AS gb_ab_diff,
    ROUND(1 - us_ab_new / us_ab, 2) AS us_ab_diff,
    ROUND(1 - jp_ab_new / jp_ab, 2) AS jp_ab_diff,
    ROUND(1 - hk_ab_new / hk_ab, 2) AS hk_ab_diff,
    ROUND(1 - row1_ab_new / row1, 2) AS row1_ab_disc,
    ROUND(1 - row_ab_new / row_, 2) AS row_ab_disc,
    ROUND(1 - au_ab_new / au, 2) AS au_ab_disc,
    ROUND(1 - kr_ab_new / kr, 2) AS kr_ab_disc,
    ROUND(1 - cn_ab_new / cn, 2) AS cn_ab_disc,
    ROUND(1 - gb_ab_new / gb, 2) AS gb_ab_disc,
    ROUND(1 - us_ab_new / us, 2) AS us_ab_disc,
    ROUND(1 - jp_ab_new / jp, 2) AS jp_ab_disc,
    ROUND(1 - hk_ab_new / hk, 2) AS hk_ab_disc
FROM p