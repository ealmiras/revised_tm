SELECT 
    df.sku,
    df.brand,
    df.ff_brand_cluster,
    season,
    season_group,
    protected,
    online_com,
    online_im,
    pb_im,
    pb_ce,
    pb_xsln1,
    pb_row1,
    pb_gb,
    actual_gm_im,
    available_qty,
    stock_on_hand,
    actual_st,
    eur_cost_price,
    online_weeks,
    target,
    eos_tm,
    goal_perc_st_weekly,
    weeks_behind_st,
    tm_reduction,
    revised_tm,
    tm_diff,
    CASE WHEN (SELECT COUNT(pr.sku) FROM promo_df pr WHERE pr.sku = df.sku) > 0 THEN 'Y' ELSE 'N' END AS in_promo,
    CASE WHEN (SELECT COUNT(exc.sku) FROM promo_exc_df exc WHERE exc.sku = df.sku) > 0 THEN 'Y' ELSE 'N' END AS promo_exc,
    ROUND (CASE 
                WHEN (SELECT COUNT(pr.sku) FROM promo_df pr WHERE pr.sku = df.sku) > 0 
                THEN pb_row1 / 1.22 * (1 - (SELECT max_disc_b2b FROM promo_df pr WHERE pr.sku = df.sku LIMIT 1))
                ELSE eur_cost_price / (1 - revised_tm)
            END, 0) AS new_pb_IM,
    ROUND (CASE 
                WHEN CASE 
                        WHEN (SELECT COUNT(pr.sku) FROM promo_df pr WHERE pr.sku = df.sku) > 0 
                        THEN pb_row1 / 1.22 * (1 - (SELECT max_disc_b2b FROM promo_df pr WHERE pr.sku = df.sku LIMIT 1))
                        ELSE eur_cost_price / (1 - revised_tm)
                    END < 600
                THEN CASE 
                        WHEN (SELECT COUNT(pr.sku) FROM promo_df pr WHERE pr.sku = df.sku) > 0 
                        THEN pb_row1 / 1.22 * (1 - (SELECT max_disc_b2b FROM promo_df pr WHERE pr.sku = df.sku LIMIT 1))
                        ELSE eur_cost_price / (1 - revised_tm)
                    END * 1.01 + 7.5
                ELSE CASE 
                        WHEN (SELECT COUNT(pr.sku) FROM promo_df pr WHERE pr.sku = df.sku) > 0 
                        THEN pb_row1 / 1.22 * (1 - (SELECT max_disc_b2b FROM promo_df pr WHERE pr.sku = df.sku LIMIT 1))
                        ELSE eur_cost_price / (1 - revised_tm)
                    END * 1 + 17
            END, 0) AS new_pb_CE,
    ROUND (CASE WHEN (SELECT COUNT(pr.sku) FROM promo_df pr WHERE pr.sku = df.sku) > 0 
                THEN pb_row1 / 1.22 * (1 - (SELECT max_disc_b2b FROM promo_df pr WHERE pr.sku = df.sku LIMIT 1))
                ELSE eur_cost_price / (1 - revised_tm)
            END, 0) AS new_pb_XSLN1
FROM df
LEFT JOIN promo_df pr ON df.sku = pr.sku
ORDER BY stock_on_hand DESC