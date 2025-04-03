import pandas as pd
import pandasql as ps
import numpy as np
import datetime
import pathlib
from sqlalchemy import create_engine
import xlsxwriter
import datetime as dt
from tqdm import tqdm

pd.options.mode.chained_assignment = None

## GENERAL INFORMATION -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
currentlocation = str(pathlib.Path(__file__).parent.absolute())[:-7]

calc_date = '2025-04-03'
arc_date = '2025-03-17'

current_season = 'SS25'
current_eos_date = '2025-05-01'

new_season = 'AW25'
new_eos_date = '2025-11-01'

last_season = 'AW24'

## FUNCTIONS ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def run_query (df):
    query = open(currentlocation + '\\- Code\\_PricingQuery_NewCurrent.sql').read()
    local_vars = {'df':df, 'promo_df':promo_df, 'promo_exc_df':promo_exc_df, 'mp_exclusions_df':mp_exclusions_df}
    return ps.sqldf(query, local_vars)

def run_query_old (df):
    query = open(currentlocation + '\\- Code\\_PricingQuery_Old.sql').read()
    local_vars = {'df':df, 'promo_df':promo_df, 'promo_exc_df':promo_exc_df, 'mp_exclusions_df':mp_exclusions_df}
    return ps.sqldf(query, local_vars)

def run_query_co (df):
    query = open(currentlocation + '\\- Code\\_PricingQuery_CO.sql').read()
    local_vars = {'df':df, 'promo_df':promo_df, 'promo_exc_df':promo_exc_df, 'mp_exclusions_df':mp_exclusions_df}
    return ps.sqldf(query, local_vars)

def run_query_ab (df):
    query = open(currentlocation + '\\- Code\\_PricingQuery_AB.sql').read()
    local_vars = {'df':df, 'promo_df':promo_df, 'v_cost':v_cost}
    return ps.sqldf(query, local_vars)

def restricted_cat (df):
    query = """
            UPDATE df
            SET 
                new_pb_IM = CASE WHEN (1-new_pb_IM/pb_row1) < 0.2 OR season_group <> '4. Restricted Categories' THEN new_pb_IM ELSE ROUND(pb_row1*0.2, 0) END,
                new_pb_CE = CASE WHEN (1-new_pb_CE/pb_row1) < 0.2 OR season_group <> '4. Restricted Categories' THEN new_pb_CE ELSE ROUND(pb_row1*0.2 + 15, 0) END,
                new_pb_XSLN1 = CASE WHEN (1-new_pb_XSLN1/pb_row1) < 0.2 OR season_group <> '4. Restricted Categories' THEN new_pb_XSLN1 ELSE ROUND(pb_row1*0.2, 0) END
            """
    local_vars = {'df':df}
    return ps.sqldf(query, local_vars)

## --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## COLLECTING THE REQUIRED INFORMATION 
start_time = dt.datetime.now()
print(f'\nStart time: {start_time}')
print('---'*20)
print('Collecting required data:')

engine = create_engine('postgresql+psycopg2://postgres:E8EMjzcplJMNj1bw@35.205.177.96:5432/bi')

print('* Excluded brands')
exclusions_q = """SELECT * FROM store_mngmnt.lncc_marketplaces_pricebooks_brands_exclusion"""
mp_exclusions_df = pd.read_sql_query(exclusions_q, engine)

print('* Promo file')
promo_file_q = """SELECT sku, max_disc_b2b, max_disc_private, chameleon_flag FROM store_mngmnt.lncc_stock_management WHERE sku IS NOT NULL"""
promo_df = pd.read_sql_query(promo_file_q, engine)

print('* Promo inclusions')
promo_exc_q = """SELECT * FROM store_mngmnt.lncc_promo_exclusions WHERE sku IS NOT NULL"""
promo_exc_df = pd.read_sql_query(promo_exc_q, engine)

print('* Variable costs')
v_cost_q = """SELECT * FROM store_mngmnt.lncc_variable_costs"""
v_cost = pd.read_sql_query(v_cost_q, engine)

# mp_exclusions_df = pd.read_csv(currentlocation + '\\- Useful\\MPExclusions.csv')
# promo_df = pd.read_csv(currentlocation + '\\- Useful\\Promo.csv')
# promo_exc_df = pd.read_csv(currentlocation + 'ù\\- Useful\\PromoExclusions.csv')
# v_cost = pd.read_csv(currentlocation + '\\- Useful\\VariableCosts.csv'ù)

# AB Price Books
print('* AB pricebooks')
pb_file_path = 'C:\\Users\\TLG User\\The Level Group\\BI - Reporting - 09. LN - 09. LN\\LN_PO\\Pricebooks\\Export_Pricebook_SKU.xlsx'

ab_pb_list = ['09ROW1_AB', '09ROW_AB', '09AU_AB', '09KR_AB', '09CN_AB', '09GB_AB', '09US_AB', '09JP_AB', '09HK_AB']
ab_pb_df = pd.DataFrame(columns=['SKU'])

for i in tqdm(range(len(ab_pb_list)), desc="-- Processing", unit="iteration"): #ab_pb_list:
    pb = ab_pb_list[i]
    pb_content = pd.read_excel(pb_file_path, sheet_name = pb)
    pb_content = pb_content[['SKU', 'Amount']].dropna(subset=['SKU'])
    pb_content = pb_content.groupby(['SKU']).max().reset_index(drop=False)
    pb_content = pb_content.rename(columns={'Amount' : pb})
    ab_pb_df = ab_pb_df.merge(pb_content, how='outer', left_on='SKU', right_on='SKU').reset_index(drop=True)

# Product data - from the power automate export csv - connected to the Business Intelligence Dashboard
print('* Importing data export')

df = pd.read_csv(currentlocation + '\\RevisedTM_DataExport.csv')
df = df.loc[(df['season'] != 'SS26')]
# df = df.loc[df['season_group'] != '4. Protected Categories']

co_df = pd.read_csv(currentlocation + '\\RevisedTM_DataExport_CO.csv')
# co_df = co_df.loc[(co_df['co_status'] != 'Existing CO') & (co_df['co_status'] != 'New CO')]
# co_df = co_df.loc[co_df['season_group'] != '4. Protected Categories']


df['publishing_date'] = pd.to_datetime(df['publishing_date'], format="%Y-%m-%d")
df['stock_on_hand'] = df['available_qty'] * df['eur_cost_price']
df['calculation_date'] = pd.to_datetime(calc_date, format="%Y-%m-%d")
df['current_eos_date'] = pd.to_datetime(current_eos_date, format="%Y-%m-%d")
df['new_eos_date'] = pd.to_datetime(new_eos_date, format="%Y-%m-%d")
df['net_whs_value'].fillna(0, inplace=True)
df = df.loc[df['publishing_date'] < df['calculation_date']]

df['actual_st'] = df['net_whs_value'] / (df['stock_on_hand'] - df['net_whs_value']) *-1
df['ff_brand_cluster'] = np.where(df['brand'].str.lower in ['balenciaga', 'saint laurent', 'gucci', 'bottega veneta', 'max mara', 'the row'], 'reduced', 'normal')

# If the price of previous week is lower, use that one
archive_tm = pd.read_excel(currentlocation + '\\ArchivePrices_' + arc_date + '.xlsx', sheet_name='Archive')[['sku', 'pb_im_arc']]
df = pd.merge(df, archive_tm, how='left', left_on='sku', right_on='sku').reset_index(drop=True)
df['pb_im_arc'].fillna(100000, inplace=True)
df['pb_im'] = np.where(df['pb_im'] > df['pb_im_arc'], df['pb_im_arc'], df['pb_im'])

# Separating data by the season
new_df = df.loc[df['season'] == new_season]
current_df = df.loc[df['season'] == current_season]
old_df = df.loc[(df['season'] != current_season) & (df['season'] != new_season) & (df['season'] != 'CO')]

## --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## NEW SEASON
print('---'*20)
print(f'\nNEW SEASON: \n')

new_season_groups = [(new_df['season_group'] == '3. Seasonal'), 
                     (new_df['season_group'] == '2. Seasonal no MD')]
new_eos_st_goals = [0.6, 0.55]
eos_tms = [0, 0.1]
tm_reduction_caps = [0.1, 0.05]

new_df['eos_st_goal'] = np.select(new_season_groups, new_eos_st_goals, default=np.nan)
new_df['eos_tm'] = np.select(new_season_groups, eos_tms, default=eos_tms[1])
new_df['max_reduction'] = np.select(new_season_groups, tm_reduction_caps, default=tm_reduction_caps[1])

new_df['actual_gm_im'] = 1 - new_df['eur_cost_price'] / new_df['pb_im']

new_df['possible_weeks'] = (new_df['new_eos_date'] - new_df['publishing_date']).dt.days / 7
new_df['goal_perc_st_weekly'] = new_df['eos_st_goal'] / new_df['possible_weeks']
new_df['online_weeks'] = round((new_df['calculation_date'] - new_df['publishing_date']).dt.days / 7, 0)
new_df['target'] = new_df['goal_perc_st_weekly'] * new_df['online_weeks']
# new_df['actual_st'] = new_df['net_whs_value'] / (new_df['stock_on_hand'] - new_df['net_whs_value']) *-1

ideal_tm_cond = [((new_df['brand'] == 'Gucci') & (new_df['season'] == 'CO')), 
                       ((new_df['brand'] == 'Gucci') & (new_df['season'] != 'CO')),
                       ((new_df['brand'] != 'Gucci') & (new_df['season'] == 'CO')),
                       ((new_df['brand'] != 'Gucci') & (new_df['season'] != 'CO'))]
ideal_tms = [0.375, 0.275, 0.325, 0.225]
new_df['ideal_tm'] = np.select(ideal_tm_cond, ideal_tms, default=ideal_tms[3])

new_df['tm_margin'] = new_df['actual_gm_im'] - new_df['eos_tm']
new_df['tm_margin week'] = new_df['tm_margin'] / ((new_df['current_eos_date'] - new_df['calculation_date']).dt.days / 7)
new_df['weeks_behind_st'] = np.where(new_df['actual_st'] > new_df['target'], 0, (new_df['target'] - new_df['actual_st']) / new_df['goal_perc_st_weekly'])
new_df['tm_reduction'] = new_df['weeks_behind_st'] * new_df['tm_margin week']
new_df['tm_reduction'] = np.where(new_df['max_reduction'] < new_df['tm_reduction'], new_df['max_reduction'], new_df['tm_reduction'])

new_df['revised_tm'] = round((new_df['actual_gm_im'] - new_df['tm_reduction']), 3)
# new_df['revised_tm'] = new_df['revised_tm'].where(new_df['revised_tm'] >= -0.25, other = -0.25)
new_df['revised_tm'] = new_df['revised_tm'].where(new_df['actual_gm_im'] >= new_df['revised_tm'], other = new_df['actual_gm_im'])
new_df['revised_tm'] = new_df['revised_tm'].where(new_df['revised_tm'] >= new_df['eos_tm'], other = new_df['eos_tm'])
new_df['min_price'] = np.where(new_df['season_group'] == '4. Protected Categories', new_df['pb_row1']*0.8, 0)
new_df['min_tm'] = np.where(new_df['season_group'] == '4. Protected Categories', 1 - new_df['eur_cost_price'] / new_df['min_price'], 0)
new_df['revised_tm'] = new_df['revised_tm'].where(new_df['min_price'] == 0, new_df['min_tm'])

# print(new_df.loc[new_df['season_group'] == '4. Protected Categories'])

new_df['tm_diff'] = new_df['revised_tm'] - new_df['actual_gm_im']
# print(new_df)

new_ab = new_df[['sku', 'season', 'season_group', 'publishing_date', 'calculation_date', 'private_high', 'private_medium', 'public_high', 'public_medium', 'eur_cost_price', 'revised_tm', 'max_reduction']]

new_summary = run_query(new_df)
new_summary = new_summary.loc[new_summary['in_promo'] == "N"].reset_index(drop=True)

new_summary['ff_high'] = np.where(new_summary['ff_brand_cluster'] == 'reduced', 0.95, 0.85)*new_summary['pb_row1']
new_summary['ff_low'] = new_summary['pb_row1']*0.6
new_summary['ff_base'] = (new_summary['new_pb_IM'] + 15)*1.22
new_summary['new_pb_FF'] = np.where(new_summary['ff_high'] < new_summary['ff_base'], new_summary['ff_high'], np.where(new_summary['ff_low'] > new_summary['ff_base'], new_summary['ff_low'], new_summary['ff_base']))
new_summary['new_pb_FF'] = round(new_summary['new_pb_FF'], 0)
new_summary['new_pb_FFGB'] = round((new_summary['new_pb_FF']/new_summary['pb_row1'])*new_summary['pb_gb'], 0)
new_summary.drop(columns={'ff_high', 'ff_low', 'ff_base', 'ff_brand_cluster'}, inplace=True)

new_summary['im_change'] = new_summary['new_pb_IM'] / new_summary['pb_im'] - 1
print('# of SKU prices calculated for ' + new_season + ': ' + str(new_summary['sku'].count()))
print(new_summary.head())

all_new = new_summary[['sku', 'new_pb_IM', 'new_pb_CE', 'new_pb_XSLN1', 'new_pb_FF', 'new_pb_FFGB']]

## --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# CURRENT SEASON
print('---'*20)
print(f'\nCURRENT SEASON: \n')

current_season_groups = [(current_df['season_group'] == '3. Seasonal'), 
                         (current_df['season_group'] == '2. Seasonal no MD')]
new_eos_st_goals = [0.65, 0.60]
eos_tms = [0, 0.05]
tm_reduction_caps = [0.1, 0.05]

current_df['eos_st_goal'] = np.select(current_season_groups, new_eos_st_goals, default=np.nan)
current_df['eos_tm'] = np.select(current_season_groups, eos_tms, default=eos_tms[1])
current_df['max_reduction'] = np.select(current_season_groups, tm_reduction_caps, default=tm_reduction_caps[1])

current_df['actual_gm_im'] = 1 - current_df['eur_cost_price'] / current_df['pb_im']

current_df['possible_weeks'] = (current_df['current_eos_date'] - current_df['publishing_date']).dt.days / 7
current_df['goal_perc_st_weekly'] = current_df['eos_st_goal'] / current_df['possible_weeks']
current_df['online_weeks'] = round((current_df['calculation_date'] - current_df['publishing_date']).dt.days / 7, 0)
current_df['target'] = current_df['goal_perc_st_weekly'] * current_df['online_weeks']
# current_df['actual_st'] = current_df['net_whs_value'] / (current_df['stock_on_hand'] - current_df['net_whs_value']) *-1

ideal_tm_cond = [((current_df['brand'] == 'Gucci') & (current_df['season'] == 'CO')), 
                       ((current_df['brand'] == 'Gucci') & (current_df['season'] != 'CO')),
                       ((current_df['brand'] != 'Gucci') & (current_df['season'] == 'CO')),
                       ((current_df['brand'] != 'Gucci') & (current_df['season'] != 'CO'))]
ideal_tms = [0.375, 0.275, 0.325, 0.225]
current_df['ideal_tm'] = np.select(ideal_tm_cond, ideal_tms, default=0.225)

current_df['tm_margin'] = current_df['actual_gm_im'] - current_df['eos_tm']
current_df['tm_margin week'] = current_df['tm_margin'] / ((current_df['current_eos_date'] - current_df['calculation_date']).dt.days / 7)
current_df['weeks_behind_st'] = np.where(current_df['actual_st'] > current_df['target'], 0, (current_df['target'] - current_df['actual_st']) / current_df['goal_perc_st_weekly'])
current_df['tm_reduction'] = current_df['weeks_behind_st'] * current_df['tm_margin week']
current_df['tm_reduction'] = np.where(current_df['max_reduction'] < current_df['tm_reduction'], current_df['max_reduction'], current_df['tm_reduction'])

current_df['revised_tm'] = round((current_df['actual_gm_im'] - current_df['tm_reduction']), 3)
# current_df['revised_tm'] = current_df['revised_tm'].where(current_df['revised_tm'] >= -0.25, other = -0.25)
current_df['revised_tm'] = current_df['revised_tm'].where(current_df['actual_gm_im'] >= current_df['revised_tm'], other = current_df['actual_gm_im'])
current_df['revised_tm'] = current_df['revised_tm'].where(current_df['revised_tm'] >= current_df['eos_tm'], other = current_df['eos_tm'])
current_df['min_price'] = np.where(current_df['season_group'] == '4. Protected Categories', current_df['pb_row1']*0.8, 0)
current_df['min_tm'] = np.where(current_df['season_group'] == '4. Protected Categories', 1 - current_df['eur_cost_price'] / current_df['min_price'], 0)
current_df['revised_tm'] = current_df['revised_tm'].where(current_df['min_price'] == 0, current_df['min_tm'])

# print(current_df.loc[current_df['season_group'] == '4. Protected Categories'])

current_df['tm_diff'] = current_df['revised_tm'] - current_df['actual_gm_im']

current_ab = current_df[['sku', 'season', 'season_group', 'publishing_date', 'calculation_date', 'private_high', 'private_medium', 'public_high', 'public_medium', 'eur_cost_price', 'revised_tm', 'max_reduction']]

current_summary = run_query(current_df)
current_summary = current_summary.loc[current_summary['in_promo'] == "N"].reset_index(drop=True)
# current_summary = restricted_cat(current_summary)

current_summary['ff_high'] = np.where(current_summary['ff_brand_cluster'] == 'reduced', 0.95, 0.85)*current_summary['pb_row1']
current_summary['ff_low'] = current_summary['pb_row1']*0.6
current_summary['ff_base'] = (current_summary['new_pb_IM'] + 15)*1.22
current_summary['new_pb_FF'] = np.where(current_summary['ff_high'] < current_summary['ff_base'], current_summary['ff_high'], np.where(current_summary['ff_low'] > current_summary['ff_base'], current_summary['ff_low'], current_summary['ff_base']))
current_summary['new_pb_FF'] = round(current_summary['new_pb_FF'], 0)
current_summary['new_pb_FFGB'] = round((current_summary['new_pb_FF']/current_summary['pb_row1'])*current_summary['pb_gb'], 0)
current_summary.drop(columns={'ff_high', 'ff_low', 'ff_base', 'ff_brand_cluster'}, inplace=True)

current_summary['im_change'] = current_summary['new_pb_IM'] / current_summary['pb_im'] - 1
print('# of SKU prices calculated for ' + current_season + ': ' + str(current_summary['sku'].count()))
print(current_summary.head())

all_current = current_summary[['sku', 'new_pb_IM', 'new_pb_CE', 'new_pb_XSLN1', 'new_pb_FF', 'new_pb_FFGB']]

## --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## OLD SEASON
print('---'*20)
print(f'\nOLD SEASON: \n')

old_df['actual_gm_im'] = 1 - old_df['eur_cost_price'] / old_df['pb_im']

tm_reduction_cond = [((old_df['season_group'] == '3. Seasonal') & (old_df['actual_st'] < 0.3)),
                     ((old_df['season_group'] == '3. Seasonal') & (old_df['actual_st'] < 0.6)),
                     ((old_df['season_group'] == '3. Seasonal') & (old_df['actual_st'] < 0.95)),
                     ((old_df['season_group'] == '3. Seasonal') & (old_df['actual_st'] <= 1)),
                     ((old_df['season_group'] == '2. Seasonal no MD') & (old_df['actual_st'] < 0.25)),
                     ((old_df['season_group'] == '2. Seasonal no MD') & (old_df['actual_st'] < 0.4)),
                     ((old_df['season_group'] == '2. Seasonal no MD') & (old_df['actual_st'] < 0.85)),
                     ((old_df['season_group'] == '2. Seasonal no MD') & (old_df['actual_st'] <= 1))]
reducted_tm_os = [-0.50, -0.40, -0.40, old_df['actual_gm_im'],
                  -0.50, -0.40, -0.30, old_df['actual_gm_im']]
max_reduction = [-0.15, -0.15, -0.15, 0,
                 -0.10, -0.10, -0.10, 0]

old_df['calculated_tm'] = np.select(tm_reduction_cond, reducted_tm_os, default=0)
old_df['max_reduction'] = np.select(tm_reduction_cond, max_reduction, default=0)
old_df['revised_tm'] = old_df['calculated_tm'].where(old_df['calculated_tm'] > (old_df['actual_gm_im'] + old_df['max_reduction']), other=(old_df['actual_gm_im'] + old_df['max_reduction']))
old_df['revised_tm'] = old_df['revised_tm'].where(old_df['actual_gm_im'] >= old_df['revised_tm'], other = old_df['actual_gm_im'])
old_df['revised_tm'] = old_df['revised_tm'].where(old_df['pb_im'] > 0, other = old_df['calculated_tm'])
old_df['min_price'] = np.where(old_df['season_group'] == '4. Protected Categories', old_df['pb_row1']*0.8, 0)
old_df['min_tm'] = np.where(old_df['season_group'] == '4. Protected Categories', 1 - old_df['eur_cost_price'] / old_df['min_price'], 0)
old_df['revised_tm'] = old_df['revised_tm'].where(old_df['min_price'] == 0, old_df['min_tm'])

old_df['tm_diff'] = old_df['revised_tm'] - old_df['actual_gm_im']

old_ab = old_df[['sku', 'season', 'season_group', 'publishing_date', 'calculation_date', 'private_high', 'private_medium', 'public_high', 'public_medium', 'eur_cost_price', 'revised_tm', 'max_reduction']]

old_summary = run_query_old(old_df).reset_index(drop=True)
old_summary = old_summary.loc[old_summary['in_promo'] == "N"]

old_summary['ff_high'] = np.where(old_summary['ff_brand_cluster'] == 'reduced', 0.95, 0.85)*old_summary['pb_row1']
old_summary['ff_low'] = np.where(old_summary['season'] == last_season, old_summary['pb_row1']*0.5, old_summary['pb_row1']*0.4)
old_summary['ff_base'] = (old_summary['new_pb_IM'] + 15)*1.22
old_summary['new_pb_FF'] = np.where(old_summary['ff_high'] < old_summary['ff_base'], old_summary['ff_high'], np.where(old_summary['ff_low'] > old_summary['ff_base'], old_summary['ff_low'], old_summary['ff_base']))
old_summary['new_pb_FF'] = round(old_summary['new_pb_FF'], 0)
old_summary['new_pb_FFGB'] = round((old_summary['new_pb_FF']/old_summary['pb_row1'])*old_summary['pb_gb'], 0)
old_summary.drop(columns={'ff_high', 'ff_low', 'ff_base', 'ff_brand_cluster'}, inplace=True)

old_summary['im_change'] = old_summary['new_pb_IM'] / old_summary['pb_im'] - 1
print('# of SKU prices calculated for ' + last_season + ' & old seasons: ' + str(old_summary['sku'].count()))
print(old_summary.head())

all_old = old_summary[['sku', 'new_pb_IM', 'new_pb_CE', 'new_pb_XSLN1', 'new_pb_FF', 'new_pb_FFGB']]

## --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## CO SEASON
print('---'*20)
print(f'\nCO SEASON: \n')

co_df['ff_brand_cluster'] = np.where(co_df['brand'].str.lower in ['balenciaga', 'saint laurent', 'gucci', 'bottega veneta', 'max mara', 'the row'], 'reduced', 'normal')

co_df = co_df.loc[((co_df['co_status'] != 'Existing CO') & (co_df['co_status'] != 'New CO')) | (co_df['coverage'] > 56)]

# If the price of previous week is lower, use that one
co_df = pd.merge(co_df, archive_tm, how='left', left_on='sku', right_on='sku').reset_index(drop=True)
co_df['pb_im_arc'].fillna(100000, inplace=True)
co_df['pb_im'] = np.where(co_df['pb_im'] > co_df['pb_im_arc'], co_df['pb_im_arc'], co_df['pb_im'])

# Calculations
co_df['net_whs_value'].fillna(0, inplace=True)
co_df['received_lm'].fillna(0, inplace=True)
co_df['returned_lm'].fillna(0, inplace=True)
co_df['sold_lm'].fillna(0, inplace=True)
co_df['avg_ops_date'].fillna(0, inplace=True)
co_df['avg_ops_date'] = pd.to_datetime(co_df['avg_ops_date'])
co_df['publishing_date'] = pd.to_datetime(co_df['publishing_date'], format="%Y-%m-%d")
co_df['stock_on_hand'] = co_df['available_qty'] * co_df['eur_cost_price']
co_df['calculation_date'] = pd.to_datetime(calc_date, format="%Y-%m-%d")
co_df['current_eos_date'] = pd.to_datetime(current_eos_date, format="%Y-%m-%d")
co_df = co_df.loc[co_df['publishing_date'] < co_df['calculation_date']]
co_df['actual_gm_im'] = 1 - co_df['eur_cost_price'] / co_df['pb_im']

co_df['actual_st'] = co_df['net_whs_value'] / (co_df['stock_on_hand'] - co_df['net_whs_value']) *-1

co_df['stock_qty_lm'] = co_df['available_qty'] - co_df['received_lm'] - co_df['returned_lm'] + co_df['sold_lm']
co_df['availability'] = (co_df['stock_qty_lm']*30 + co_df['received_lm'] * (co_df['calculation_date'] - co_df['avg_ops_date']).dt.days) / (co_df['stock_qty_lm'] + co_df['received_lm'])
co_df['sales_velocity'] = co_df['sold_lm'] / co_df['availability']
co_df['coverage'] = co_df['available_qty'] / co_df['sales_velocity'] / 7

co_df = co_df.loc[((co_df['calculation_date'] - co_df['publishing_date']).dt.days > 14)]
# print(co_df)

tm_reduction_cond_co = [(co_df['co_status'] == 'Existing CO') & (co_df['available_qty'] > 2) & (co_df['coverage'] > 56), #Existing CO
                       (co_df['co_status'] == 'Existing CO') & (co_df['available_qty'] > 2) & (co_df['coverage'] > 36), #Existing CO
                       (co_df['co_status'] == 'Existing CO') & (co_df['coverage'] < 20), #Existing CO - reverse constraint
                       (co_df['co_status'] == 'New CO') & (co_df['available_qty'] > 2) & (co_df['coverage'] > 56), #New CO
                       (co_df['co_status'] == 'New CO') & (co_df['available_qty'] > 2) & (co_df['coverage'] > 36), #New CO
                       (co_df['co_status'] == 'LS Existing CO') & (co_df['available_qty'] > 2) & (co_df['coverage'] > 56), #LS Existing CO
                       (co_df['co_status'] == 'LS Existing CO') & (co_df['available_qty'] > 2) & (co_df['coverage'] > 36), #LS Existing CO
                       (co_df['co_status'] == 'LS New CO') & (co_df['coverage'] > 56), #LS New CO
                       (co_df['co_status'] == 'LS New CO') & (co_df['coverage'] > 36), #LS New CO
                       (co_df['co_status'] == 'Discontinued CO') & (co_df['coverage'] > 56), #Discontinue CO
                       (co_df['co_status'] == 'Discontinued CO') & (co_df['coverage'] > 36)] #Discontinue CO

reducted_tm_os_co = [0.05, 0.15, co_df['actual_gm_im'] + 0.05,
                     0.03, 0.05, 
                     -0.05, 0, 
                     -0.15, -0.1,                   
                     -0.30, -0.20]

max_reduction_co = [-0.1, -0.1, 0.05,
                    -0.1, -0.1, 
                    -0.1, -0.1,
                    -0.15, -0.15,
                    -0.2, -0.2]

co_df['calculated_tm'] = np.select(tm_reduction_cond_co, reducted_tm_os_co, default=co_df['actual_gm_im'])
co_df['max_reduction'] = np.select(tm_reduction_cond_co, max_reduction_co, default=0)
co_df['revised_tm'] = np.where(co_df['calculated_tm'] < (co_df['actual_gm_im'] + co_df['max_reduction']), (co_df['actual_gm_im'] + co_df['max_reduction']), co_df['calculated_tm'])
co_df['revised_tm'] = co_df['revised_tm'].where(co_df['revised_tm'] > -0.25, other=-0.25)
co_df['revised_tm'] = np.where((tm_reduction_cond_co[2]) | (co_df['revised_tm'] <= co_df['actual_gm_im']), co_df['revised_tm'], co_df['actual_gm_im'])
co_df['min_price'] = np.where(co_df['season_group'] == '4. Protected Categories', co_df['pb_row1']*0.8, 0)
co_df['min_tm'] = np.where(co_df['season_group'] == '4. Protected Categories', 1 - co_df['eur_cost_price'] / co_df['min_price'], 0)
co_df['revised_tm'] = co_df['revised_tm'].where(co_df['min_price'] == 0, co_df['min_tm'])

co_df['tm_diff'] = co_df['revised_tm'] - co_df['actual_gm_im']

co_ab = co_df[['sku', 'season', 'season_group', 'publishing_date', 'calculation_date', 'private_high', 'private_medium', 'public_high', 'public_medium', 'eur_cost_price', 'revised_tm', 'max_reduction']]

co_summary = run_query_co(co_df).reset_index(drop=True)
co_summary = co_summary.loc[co_summary['in_promo'] == "N"].reset_index(drop=True)

seasons = [co_summary['last_season'] == new_season,
           co_summary['last_season'] == current_season,
           co_summary['last_season'] == last_season]

co_summary['ff_high'] = np.where(co_summary['ff_brand_cluster'] == 'reduced', 0.95, 0.85)*co_summary['pb_row1']
co_summary['ff_low'] = np.select(seasons, [0.6, 0.6, 0.5], default=0.4)*co_summary['pb_row1']
co_summary['ff_base'] = (co_summary['new_pb_IM'] + 15)*1.22
co_summary['new_pb_FF'] = np.where(co_summary['ff_high'] < co_summary['ff_base'], co_summary['ff_high'], np.where(co_summary['ff_low'] > co_summary['ff_base'], co_summary['ff_low'], co_summary['ff_base']))
co_summary['new_pb_FF'] = round(co_summary['new_pb_FF'], 0)
co_summary['new_pb_FFGB'] = round((co_summary['new_pb_FF']/co_summary['pb_row1'])*co_summary['pb_gb'], 0)
co_summary.drop(columns={'ff_high', 'ff_low', 'ff_base', 'ff_brand_cluster'}, inplace=True)

co_summary['im_change'] = co_summary['new_pb_IM'] / co_summary['pb_im'] - 1
print('# of SKU prices calculated for CO: ' + str(co_summary['sku'].count()))
print(co_summary.head())

all_co = co_summary[['sku', 'new_pb_IM', 'new_pb_CE', 'new_pb_XSLN1', 'new_pb_FF', 'new_pb_FFGB']]

## --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## RESELLER PRICEBOOKS
print('---'*20)
print(f'\nRESELLER PRICEBOOKS: \n')

ab_df = pd.concat([new_ab, current_ab, co_ab, old_ab])
# ab_df = co_ab

sales_case = ab_df['private_high'].str.contains('dded') | ab_df['private_medium'].str.contains('dded') | ab_df['public_high'].str.contains('dded')
ab_df['sales_filter'] = np.where(sales_case, 'N', 'Y')

ab_df['day_since'] = (ab_df['calculation_date'] - ab_df['publishing_date']).dt.days
ab_df['publishing_filter'] = np.where(ab_df['day_since'] <= 14, 'N', 'Y')

ab_df = ab_df.merge(promo_df, how='left', left_on='sku', right_on='sku')
ab_df['promo_filter'] = np.where(ab_df['max_disc_b2b'] > 0, 'N', 'Y')

ab_df = ab_df.loc[(ab_df['sales_filter'] == 'Y') & (ab_df['publishing_filter'] == 'Y') & (ab_df['promo_filter'] == 'Y')][['sku', 'season', 'season_group', 'eur_cost_price', 'revised_tm', 'max_reduction']].reset_index(drop=False)
ab_df['target_cm'] = ab_df['revised_tm'] - 0.05

ab_df = ab_df.merge(ab_pb_df, how='left', left_on='sku', right_on='SKU')[['sku', 'season', 'season_group', 'eur_cost_price', 'revised_tm', 'max_reduction', 'target_cm', 
                                                                          '09ROW1_AB', '09ROW_AB', '09AU_AB', '09KR_AB', '09CN_AB',
                                                                          '09GB_AB', '09US_AB', '09JP_AB', '09HK_AB']]
ab_df = ab_df.rename(columns={'09ROW1_AB' : 'row1_ab', '09ROW_AB' : 'row_ab', '09AU_AB' : 'au_ab', '09KR_AB' : 'kr_ab',
                              '09CN_AB' : 'cn_ab', '09GB_AB' : 'gb_ab' , '09US_AB' : 'us_ab', '09JP_AB' : 'jp_ab', '09HK_AB' : 'hk_ab'})

ab_df['max_reduction'] = abs(ab_df['max_reduction'])

ab_df_prices = run_query_ab(ab_df)
ab_df_prices['lnbx_new'] = ab_df_prices['hk_ab_new']
ab_summary = ab_df_prices[['sku', 'season', 'season_group', 'eur_cost_price', 'revised_tm', 'max_reduction', 'target_cm',
                          'row1_ab_new', 'row_ab_new', 'au_ab_new', 'kr_ab_new', 'cn_ab_new', 'gb_ab_new', 'us_ab_new', 'jp_ab_new', 'hk_ab_new', 'lnbx_new',
                          'row1_ab_diff', 'row_ab_diff', 'au_ab_diff', 'kr_ab_diff', 'cn_ab_diff', 'gb_ab_diff', 'us_ab_diff', 'jp_ab_diff', 'hk_ab_diff']]

print('# of SKU prices calculated for Resellers: ' + str(ab_df_prices['sku'].count()))
print(ab_summary.head())

## --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## EXPORTS
print('---'*20)
print(f'\nEXPORTING FILES: \n')

print('* Price archives')
archive_df = pd.concat([all_new, all_current, all_co, all_old]).rename(columns={'new_pb_IM': 'pb_im_arc', 'new_pb_CE': 'pb_ce_arc', 'new_pm_XSLN1': 'pb_xsln1_arc'}).reset_index(drop=True)
archive_df = archive_df.loc[archive_df['pb_im_arc'] != 0]
archive_df.to_excel(currentlocation + '\\ArchivePrices_' + calc_date + '.xlsx', sheet_name='Archive', index=False)

all_df = pd.concat([new_summary, current_summary, co_summary, old_summary])
all_df = all_df.loc[all_df['pb_im'] > 0].reset_index(drop=True)
all_df = all_df[['sku', 'brand', 'season', 'season_group', 'co_status', 'pb_row1', 'pb_im', 'actual_gm_im', 'available_qty', 'eur_cost_price', 'stock_on_hand', 
                 'actual_st', 'coverage', 'max_reduction', 'revised_tm', 'tm_diff', 'new_pb_IM', 'new_pb_CE', 'new_pb_XSLN1', 'new_pb_FF', 'new_pb_FFGB', 'im_change']]

def create_excel (writer:pd.ExcelWriter, df:pd.DataFrame, sheetname:str='Sheet1', index=True):
    df.style.set_properties(**{'text-align': 'left'}).to_excel(writer, sheet_name=sheetname, index=index, float_format='%.2f')
    for column in df:
        column_length = max(df[column].astype(str).map(len).max(), len(column))
        col_idx = df.columns.get_loc(column)
        writer.sheets[sheetname].set_column(col_idx, col_idx, column_length)

print('* Detailed output')
with pd.ExcelWriter(currentlocation + '\\x_RevisedTM_ALL_' + calc_date + '.xlsx') as writer:
    create_excel(writer, new_df, 'SS25', index=False)
    create_excel(writer, current_df, 'AW24', index=False)
    create_excel(writer, co_df, 'CO', index=False)
    create_excel(writer, old_df, 'OLD', index=False)
    create_excel(writer, ab_df_prices, 'AB', index=False)

print('* Summaries')
with pd.ExcelWriter(currentlocation + '\\x_RevisedTM_Summary_' + calc_date + '.xlsx') as writer:
    create_excel(writer, all_df, 'ALL', index=False)
    create_excel(writer, new_summary, 'SS25', index=False)
    create_excel(writer, current_summary, 'AW24', index=False)
    create_excel(writer, co_summary, 'CO', index=False)
    create_excel(writer, old_summary, 'OLD', index=False)
    create_excel(writer, ab_summary, 'AB', index=False)

print('* All products for upload')
with pd.ExcelWriter(currentlocation + '\\x_RevisedTM_Summary_All_' + calc_date + '.xlsx') as writer:
    create_excel(writer, all_df, 'ALL', index=False)

finish_time = dt.datetime.now()
print(f'\nFinish time: {start_time}')
print(f'Time spent: {finish_time-start_time}')
print('---'*20)