import pandas as pd
import numpy as np
import clickhouse_connect
import os


# 建立连接
client = clickhouse_connect.get_client(
    host='XXX',
    port=8123,
    username='XXX',
    password='XXX',
    database='XXX'
)
#文件夹命名
pkg_name = ('25419444')


query = ("""
SELECT part,
CASE WHEN adv_manager IN ('117139','118223','116909', '117130', '117137', '117119', '118587', '117715', '116370', '117972', '111835', '116352', '117702', '117059', '118389', '117703', '117811') THEN 1 WHEN adv_manager IN ('118731','117079', '117170', '118303', '117078', '118238', '117140', '117741', '117077') THEN 2 ELSE NULL END team,
CASE WHEN adv_manager = '117139' THEN 'gabrielle wu'    WHEN adv_manager = '116909' THEN 'molly lu' WHEN adv_manager = '118731' THEN 'lumi mu'   WHEN adv_manager = '118223' THEN 'cheryl shi'    WHEN adv_manager = '117130' THEN 'rowan wang'        WHEN adv_manager = '117137' THEN 'beryl ji'        WHEN adv_manager = '117119' THEN 'sherry zhu'        WHEN adv_manager = '118587' THEN 'jeanne chen'        WHEN adv_manager = '117715' THEN 'shirely su'        WHEN adv_manager = '116370' THEN 'cora chen'        WHEN adv_manager = '117972' THEN 'sabrina zhang'        WHEN adv_manager = '111835' THEN 'gary yang'        WHEN adv_manager = '116352' THEN 'cayla li'        WHEN adv_manager = '117702' THEN 'weiyi wang'        WHEN adv_manager = '117059' THEN 'zoe ju'        WHEN adv_manager = '118389' THEN 'yuni chen'        WHEN adv_manager = '117703' THEN 'cathie dong'        WHEN adv_manager = '117811' THEN 'bobo wang'        WHEN adv_manager = '117079' THEN 'celeste xue'        WHEN adv_manager = '117170' THEN 'zephyr zhu'        WHEN adv_manager = '118303' THEN 'lily li'        WHEN adv_manager = '117078' THEN 'aimee zhao'        WHEN adv_manager = '118238' THEN 'luna ren'        WHEN adv_manager = '117140' THEN 'zou zou'        WHEN adv_manager = '117741' THEN 'leslie xie'        WHEN adv_manager = '117077' THEN 'sonja xing'        ELSE NULL    END AM,
CASE when pid ='' then '-' else pid END pid,
CASE when length(agency_name) >0 then agency_name else '-' END agency_name, country ,offer_id ,
CASE when def_sub4 in ('hc-gateway','hc-offline') then def_sub2  when def_sub4 in ('pushnode') and def_sub3 like '%proxy%' then def_sub3 else 's2s' END trace_or_tracenotify,
def_sub4, CASE  when def_sub4 in ('hc-gateway','hc-offline')  then tid when def_sub4 in ('pushnode') then aff_sub2 END strategy,
def_sub3,ds_adx, case when def_sub4 = 'hc-offline'  then '/' else ds_bundle  end bundle,
sum(click+impressions) value,sum(conversion) conv ,sum(conversion3-conversion2) deny,sum(event_0_cnt+event_1_cnt) event
from XXX
--where part between '2025-05-12' and '2025-05-18'
where part in ('2025-06-12')
--and agency_name  ='Mobvista'
--and pkg_name ='1359763701'
and offer_id = '25419444'
--and app_id = '1880'
--and def_sub4 in ('hc-offline','hc-gateway')
--and offer_id ='25349839'
and def_sub4!= ''
group by part,team ,AM,pid,agency_name,country ,offer_id ,trace_or_tracenotify,def_sub4,strategy,def_sub3,ds_adx,bundle
""")


result = client.query(query)
df = pd.DataFrame(result.result_rows, columns=result.column_names)
folder_path = os.path.join(r'C:\Users\raelynn.peng\Desktop\导出', pkg_name)
def save_original_data():
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, 'original_data.csv')
    df.to_csv(file_path, index=False)
save_original_data()


# 区分category
conditions = [
    df['part'].isin(['2025-06-04']),
    df['part'].isin(['2025-06-06'])
]
choices = ['category1', 'category2']
df['category'] = np.select(conditions, choices, default=None)

#做透视
def make_pivot_table(df):

    # 根据国家做透视
    pivot_table_country = pd.pivot_table(
        df,
        index='country',  # 将 'country' 放在行
        columns='category',  # 将 'category' 放在列
        values=['value', 'conv','deny'],  # 对 'value' 和 'conv' 进行求和
        aggfunc='sum',  # 使用求和函数
        fill_value=0
    )
    pivot_table_country = pivot_table_country.replace([np.inf, -np.inf], np.nan).fillna(0)
    # 新加占比列
    category_totals = pivot_table_country['value'].sum(axis=0)  # 按列求和（每个 category 的总和）
    country_value_percentage = (pivot_table_country['value'] / category_totals) * 100  # 计算占比并转换为百分比
    # 将占比结果作为新列添加到透视表中
    for category in country_value_percentage.columns:
        pivot_table_country[('country_value_percentage', category)] = country_value_percentage[category]
    # 计算 cr 值（conv / value），并转换为百分比形式，保留 4 位小数
    for category in pivot_table_country['conv'].columns:
        cr_values = (pivot_table_country['conv'][category] / pivot_table_country['value'][category]) * 100  # 计算 cr 值
        cr_values = cr_values.round(4)  # 保留 4 位小数
        pivot_table_country[('cr', category)] = cr_values  # 将 cr 值作为新列添加到透视表中
    for category in pivot_table_country['deny'].columns:
        deny_values = pivot_table_country['deny'][category] /((pivot_table_country['conv'][category]) +pivot_table_country['deny'][category] )* 100  # 计算 deny 值
        deny_values = deny_values.round(4)  # 保留 4 位小数
        pivot_table_country[('deny_rate', category)] = deny_values  # 将 cr 值作为新列添加到透视表中
    # 调整格式
    pivot_table_country['conv'] = pivot_table_country['conv'].astype(int)
    pivot_table_country['value'] = pivot_table_country['value'].astype(int)
    country_value_percentage = country_value_percentage.apply(lambda x: x.map(lambda y: f'{y:.2f}%'))
    # 更新透视表中的占比列
    for category in country_value_percentage.columns:
        pivot_table_country[('country_value_percentage', category)] = country_value_percentage[category]

    # 对 category1 和 category2 分别计数和计算合计量级
    for category in ['category1', 'category2']:
        # 计算当前 category 的合计量级（从 category_totals 中获取）
        total_value = category_totals[category]

        # 计算当前 category 中包含的 country 数量
        country_count = pivot_table_country['value'][category].astype(bool).sum()  # 统计非零值的数量

        # 打印结果
        print(f"{category} 包含 {country_count} 个 country，合计量级为 {total_value}")

    # 对 'value' 列下的 'category1' 进行降序排序
    pivot_table_country = pivot_table_country.sort_values(by=('value', 'category1'), ascending=False)



    #根据pid_and_agency做透视
    pivot_table_pid_and_agency = pd.pivot_table(
        df,
        index=['team','AM','pid','agency_name'],  # 将 'country' 放在行
        columns='category',  # 将 'category' 放在列
        values=['value', 'conv', 'deny'],  # 对 'value' 和 'conv' 进行求和
        aggfunc='sum',  # 使用求和函数
        fill_value=0
    )
    pivot_table_pid_and_agency = pivot_table_pid_and_agency.replace([np.inf, -np.inf], np.nan).fillna(0) #剔除空值和无穷
    #计算cr
    for category in pivot_table_pid_and_agency['conv'].columns:
        cr_values = (pivot_table_pid_and_agency['conv'][category] / pivot_table_pid_and_agency['value'][category]) * 100  # 计算 cr 值
        cr_values = cr_values.round(4)
        pivot_table_pid_and_agency[('cr', category)] = cr_values
    for category in pivot_table_pid_and_agency['deny'].columns:
        deny_values = (pivot_table_pid_and_agency['deny'][category] / (pivot_table_pid_and_agency['deny'][category] + pivot_table_pid_and_agency['conv'][category])) * 100
        deny_values = deny_values.round(4)  # 保留 4 位小数
        pivot_table_pid_and_agency[('deny_rate', category)] = deny_values  # 将 deny_rate 作为新列添加到透视表中

    #添加量级占比
    category_totals = pivot_table_pid_and_agency['value'].sum(axis=0)
    pid_and_agency_value_percentage = pivot_table_pid_and_agency['value']*100/category_totals
    pid_and_agency_value_percentage = pid_and_agency_value_percentage.apply(lambda x: x.map(lambda y: f'{y:.2f}%'))
    #并到透视表后面
    for category in pid_and_agency_value_percentage.columns:
        pivot_table_pid_and_agency['pid_and_agency_value_percentage',category] = pid_and_agency_value_percentage[category]
    pivot_table_pid_and_agency  = pivot_table_pid_and_agency.sort_values(by=('value', 'category1'), ascending=False)



    #根据def_sub4做透视
    pivot_table_sub4 = pd.pivot_table(
        df,
        index='def_sub4',  # 将 'country' 放在行
        columns='category',  # 将 'category' 放在列
        values=['value', 'conv','deny'],  # 对 'value' 和 'conv' 进行求和
        aggfunc='sum',  # 使用求和函数
        fill_value=0
    )
    pivot_table_sub4 = pivot_table_sub4.replace([np.inf, -np.inf], np.nan).fillna(0)
    #计算cr
    for category in pivot_table_sub4['conv'].columns:
        cr_values = (pivot_table_sub4['conv'][category] / pivot_table_sub4['value'][category]) * 100  # 计算 cr 值
        cr_values = cr_values.round(4)
        pivot_table_sub4[('cr', category)] = cr_values
    for category in pivot_table_sub4['deny'].columns:
        deny_values = (pivot_table_sub4['deny'][category] / (pivot_table_sub4['deny'][category] + pivot_table_sub4['conv'][category])) * 100
        deny_values = deny_values.round(4)  # 保留 4 位小数
        pivot_table_sub4[('deny_rate', category)] = deny_values  # 将 deny_rate 作为新列添加到透视表中
    #添加量级占比
    category_totals = pivot_table_sub4['value'].sum(axis=0)
    sub4_value_percentage = pivot_table_sub4['value']*100/category_totals
    sub4_value_percentage = sub4_value_percentage.apply(lambda x: x.map(lambda y: f'{y:.2f}%'))
    #并到透视表后面
    for category in sub4_value_percentage.columns:
        pivot_table_sub4['def_sub4_value_percentage',category] = sub4_value_percentage[category]




    #根据trace_or_tracenotify做透视
    pivot_table_trace_or_tracenotify = pd.pivot_table(
        df,
        index='trace_or_tracenotify',  # 将 'country' 放在行
        columns='category',  # 将 'category' 放在列
        values=['value', 'conv','deny'],  # 对 'value' 和 'conv' 进行求和
        aggfunc='sum',  # 使用求和函数
        fill_value=0
    )
    pivot_table_trace_or_tracenotify = pivot_table_trace_or_tracenotify.replace([np.inf, -np.inf], np.nan).fillna(0)
    #计算cr
    for category in pivot_table_trace_or_tracenotify['conv'].columns:
        cr_values = (pivot_table_trace_or_tracenotify['conv'][category] / pivot_table_trace_or_tracenotify['value'][category]) * 100  # 计算 cr 值
        cr_values = cr_values.round(4)
        pivot_table_trace_or_tracenotify[('cr', category)] = cr_values
    for category in pivot_table_trace_or_tracenotify['deny'].columns:
        deny_values = (pivot_table_trace_or_tracenotify['deny'][category] / (pivot_table_trace_or_tracenotify['deny'][category] + pivot_table_trace_or_tracenotify['conv'][category])) * 100
        deny_values = deny_values.round(4)  # 保留 4 位小数
        pivot_table_trace_or_tracenotify[('deny_rate', category)] = deny_values  # 将 deny_rate 作为新列添加到透视表中
    #添加量级占比
    category_totals = pivot_table_trace_or_tracenotify['value'].sum(axis=0)
    trace_or_tracenotify_value_percentage = pivot_table_trace_or_tracenotify['value']*100/category_totals
    trace_or_tracenotify_value_percentage = trace_or_tracenotify_value_percentage.apply(lambda x: x.map(lambda y: f'{y:.2f}%'))
    #并到透视表后面
    for category in sub4_value_percentage.columns:
        pivot_table_trace_or_tracenotify['trace_or_tracenotify_value_percentage',category] = sub4_value_percentage[category]



    #根据def_sub3做透视
    pivot_table_sub3 = pd.pivot_table(
        df,
        index='def_sub3',  # 将 'country' 放在行
        columns='category',  # 将 'category' 放在列
        values=['value', 'conv','deny'],  # 对 'value' 和 'conv' 进行求和
        aggfunc='sum',  # 使用求和函数
        fill_value=0
    )
    pivot_table_sub3 = pivot_table_sub3.replace([np.inf, -np.inf], np.nan).fillna(0) #剔除空值和无穷
    #计算cr
    for category in pivot_table_sub3['conv'].columns:
        cr_values = (pivot_table_sub3['conv'][category] / pivot_table_sub3['value'][category]) * 100  # 计算 cr 值
        cr_values = cr_values.round(4)
        pivot_table_sub3[('cr', category)] = cr_values
    for category in pivot_table_sub3['deny'].columns:
        deny_values = (pivot_table_sub3['deny'][category] / (pivot_table_sub3['deny'][category] + pivot_table_sub3['conv'][category])) * 100
        deny_values = deny_values.round(4)  # 保留 4 位小数
        pivot_table_sub3[('deny_rate', category)] = deny_values  # 将 deny_rate 作为新列添加到透视表中
    #添加量级占比
    category_totals = pivot_table_sub3['value'].sum(axis=0)
    sub3_value_percentage = pivot_table_sub3['value']*100/category_totals
    sub3_value_percentage = sub3_value_percentage.apply(lambda x: x.map(lambda y: f'{y:.2f}%'))
    #并到透视表后面
    for category in sub3_value_percentage.columns:
        pivot_table_sub3['def_sub3_value_percentage',category] = sub3_value_percentage[category]
    pivot_table_sub3  = pivot_table_sub3.sort_values(by=('value', 'category1'), ascending=False)


    #根据ds_adx做透视
    pivot_table_ds_adx = pd.pivot_table(
        df,
        index=['def_sub3','ds_adx'],  # 将 'country' 放在行
        columns='category',  # 将 'category' 放在列
        values=['value', 'conv','deny'],  # 对 'value' 和 'conv' 进行求和
        aggfunc='sum',  # 使用求和函数
        fill_value=0
    )
    pivot_table_ds_adx = pivot_table_ds_adx.replace([np.inf, -np.inf], np.nan).fillna(0) #剔除空值和无穷
    #计算cr
    for category in pivot_table_ds_adx['conv'].columns:
        cr_values = (pivot_table_ds_adx['conv'][category] / pivot_table_ds_adx['value'][category]) * 100  # 计算 cr 值
        cr_values = cr_values.round(4)
        pivot_table_ds_adx[('cr', category)] = cr_values
    for category in pivot_table_ds_adx['deny'].columns:
        deny_values = (pivot_table_ds_adx['deny'][category] / (pivot_table_ds_adx['deny'][category] + pivot_table_ds_adx['conv'][category])) * 100
        deny_values = deny_values.round(4)  # 保留 4 位小数
        pivot_table_ds_adx[('deny_rate', category)] = deny_values  # 将 deny_rate 作为新列添加到透视表中
    #添加量级占比
    category_totals = pivot_table_ds_adx['value'].sum(axis=0)
    adx_value_percentage = pivot_table_ds_adx['value']*100/category_totals
    adx_value_percentage = adx_value_percentage.apply(lambda x: x.map(lambda y: f'{y:.2f}%'))
    #并到透视表后面
    for category in adx_value_percentage.columns:
        pivot_table_ds_adx['ds_adx_value_percentage',category] = adx_value_percentage[category]
    # 对 'value' 列下的 'category1' 进行降序排序，并取前30行
    pivot_table_ds_adx = pivot_table_ds_adx.sort_values(by=('value', 'category1'), ascending=False).head(30)


    #根据ds_bundle做透视
    pivot_table_ds_bundle = pd.pivot_table(
        df,
        index='bundle',  # 将 'country' 放在行
        columns='category',  # 将 'category' 放在列
        values=['value', 'conv','deny'],  # 对 'value' 和 'conv' 进行求和
        aggfunc='sum',  # 使用求和函数
        fill_value=0
    )
    pivot_table_ds_bundle = pivot_table_ds_bundle.replace([np.inf, -np.inf], np.nan).fillna(0) #剔除空值和无穷
    #计算cr
    for category in pivot_table_ds_bundle['conv'].columns:
        cr_values = (pivot_table_ds_bundle['conv'][category] / pivot_table_ds_bundle['value'][category]) * 100  # 计算 cr 值
        cr_values = cr_values.round(4)
        pivot_table_ds_bundle[('cr', category)] = cr_values
    for category in pivot_table_ds_bundle['deny'].columns:
        deny_values = (pivot_table_ds_bundle['deny'][category] / (pivot_table_ds_bundle['deny'][category] + pivot_table_ds_bundle['conv'][category])) * 100
        deny_values = deny_values.round(4)  # 保留 4 位小数
        pivot_table_ds_bundle[('deny_rate', category)] = deny_values  # 将 deny_rate 作为新列添加到透视表中
    #添加量级占比
    category_totals = pivot_table_ds_bundle['value'].sum(axis=0)
    bundle_value_percentage = pivot_table_ds_bundle['value']*100/category_totals
    bundle_value_percentage = bundle_value_percentage.apply(lambda x: x.map(lambda y: f'{y:.2f}%'))
    #并到透视表后面
    for category in bundle_value_percentage.columns:
        pivot_table_ds_bundle['ds_bundle_value_percentage',category] = bundle_value_percentage[category]
    # 对 'value' 列下的 'category1' 进行降序排序，并取前50行
    pivot_table_ds_bundle = pivot_table_ds_bundle.sort_values(by=('value', 'category1'), ascending=False).head(50)


    #根据offer_id做透视
    pivot_table_offer_id = pd.pivot_table(
        df,
        index='offer_id',  # 将 'country' 放在行
        columns='category',  # 将 'category' 放在列
        values=['value', 'conv','deny'],  # 对 'value' 和 'conv' 进行求和
        aggfunc='sum',  # 使用求和函数
        fill_value=0
    )
    pivot_table_offer_id = pivot_table_offer_id.replace([np.inf, -np.inf], np.nan).fillna(0) #剔除空值和无穷
    #计算cr
    for category in pivot_table_offer_id['conv'].columns:
        cr_values = (pivot_table_offer_id['conv'][category] / pivot_table_offer_id['value'][category]) * 100  # 计算 cr 值
        cr_values = cr_values.round(4)
        pivot_table_offer_id[('cr', category)] = cr_values
    for category in pivot_table_offer_id['deny'].columns:
        deny_values = (pivot_table_offer_id['deny'][category] / (pivot_table_offer_id['deny'][category] + pivot_table_offer_id['conv'][category])) * 100
        deny_values = deny_values.round(4)  # 保留 4 位小数
        pivot_table_offer_id[('deny_rate', category)] = deny_values  # 将 deny_rate 作为新列添加到透视表中
    #添加量级占比
    category_totals = pivot_table_offer_id['value'].sum(axis=0)
    offer_id_value_percentage = pivot_table_offer_id['value']*100/category_totals
    offer_id_value_percentage = offer_id_value_percentage.apply(lambda x: x.map(lambda y: f'{y:.2f}%'))
    #并到透视表后面
    for category in offer_id_value_percentage.columns:
        pivot_table_offer_id['offer_id_value_percentage',category] = offer_id_value_percentage[category]
    pivot_table_offer_id  = pivot_table_offer_id.sort_values(by=('value', 'category1'), ascending=False)



    #根据strategy做透视
    pivot_table_strategy = pd.pivot_table(
        df,
        index='strategy',  # 将 'country' 放在行
        columns='category',  # 将 'category' 放在列
        values=['value', 'conv','deny'],  # 对 'value' 和 'conv' 进行求和
        aggfunc='sum',  # 使用求和函数
        fill_value=0
    )
    pivot_table_strategy = pivot_table_strategy.replace([np.inf, -np.inf], np.nan).fillna(0) #剔除空值和无穷
    #计算cr
    for category in pivot_table_strategy['conv'].columns:
        cr_values = (pivot_table_strategy['conv'][category] / pivot_table_strategy['value'][category]) * 100  # 计算 cr 值
        cr_values = cr_values.round(4)
        pivot_table_strategy[('cr', category)] = cr_values
    for category in pivot_table_strategy['deny'].columns:
        deny_values = (pivot_table_strategy['deny'][category] / (pivot_table_strategy['deny'][category] + pivot_table_strategy['conv'][category])) * 100
        deny_values = deny_values.round(4)  # 保留 4 位小数
        pivot_table_strategy[('deny_rate', category)] = deny_values  # 将 deny_rate 作为新列添加到透视表中
    #添加量级占比
    category_totals = pivot_table_strategy['value'].sum(axis=0)
    strategy_value_percentage = pivot_table_strategy['value']*100/category_totals
    strategy_value_percentage = strategy_value_percentage.apply(lambda x: x.map(lambda y: f'{y:.2f}%'))
    #并到透视表后面
    for category in strategy_value_percentage.columns:
        pivot_table_strategy['strategy_value_percentage',category] = strategy_value_percentage[category]
    pivot_table_strategy  = pivot_table_strategy.sort_values(by=('value', 'category1'), ascending=False)


    output_path = os.path.join(folder_path, 'output.xlsx')
    with pd.ExcelWriter(output_path) as writer:
        pivot_table_country.to_excel(writer, sheet_name='country', index=True)
        pivot_table_pid_and_agency.to_excel(writer, sheet_name='pid_and_agency', index=True)
        pivot_table_sub4.to_excel(writer, sheet_name='def_sub4', index=True)
        pivot_table_trace_or_tracenotify.to_excel(writer, sheet_name='trace_or_tracenotify', index=True)
        pivot_table_sub3.to_excel(writer, sheet_name='def_sub3', index=True)
        pivot_table_ds_adx.to_excel(writer, sheet_name='ds_adx', index=True)
        pivot_table_ds_bundle.to_excel(writer, sheet_name='ds_bundle', index=True)
        pivot_table_offer_id.to_excel(writer, sheet_name='offer_id', index=True)
        pivot_table_strategy.to_excel(writer, sheet_name='strategy', index=True)
make_pivot_table(df)
def save_sql():
    txt_path = os.path.join(folder_path, 'sql.txt')
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write(query)
save_sql()

print("流量结构分析已完成！")