#####################################################################################################################################
import pandas as pd

# 读取数据
order = pd.read_excel("./order_cust_info_202307.xlsx")
# 数据集预览
order.info()


#####################################################################################################################################
# 提取有用的字段
data = pd.concat([order.iloc[:,0:2], order['executed_flag'], order['star_level'], order.iloc[:,11:14], order.iloc[:,16:20], order.iloc[:,22:33], order.iloc[:,35:]], axis=1)
data['sex'] = data['sex'].map({'男': 1, '女': 0})
data['age_group'] = data['age_group'].map({'0~15岁': 1, '16~25岁': 2, '26~35岁': 3, '36~45岁': 4, '大于46岁': 5})
data.iloc[:, 2:7] = data.iloc[:, 2:7].astype('category')
data.info()


#####################################################################################################################################
import matplotlib.pyplot as plt
import seaborn as sns

# 使用 seaborn 的 pairplot 绘制散点矩阵
sns.pairplot(data.iloc[:, 5:], diag_kind='hist', markers='+', plot_kws={'alpha': 0.8})
plt.show()


#####################################################################################################################################
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = 'SimHei' # 黑体

## 性别分布
cross_tab = pd.crosstab(order['sex'], order['executed_flag'])
# 绘制环形图
fig, ax = plt.subplots(figsize=(5, 5))
sex_labels = cross_tab.index
executed_labels = cross_tab.columns
data = cross_tab.values.flatten()
wedges, texts, autotexts = ax.pie(data, labels=[f'{sex} {exec}' for sex in sex_labels for exec in executed_labels],
                                   autopct='%1.2f%%', startangle=90, colors=plt.cm.Blues(np.linspace(0.2, 0.8, len(data))),
                                   wedgeprops={'edgecolor': 'w'})
for text in texts + autotexts:
    text.set_fontsize(12)
ax.set_title('Orders by Sex and Executed Flag')
centre_circle = plt.Circle((0, 0), 0.70, fc='white')
fig.gca().add_artist(centre_circle)
# 显示图表
plt.show()

## 年龄段分布
# 计算成交和未成交的数量和占比
executed_counts = order[order['executed_flag'] == 1]['age_group'].value_counts()
non_executed_counts = order[order['executed_flag'] == 0]['age_group'].value_counts()
total_counts = executed_counts + non_executed_counts
executed_ratios = executed_counts / total_counts
non_executed_ratios = non_executed_counts / total_counts
# 设置图形参数
plt.figure(figsize=(8, 5))
plt.rcParams['font.size'] = 12
# 绘制堆叠条形图
plt.bar(executed_counts.index, executed_counts, label='Executed')
plt.bar(non_executed_counts.index, non_executed_counts, bottom=executed_counts, label='Non-executed')
# 在图上显示各部分占比
for age_group, executed_ratio, non_executed_ratio in zip(executed_counts.index, executed_ratios, non_executed_ratios):
    plt.text(age_group, total_counts[age_group] / 2, f'{executed_ratio:.2%}', ha='center', va='center', color='white')
    plt.text(age_group, executed_counts[age_group] + non_executed_counts[age_group] / 2, f'{non_executed_ratio:.2%}', ha='center', va='center', color='white')
# 设置图例和标签
plt.legend()
plt.xlabel('Age Group')
plt.ylabel('Count')
plt.title('Executed vs Non-executed Orders by Age Group')
plt.xticks(rotation=45)
plt.tight_layout()
# 显示图表
plt.show()


#####################################################################################################################################
# 导出省份与城市的对应关系——中国省份_城市.txt
import requests
import json
header = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
}
response = requests.get('https://j.i8tq.com/weather2020/search/city.js',headers=header)
result = json.loads(response.text[len('var city_data ='):])

# 省份名字的映射字典
province_mapping = {
    '北京': '北京市',
    '天津': '天津市',
    '河北': '河北省',
    '山西': '山西省',
    '内蒙古': '内蒙古自治区',
    '辽宁': '辽宁省',
    '吉林': '吉林省',
    '黑龙江': '黑龙江省',
    '上海': '上海市',
    '江苏': '江苏省',
    '浙江': '浙江省',
    '安徽': '安徽省',
    '福建': '福建省',
    '江西': '江西省',
    '山东': '山东省',
    '河南': '河南省',
    '湖北': '湖北省',
    '湖南': '湖南省',
    '广东': '广东省',
    '广西': '广西壮族自治区',
    '海南': '海南省',
    '重庆': '重庆市',
    '四川': '四川省',
    '贵州': '贵州省',
    '云南': '云南省',
    '西藏': '西藏自治区',
    '陕西': '陕西省',
    '甘肃': '甘肃省',
    '青海': '青海省',
    '宁夏': '宁夏回族自治区',
    '新疆': '新疆维吾尔自治区',
    '台湾': '台湾省',
    '香港': '香港特别行政区',
    '澳门': '澳门特别行政区'
}
each_province_data = {}
f = open('./中国省份_城市.txt',mode='w',encoding='utf-8')
for k,v in result.items():
    province = province_mapping.get(k, k)
    if k in ['上海', '北京', '天津', '重庆']:
        city = '，'.join(list(v[k].keys()))
    else:
        city = '，'.join(list(v.keys()))
    f.write(f'{province}_{city}\n')
    each_province_data[province] = city
f.close()
print(each_province_data)


# 用户所在省份分布
import copy
from pyecharts import options as opts
from pyecharts.charts import Map

order['user_city'] = order['user_city'].str.replace(r'市$', '', regex=True)
city = order['user_city'].value_counts()
city_list = [list(ct) for ct in city.items()]
def province_city():
    '''这是从接口里爬取的数据（不太准，但是误差也可以忽略不计！）'''
    area_data = {}
    with open('./中国省份_城市.txt', mode='r', encoding='utf-8') as f:
        for line in f:
            line = line.strip().split('_')
            area_data[line[0]] = line[1].split(',')
    province_data = []
    for ct in city_list:
        for k, v in area_data.items():
            for i in v:
                if ct[0] in i:
                    ct[0] = k
                    province_data.append(ct)
    area_data_deepcopy = copy.deepcopy(area_data)
    for k in area_data_deepcopy.keys():
        area_data_deepcopy[k] = 0
    for i in province_data:
        if i[0] in area_data_deepcopy.keys():
            area_data_deepcopy[i[0]] = area_data_deepcopy[i[0]] +i[1]
    province_data = [[k,v]for k,v in area_data_deepcopy.items()]
    best = max(area_data_deepcopy.values())
    return province_data,best
province_data,best = province_city()
#地图_中国地图（带省份）Map-VisualMap（连续型）
c1 = (
    Map()
    .add( "各省份下单用户量", province_data, "china")
    .set_global_opts(
        title_opts=opts.TitleOpts(title="7月份下单用户所在省份的分布情况"),
        visualmap_opts=opts.VisualMapOpts(max_=int(best / 2)),
    )
    .render("map_china.html")
)


# 用户所在城市地图
city = order['user_city'].value_counts()
city_list = [list(ct) for ct in city.items()]
# 江苏省地级市
jiangsu_cities_mapping = {
    '南京': '南京市',
    '无锡': '无锡市',
    '徐州': '徐州市',
    '常州': '常州市',
    '苏州': '苏州市',
    '南通': '南通市',
    '连云港': '连云港市',
    '淮安': '淮安市',
    '盐城': '盐城市',
    '扬州': '扬州市',
    '镇江': '镇江市',
    '泰州': '泰州市',
    '宿迁': '宿迁市'
}
# 找出属于江苏省的城市列表并进行映射转换
jiangsu_cities_data = []
for city, value in city_list:
    if city in jiangsu_cities_mapping:
        jiangsu_cities_data.append((jiangsu_cities_mapping[city], value))
# 创建地图
c2 = (
    Map()
    .add("各城市下单用户量", jiangsu_cities_data, "江苏")
    .set_global_opts(
        title_opts=opts.TitleOpts(title="江苏省下单用户分布"),
        visualmap_opts=opts.VisualMapOpts(max_=int(max([_[1] for _ in jiangsu_cities_data])/2)),
    )
    .render("jiangsu_cities_map.html")
)

# 浙江省地级市
zhejiang_cities_mapping = {
    '杭州': '杭州市',
    '宁波': '宁波市',
    '温州': '温州市',
    '嘉兴': '嘉兴市',
    '湖州': '湖州市',
    '绍兴': '绍兴市',
    '金华': '金华市',
    '衢州': '衢州市',
    '舟山': '舟山市',
    '台州': '台州市',
    '丽水': '丽水市'
}
# 找出属于浙江省的城市列表并进行映射转换
zhejiang_cities_data = []
for city, value in city_list:
    if city in zhejiang_cities_mapping:
        zhejiang_cities_data.append((zhejiang_cities_mapping[city], value))
# 创建地图
c3 = (
    Map()
    .add("各城市下单用户量", zhejiang_cities_data, "浙江")
    .set_global_opts(
        title_opts=opts.TitleOpts(title="浙江省下单用户分布"),
        visualmap_opts=opts.VisualMapOpts(max_=int(max([_[1] for _ in zhejiang_cities_data])/2)),
    )
    .render("zhejiang_cities_map.html")
)

# 广东省地级市
guangdong_cities_mapping = {
    '广州': '广州市',
    '深圳': '深圳市',
    '珠海': '珠海市',
    '汕头': '汕头市',
    '韶关': '韶关市',
    '佛山': '佛山市',
    '江门': '江门市',
    '湛江': '湛江市',
    '茂名': '茂名市',
    '肇庆': '肇庆市',
    '惠州': '惠州市',
    '梅州': '梅州市',
    '汕尾': '汕尾市',
    '河源': '河源市',
    '阳江': '阳江市',
    '清远': '清远市',
    '东莞': '东莞市',
    '中山': '中山市',
    '潮州': '潮州市',
    '揭阳': '揭阳市',
    '云浮': '云浮市'
}
# 找出属于广东省的城市列表并进行映射转换
guangdong_cities_data = []
for city, value in city_list:
    if city in guangdong_cities_mapping:
        guangdong_cities_data.append((guangdong_cities_mapping[city], value))
# 创建地图
c4 = (
    Map()
    .add("各城市下单用户量", guangdong_cities_data, "广东")
    .set_global_opts(
        title_opts=opts.TitleOpts(title="广东省下单用户分布"),
        visualmap_opts=opts.VisualMapOpts(max_=int(max([_[1] for _ in guangdong_cities_data])/2)),
    )
    .render("guangdong_cities_map.html")
)

# 安徽省地级市
anhui_cities_mapping = {
    '合肥': '合肥市',
    '芜湖': '芜湖市',
    '蚌埠': '蚌埠市',
    '淮南': '淮南市',
    '马鞍山': '马鞍山市',
    '淮北': '淮北市',
    '铜陵': '铜陵市',
    '安庆': '安庆市',
    '黄山': '黄山市',
    '滁州': '滁州市',
    '阜阳': '阜阳市',
    '宿州': '宿州市',
    '六安': '六安市',
    '亳州': '亳州市',
    '池州': '池州市',
    '宣城': '宣城市'
}
# 找出属于安徽省的城市列表并进行映射转换
anhui_cities_data = []
for city, value in city_list:
    if city in anhui_cities_mapping:
        anhui_cities_data.append((anhui_cities_mapping[city], value))
# 创建地图
c5 = (
    Map()
    .add("各城市下单用户量", anhui_cities_data, "安徽")
    .set_global_opts(
        title_opts=opts.TitleOpts(title="安徽省下单用户分布"),
        visualmap_opts=opts.VisualMapOpts(max_=int(max([_[1] for _ in anhui_cities_data])/2)),
    )
    .render("anhui_cities_map.html")
)


#####################################################################################################################################
# 缺失值填充
data['access_channel_cnt'] = data['access_channel_cnt'].fillna(0) 
data['lowest_price_avg'] = data['lowest_price_avg'].fillna(data['lowest_price_avg'].mean()) 


#####################################################################################################################################
from sklearn.preprocessing import StandardScaler

# 数值型变量——数据标准化
data_std = data.copy()
scaler = StandardScaler()
data_std.iloc[:, 7:] = scaler.fit_transform(data.iloc[:, 7:])

# 分类变量——独热编码
encoded_data = pd.get_dummies(data.iloc[:, 3:7], prefix=data.iloc[:, 3:7].columns, drop_first=True).astype(int)

# 将编码后的数据与原始数据合并
final_data = pd.concat([data_std.iloc[:,:3], encoded_data, data_std.iloc[:, 7:]], axis=1)
final_data

# 标签重分
# 1：1-度假产品成交，0：0 and 2-未成交 and 单资源产品成交
final_data['executed_flag'] = final_data['executed_flag'].replace({2: 0})
final_data['executed_flag'].unique()

final_data.info()


#####################################################################################################################################
import matplotlib.pyplot as plt
import seaborn as sns

# 绘制热力图
plt.figure(figsize=(8, 8))
sns.heatmap(final_data.iloc[:, 1:21].corr(), cmap="RdBu_r")
plt.title("Correlation Heatmap")
plt.show()

plt.figure(figsize=(8, 8))
sns.heatmap(final_data.iloc[:, 21:].corr(), cmap="RdBu_r")
plt.title("Correlation Heatmap")
plt.show()


#####################################################################################################################################
import matplotlib.pyplot as plt
import seaborn as sns

# 绘制热力图
plt.figure(figsize=(8, 8))
sns.heatmap(final_data[select_col[:19]].corr(), annot=True, annot_kws={"size": 8}, fmt=".2f", cmap="RdBu_r")
plt.title("Correlation Heatmap")
plt.show()

plt.figure(figsize=(8, 8))
sns.heatmap(final_data[select_col[19:]].corr(), annot=True, annot_kws={"size": 8}, fmt=".2f", cmap="RdBu_r")
plt.title("Correlation Heatmap")
plt.show()


#####################################################################################################################################
# 对于分类变量，使用卡方检验
from scipy.stats import chi2_contingency

results = []
for var in ['star_level', 'cust_type', 'sex', 'age_group', 'access_channel_cnt']:
    contingency_table = pd.crosstab(data[var], data['executed_flag'])
    chi2, p, dof, expected = chi2_contingency(contingency_table)
    p_value_significant = "显著" if p < 0.05 else "不显著"
    results.append({'Variable': var,
                    'P-value': p,
                    'Significant': p_value_significant,
                    'Executed Most Level': contingency_table[1].idxmax(),
                    'Executed Least Level': contingency_table[1].idxmin()})
print(pd.DataFrame(results))


# 对于连续性变量，使用方差分析
from scipy.stats import f_oneway

unselect = set(final_data.columns) - set(select_col)
variables = list(set(varnames) - unselect)
variables = sorted(variables, key=lambda x: colnames.index(x))
results = []
for var in variables[5:]:
    executed_data = data[data['executed_flag'] == 1][var]
    unexecuted_data = data[data['executed_flag'] == 0][var]
    result = f_oneway(executed_data, unexecuted_data)
    var_mean = data.groupby('executed_flag')[var].mean()
    results.append({'Variable': var,
                    'P-value': result.pvalue,
                    'Significant': p_value_significant,
                    'Executed Mean': round(var_mean.loc[1], 4),
                    'Unexecuted Mean': round(var_mean.loc[0], 4)})
pd.DataFrame(results)


#####################################################################################################################################

