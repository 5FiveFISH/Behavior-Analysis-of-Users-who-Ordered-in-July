####################################################################建模数据####################################################################
#####################################################################################################################################
'''逐步回归'''
def stepwise_select(data,label,cols_all,method='forward'):
    '''
    args:
        data：数据源，df
        label：标签，str
        cols_all：逐步回归的全部字段
        methrod：方法，forward:向前，backward:向后，both:双向
    return:
        select_col：最终保留的字段列表，list 
        summary：模型参数
        AIC：aic
    '''
    import statsmodels.api as sm
    
    ######################## 1.前向回归
    # 前向回归：从一个变量都没有开始，一个变量一个变量的加入到模型中，直至没有可以再加入的变量结束
    if method == 'forward':  
        add_col = [] 
        AIC_None_value = np.inf
        while cols_all:
            # 单个变量加入，计算aic
            AIC = {}
            for col in cols_all:
                print(col)
                X_col = add_col.copy()
                X_col.append(col)
                X = sm.add_constant(data[X_col])
                y = data[label]
                LR = sm.Logit(y, X).fit()
                AIC[col] = LR.aic
            AIC_min_value = min(AIC.values())   
            AIC_min_key = min(AIC,key=AIC.get)
            # 如果最小的aic小于不加该变量时的aic，则加入变量，否则停止
            if AIC_min_value < AIC_None_value:
                cols_all.remove(AIC_min_key)
                add_col.append(AIC_min_key)
                AIC_None_value = AIC_min_value
            else:
                break
        select_col = add_col
    ######################## 2.后向回归
    # 从全部变量都在模型中开始，一个变量一个变量的删除，直至没有可以再删除的变量结束
    elif method == 'backward': 
        p = True  
        # 全部变量，一个都不剔除，计算初始aic
        X_col = cols_all.copy()
        X = sm.add_constant(data[X_col])
        y = data[label]
        LR = sm.Logit(y, X).fit()
        AIC_None_value = LR.aic        
        while p:      
           # 删除一个字段提取aic最小的字段
           AIC = {}
           for col in cols_all:
               print(col)
               X_col = [i for i in cols_all if i!=col]
               X = sm.add_constant(data[X_col])
               LR = sm.Logit(y, X).fit()
               AIC[col] = LR.aic
           AIC_min_value = min(AIC.values()) 
           AIC_min_key = min(AIC, key=AIC.get)  
           # 如果最小的aic小于不删除该变量时的aic，则删除该变量，否则停止
           if AIC_min_value < AIC_None_value:
               cols_all.remove(AIC_min_key)
               AIC_None_value = AIC_min_value
               p = True
           else:
               break 
        select_col = cols_all             
    ######################## 3.双向回归
    elif method == 'both': 
        p = True
        add_col = []
        # 全部变量，一个都不剔除，计算初始aic
        X_col = cols_all.copy()
        X = sm.add_constant(data[X_col])
        y = data[label]
        LR = sm.Logit(y, X).fit()
        AIC_None_value = LR.aic        
        while p: 
            # 删除一个字段提取aic最小的字段
            AIC={}
            for col in cols_all:
                print(col)
                X_col = [i for i in cols_all if i!=col]
                X = sm.add_constant(data[X_col])
                LR = sm.Logit(y, X).fit()
                AIC[col] = LR.aic     
            AIC_min_value = min(AIC.values())
            AIC_min_key = min(AIC, key=AIC.get)
            if len(add_col) == 0: # 第一次只有删除操作，不循环加入变量
                if AIC_min_value < AIC_None_value:
                    cols_all.remove(AIC_min_key)
                    add_col.append(AIC_min_key)
                    AIC_None_value = AIC_min_value
                    p = True
                else:
                    break
            else:
                # 单个变量加入，计算aic
                for col in add_col:
                    print(col)
                    X_col = cols_all.copy()
                    X_col.append(col)
                    X = sm.add_constant(data[X_col])
                    LR = sm.Logit(y, X).fit()
                    AIC[col] = LR.aic
                AIC_min_value = min(AIC.values())
                AIC_min_key = min(AIC, key=AIC.get)
                if AIC_min_value < AIC_None_value:
                    # 如果aic最小的字段在添加变量阶段产生，则加入该变量，如果aic最小的字段在删除阶段产生,则删除该变量
                    if AIC_min_key in add_col:
                        cols_all.append(AIC_min_key)
                        add_col = list(set(add_col)-set(AIC_min_key))
                        p = True                    
                    else: 
                        cols_all.remove(AIC_min_key)
                        add_col.append(AIC_min_key)
                        p = True
                    AIC_None_value = AIC_min_value
                else:
                    break
        select_col = cols_all 
    ######################## 模型
    X = sm.add_constant(data[select_col])
    LR = sm.Logit(y, X).fit()    
    summary = LR.summary()
    AIC = LR.aic
    return select_col,summary,AIC


# 逐步回归
select_col, summary, AIC = stepwise_select(final_data, final_data.columns.tolist()[2], final_data.columns.tolist()[3:], method='both')

# 不显著特征
list(set(final_data.columns) - set(select_col) - set(['executed_flag', 'cust_id', 'order_id']))

print(summary)


#####################################################################################################################################
'''计算VIF，解决特征共线性问题'''
def calculate_vif(data):
    vif_data = pd.DataFrame()
    vif_data["feature"] = data.columns
    vif_data["VIF"] = [variance_inflation_factor(data.values, i) for i in range(len(data.columns))]
    return vif_data

def stepwise_vif_selection(data, target, significance_level=0.05):
    selected_features = list(data.columns)
    dropped = True

    while dropped:
        dropped = False
        vif_data = calculate_vif(data[selected_features])
        max_vif_feature = vif_data.loc[vif_data['VIF'].idxmax()]['feature']
        if vif_data['VIF'].max() > 10:
            print(f"Removing feature with high VIF: {max_vif_feature}")
            selected_features.remove(max_vif_feature)
            dropped = True
    
    return selected_features

# final_data 是数据集，'executed_flag' 是目标变量
selected_features = stepwise_vif_selection(final_data[select_col], 'executed_flag')
print(selected_features)


#####################################################################################################################################
'''随机森林建模'''
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.feature_selection import SelectFromModel
from sklearn.metrics import roc_auc_score
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix

X = final_data[selected_features]
y = final_data['executed_flag']

# 划分训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 构建随机森林分类器
rf_classifier = RandomForestClassifier(n_estimators=100, random_state=42)

# 在训练集上训练模型
rf_classifier.fit(X_train, y_train)

# 在测试集上进行预测
y_pred = rf_classifier.predict(X_test)

# 计算模型评价指标
print("AUC：", round(roc_auc_score(y_test, y_pred), 4))
print(classification_report(y_test, y_pred))
print("混淆矩阵：", confusion_matrix(y_test, y_pred))

# 保存模型
joblib.dump(rf_classifier, './rf_classifier_model.joblib')


# 获取特征重要性
feature_importances = rf_classifier.feature_importances_
# 对特征重要性进行排序
sorted_indices = np.argsort(feature_importances)[::-1]  # 降序排列
# 可视化特征重要性
plt.figure(figsize=(10, 6))
plt.rcParams['font.size'] = 12
plt.bar(range(X_train.shape[1]), feature_importances[sorted_indices])
plt.xticks(range(X_train.shape[1]), X_train.columns[sorted_indices], rotation=90)
plt.xlabel('Feature')
plt.ylabel('Importance')
plt.title('Feature Importance')
plt.tight_layout()
plt.show()


#####################################################################################################################################





####################################################################测试数据####################################################################
order_1 = pd.read_csv('./tmp_order_cust_feature_behavior_info_20230810.csv", encoding='utf-16',sep='\t')
data_1 = order_1.copy()
data_1['sex'] = data_1['sex'].map({'男': 1, '女': 0})
data_1['age_group'] = data_1['age_group'].map({'0~15岁': 1, '16~25岁': 2, '26~35岁': 3, '36~45岁': 4, '大于46岁': 5})
data_1.iloc[:, 1:5] = data_1.iloc[:, 1:5].astype('category')
data_1

# 缺失值填充
data_1['access_channel_cnt'] = data_1['access_channel_cnt'].fillna(0) 
data_1['lowest_price_avg'] = data_1['lowest_price_avg'].fillna(data_1['lowest_price_avg'].mean()) 

# 数据标准化
from sklearn.preprocessing import StandardScaler
data_std_1 = data_1.copy()
scaler = StandardScaler()
data_std_1.iloc[:, 5:] = scaler.fit_transform(data_1.iloc[:, 5:])

# 对类别变量进行独热编码
encoded_data_1 = pd.get_dummies(data_1.iloc[:, 1:5], prefix=data_1.iloc[:, 1:5].columns, drop_first=True).astype(int)

# 将编码后的数据与原始数据合并
final_data_1 = pd.concat([data_std_1.iloc[:,0], encoded_data_1, data_std_1.iloc[:, 5:]], axis=1)
final_data_1.info()

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
import joblib

# 加载模型
loaded_rf_classifier = joblib.load('./rf_classifier_model.joblib')
# 使用加载的模型进行预测
X_test_1 = final_data_1.iloc[:, 1:]
proba_predictions = loaded_rf_classifier.predict_proba(X_test_1)

# 获取类别为1的概率预测值
class_1_probabilities = proba_predictions[:, 1]
# 根据条件生成 y_pred_loaded
y_pred_loaded = [1 if prob >= 0.5 else 0 for prob in class_1_probabilities]
y_pred_series = pd.Series(y_pred_loaded, name='executed_flag_pred')
result = pd.concat([final_data_1['cust_id'].reset_index(drop=True), y_pred_series], axis=1)

# 获取2023.08.10当天实际成交数据量
test = pd.read_csv('./tmp_order_20230810.csv', encoding='utf-16',sep='\t')
merged_df= pd.merge(test, result, on='cust_id')

# 计算模型评价指标
y_test = merged_df['cust_executed_flag']
y_pred_loaded = merged_df['executed_flag_pred']
print("AUC (Loaded Model):", round(roc_auc_score(y_test, y_pred_loaded), 4))
print(classification_report(y_test, y_pred_loaded))
print("混淆矩阵 (Loaded Model):", confusion_matrix(y_test, y_pred_loaded))


