from flask import request, Flask
from flask_cors import CORS
import json
import configs
from exts import db
import numpy as np
import pandas as pd

app = Flask(__name__)
CORS(app, supports_credentials=True)
# 加载配置文件
app.config.from_object(configs)
# db绑定app
db.init_app(app)
app.config['JSON_AS_ASCII'] = False


def datasql(tablename, hlzh, type):
    sql = 'select * from ' + tablename
    result = db.session.execute(sql)
    result = result.cursor.fetchall()
    sql2 = 'select * from ' + tablename + '_contrast'
    result2 = db.session.execute(sql2)
    result2 = result2.cursor.fetchall()
    # data
    xLabelList = []
    dataTable = []
    # hlzh = '0'转换
    if hlzh == '0':
        index = 0
        for re1 in result2:
            b = {}
            b['c_yAxis'] = re1[1]
            b['e_yAxis'] = re1[2]
            for i in range(len(result)):
                b['xLabel' + str(i)] = result[i][index + 2]
            index += 1
            dataTable.append(b)
        for re2 in result:
            a = {}
            a['cname'] = re2[1]
            a['ename'] = re2[1]
            a['pointer'] = 'xLabel' + str(re2[0])
            xLabelList.append(a)
    # hlzh = '1'不转换
    elif hlzh == '1':
        for re2 in result2:
            a = {}
            a['cname'] = re2[1]
            a['ename'] = re2[2]
            a['pointer'] = 'xLabel' + str(re2[0])
            xLabelList.append(a)
        for re in result:
            b = {}
            b['c_yAxis'] = re[1]
            b['e_yAxis'] = re[1]
            for i in range(len(result2)):
                b['xLabel' + str(i)] = re[i + 2]
            dataTable.append(b)
    return xLabelList, dataTable


@app.route('/data', methods=['POST'])
def datamethod():
    # data
    datarequest = request.form.to_dict()
    tablename = ''
    count = 5
    search_list = ''
    hlzh = 1
    type = ''
    func = ''
    formula = ''
    newField = ''
    for key in datarequest:
        if key == 'tablename':
            tablename = datarequest[key]
        if key == 'count':
            count = datarequest[key]
        if key == 'search_list':
            search_list = datarequest[key]
        if key == 'hlzh':
            hlzh = datarequest[key]
        if key == 'type':
            type = datarequest[key]
        if key == 'func':
            func = datarequest[key]
        if key == 'formula':
            formula = datarequest[key]
        if key == 'newField':
            newField = datarequest[key]

    # sql
    data = {}
    if tablename is None:
        return {"code": 404, "data": "error"}
    # hlzh
    xLabelList, dataTable = datasql(tablename, hlzh, type)

    # Data filtering
    dtpd = pd.DataFrame(dataTable)
    dtpdtemp = dtpd.loc[:, ['c_yAxis', 'e_yAxis']]
    dtpd = dtpd.set_index('e_yAxis')
    dtpdtemp.index = dtpd.index
    dtpd = dtpd.drop(labels='c_yAxis', axis=1, index=None, columns=None, inplace=False)

    # 按行筛选
    if search_list != '':
        items = json.loads(search_list)
        for item in items:
            for key in item:
                if type == 'row':
                    for elist in xLabelList:
                        if elist.get('ename') == key:
                            pointer = elist.get('pointer')
                            break
                    for v in item[key]:
                        if 'gt' in v:
                            dtpd = dtpd.loc[(dtpd[pointer] > int(v['gt'])), :]
                        elif 'lt' in v:
                            dtpd = dtpd.loc[(dtpd[pointer] < int(v['lt'])), :]
                        elif 'le' in v:
                            dtpd = dtpd.loc[(dtpd[pointer] <= int(v['le'])), :]
                        elif 'ge' in v:
                            dtpd = dtpd.loc[(dtpd[pointer] >= int(v['ge'])), :]
                elif type == 'column':
                    for v in item[key]:
                        if 'gt' in v:
                            dtpd = dtpd.loc[:, (dtpd.xs(key, axis=0) > int(v['gt']))]
                        elif 'lt' in v:
                            dtpd = dtpd.loc[:, (dtpd.xs(key, axis=0) < int(v['lt']))]
                        elif 'le' in v:
                            dtpd = dtpd.loc[:, (dtpd.xs(key, axis=0) <= int(v['le']))]
                        elif 'ge' in v:
                            dtpd = dtpd.loc[:, (dtpd.xs(key, axis=0) >= int(v['ge']))]

    # count
    if hlzh == '0':
        dtpd = dtpd.iloc[:, 0:int(count)]
        xLabelList = xLabelList[0:int(count)]
    if hlzh == '1':
        dtpd = dtpd.iloc[:int(count)]

    # func
    if func != '':
        if func == 'sum':
            dtpd.loc['求和'] = dtpd.apply(lambda x: x.sum())
            dtpdtemp.loc['求和'] = ['求和', 'sum']
        elif func == 'avg':
            dtpd.loc['求平均'] = dtpd.apply(lambda x: x.mean())
            dtpdtemp.loc['求平均'] = ['求平均', 'avg']
        elif func == 'max':
            dtpd.loc['最大值'] = dtpd.max()
            dtpdtemp.loc['最大值'] = ['最大值', 'max']
        elif func == 'min':
            dtpd.loc['最小值'] = dtpd.min()
            dtpdtemp.loc['最小值'] = ['最小值', 'min']

    if (formula != '') & (newField != ''):
        formula = formula.split(',')
        formulaa = []
        formulab = []
        for index in range(len(formula)):
            if formula[index] == '+' or formula[index] == '-' or formula[index] == '*' or formula[index] == '/':
                formulab.append(formula[index])
            else:
                formulaa.append(formula[index])
        i = 0
        formulaa = pd.Series(formulaa)
        formulab = pd.Series(formulab)
        while (i < len(formulab)):
            if (formulab[i] == '*' or formulab[i] == '/'):
                formulaa[i] = operator(formulaa[i], formulaa[i + 1], formulaa[i],dtpd,hlzh,type)
                formulab.remove(formulab[i])
                formulaa.remove(formulaa[i + 1])
            i = i + 1
        while (len(formulaa) > 1):
            formulaa[0] = operator(formulaa[0], formulaa[1], formulab[0],dtpd,hlzh,type)
            del formulab[0]
            del formulaa[1]
        if (hlzh == '0') & (type == 'column') | (hlzh == '1') & (type == 'row'):
            dtpd[newField] = formulaa[0].to_frame()
        else:
            dtpd.loc[newField] = formulaa[0]
            dtpdtemp.loc[newField] = [newField, newField]

    dtpd = dtpdtemp.join(dtpd)
    dtpd = dtpd.dropna(axis=0, how='any')

    data['xLabelList'] = xLabelList
    data['dataTable'] = dtpd.to_dict(orient='records')
    return {"code": 200, "data": data}


def operator(x, y, ope, dtpd, hlzh,type):
    if (ope == '+'):
        if (hlzh == '0') & (type == 'column') | (hlzh == '1') & (type == 'row'):
            return dtpd[x] + dtpd[y]
        else:
            return dtpd.xs(x, axis=0) + dtpd.xs(y, axis=0)
    elif (ope == '-'):
        return dtpd[x] - dtpd[y]
    elif (ope == '*'):
        return dtpd[x] * dtpd[y]
    elif (ope == '/'):
        return dtpd[x] / dtpd[y]


@app.route('/login', methods=['POST'])
def login():
    return {"code": 200, "data": {"token": "admin-token"}}


@app.route('/logout', methods=['POST'])
def logout():
    return {"code": 200, "data": "success"}


@app.route('/info', methods=['GET'])
def info():
    return {"code": 200, "data": {"roles": ["admin"], "introduction": "I am a super administrator",
                                  "avatar": "https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif",
                                  "name": "Super Admin"}}


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8888)
