from flask import request, Flask
from flask_cors import CORS

import configs
from exts import db

app = Flask(__name__)
CORS(app, supports_credentials=True)
# 加载配置文件
app.config.from_object(configs)
# db绑定app
db.init_app(app)
app.config['JSON_AS_ASCII'] = False


@app.route('/data', methods=['POST'])
def data():
    #sql
    tablename = request.form['tablename']
    sql = 'select * from ' + tablename
    result = db.session.execute(sql)
    result = result.cursor.fetchall()
    sql2 = 'select * from ' + tablename + '_contrast'
    result2 = db.session.execute(sql2)
    result2 = result2.cursor.fetchall()

    #data
    data = {}
    xLabelList = []
    dataTable = []
    for re2 in result2:
        a = {}
        a['cname'] = re2[1]
        a['ename'] = re2[2]
        a['pointer'] = 'xLabel' +str(re2[0])
        xLabelList.append(a)
    for re in result:
        b = {}
        b['c_yAxis'] = re[0]
        b['e_yAxis'] = re[1]
        for i in range(len(result2)):
            b['xLabel' + str(i)] = re[i + 2]
        dataTable.append(b)


    #count
    count = request.form['count']
    if count == None:
        count = 5
    dataTable = dataTable[:int(count)]

    data['xLabelList'] = xLabelList
    data['dataTable'] = dataTable

    return {"code":200,"data":data}

@app.route('/login',methods=['POST'])
def login():
    return {"code":200,"data":{"token":"admin-token"}}


@app.route('/logout',methods=['POST'])
def logout():
    return {"code":200,"data":"success"}

@app.route('/info',methods=['GET'])
def info():
    return {"code":200,"data": {"roles":["admin"],"introduction":"I am a super administrator","avatar":"https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif","name":"Super Admin"}}


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8888)







