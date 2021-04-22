from flask import request, Flask
import configs
from exts import db

app = Flask(__name__)
# 加载配置文件
app.config.from_object(configs)
# db绑定app
db.init_app(app)
app.config['JSON_AS_ASCII'] = False


@app.route('/data', methods=['POST'])
def data():
    tablename = request.form['tablename']
    count = request.form['count']
    if count == None:
        count = 5
    #
    sql = 'select * from ' + tablename
    result = db.session.execute(sql)
    result = result.cursor.fetchall()
    sql2 = 'select * from ' + tablename + '_contrast'
    result2 = db.session.execute(sql2)
    result2 = result2.cursor.fetchall()
    #
    data = {}
    xLabelList = []
    dataTable = []
    #
    for re2 in result2:
        a = {}
        a['cname'] = re2[1]
        a['ename'] = re2[2]
        a['pointer'] = 'xLabel' +str(re2[0])
        xLabelList.append(a)
    #
    for re in result:
        b = {}
        b['c_yAxis'] = re[0]
        b['e_yAxis'] = re[1]
        for i in range(len(result2)):
            b['xLabel' + str(i)] = re[i + 2]
        dataTable.append(b)
    data['xLabelList'] = xLabelList
    data['dataTable'] = dataTable
    return data


if __name__ == '__main__':
    app.run(debug=False, host='127.0.0.1', port=8888)







