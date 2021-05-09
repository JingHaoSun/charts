from flask import Flask
import config

app = Flask(__name__)

# 加载配置文件
app.config.from_object(config)
import charts
