from flask import Flask, render_template, request
import runpy

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('home_page.html')


@app.route('/ETL_Data')
def ELT():
    runpy.run_path("VTA_pipline2.py")
    return render_template('ETL_data.html')

if __name__ == '__main__':
    app.run()