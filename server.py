from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('home_page.html')


@app.route('/ETL_Data')
def ELT():
    return render_template('ETL_data.html')

if __name__ == '__main__':
    app.run()